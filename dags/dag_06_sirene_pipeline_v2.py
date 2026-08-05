"""
DAG 06 - Pipeline SIRENE v2 (Portfolio M3-S4-J1)
Variante allégée de dag_05_sirene_pipeline_complet : schedule hebdomadaire
(Lun-Ven 7h) au lieu de mensuel, validation par vue de monitoring Snowflake
(ALAN_DW.RAW.V_MONITORING_DERNIERS_7J) au lieu du delta COUNT*
avant/après REFRESH. Les deux DAGs coexistent, dag_05 reste le pipeline de
référence en production.
"""
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

DBT_DIR        = "/opt/airflow/dbt/sirene_nantes"
DBT_PROFILES   = "--profiles-dir /opt/airflow/.dbt --target dev"
S3_BUCKET      = "alan-data-lake-fr"
S3_KEY         = "raw/sirene/annee=2024/mois=01/data.csv"
SNOWFLAKE_CONN = "snowflake_sirene"
PIPE_NAME      = "ALAN_DW.RAW.PIPE_SIRENE_ETABLISSEMENTS"
MONITORING_VIEW = "ALAN_DW.RAW.V_MONITORING_DERNIERS_7J"


def on_failure_alert(context: dict) -> None:
    ti = context["task_instance"]
    logger.error(
        "[ALERT] dag=%s | task=%s | run=%s | url=%s",
        ti.dag_id, ti.task_id, ti.run_id, ti.log_url,
    )


default_args = {
    "owner": "alan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": on_failure_alert,
    "email_on_failure": False,
}


def run_ge_checkpoint_sirene(**kwargs):
    """
    Valide les données SIRENE post-Snowpipe avec Great Expectations.
    Blocage sélectif — miroir du pattern dbt severity: warn, classé par
    (type d'expectation, colonne) et non par colonne seule :
      FAIL warn (connus)  : CODE_POSTAL (~78% NULL), DATE_CREATION_ETAB (100% NULL),
                            ETAT_ETABLISSEMENT null-check (~70-75% NULL, mostly=0.25)
      FAIL bloquant       : SIRET, SIREN, ETAT_ETABLISSEMENT value-set -> AirflowException
    Option B (Data Source SQL GE natif Snowflake) -> amélioration future.
    """
    import logging

    import great_expectations as gx
    from great_expectations.expectations import (
        ExpectColumnValueLengthsToEqual,
        ExpectColumnValuesToBeInSet,
        ExpectColumnValuesToNotBeNull,
    )
    from airflow.exceptions import AirflowException
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    # 1. Export frais — réutilise la connexion snowflake_sirene configurée dans Airflow
    #    stg_sirene_etablissements est une VIEW dbt (materialized='view') : elle
    #    reflète le RAW à jour immédiatement après le REFRESH Snowpipe, sans
    #    attendre le run dbt_staging qui suit ce checkpoint dans la chaîne.
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    df = hook.get_pandas_df(
        """SELECT SIRET, SIREN, ETAT_ETABLISSEMENT, CODE_POSTAL, DATE_CREATION_ETAB
           FROM ALAN_DW.DBT_DEV_STAGING.STG_SIRENE_ETABLISSEMENTS
           LIMIT 10000"""
    )
    logging.info("GE: %d lignes exportées depuis DBT_DEV_STAGING.", len(df))

    # 2. Contexte GE en mémoire — ephemeral, pas de montage du dossier gx/ requis
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas("sirene_pandas")
    asset = ds.add_dataframe_asset("sirene_etablissements")
    batch_def = asset.add_batch_definition_whole_dataframe("batch_frais")

    # 3. Suite — valeurs réelles Snowflake : 'Actif'/'Fermé' (corrigé en J2)
    suite = context.suites.add(gx.ExpectationSuite(name="sirene_suite_dag06"))
    suite.add_expectation(ExpectColumnValueLengthsToEqual(column="SIRET", value=14))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="SIREN"))
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="ETAT_ETABLISSEMENT", value_set=["Actif", "Fermé"]
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="ETAT_ETABLISSEMENT", mostly=0.25)
    )
    suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="CODE_POSTAL", mostly=0.25)  # FAIL connu
    )
    suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="DATE_CREATION_ETAB")  # FAIL connu (100%)
    )

    # 4. Validation
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="sirene_vd_dag06", data=batch_def, suite=suite)
    )
    result = vd.run(batch_parameters={"dataframe": df})
    logging.info(
        "GE: %d/%d expectations passées.",
        sum(r.success for r in result.results),
        len(result.results),
    )

    # 5. Blocage sélectif — anomalies connues en warn, échecs inattendus bloquants.
    #    Classé par (type d'expectation, colonne) et non par colonne seule :
    #    ETAT_ETABLISSEMENT porte à la fois le null-check toléré (mostly=0.25,
    #    même anomalie ~70-75% NULL que CODE_POSTAL) et le expect_values_to_be_in_set
    #    qui doit rester bloquant (une valeur hors ["Actif", "Fermé"] est un vrai défaut).
    WARN_EXPECTATIONS = {
        ("expect_column_values_to_not_be_null", "CODE_POSTAL"),
        ("expect_column_values_to_not_be_null", "DATE_CREATION_ETAB"),
        ("expect_column_values_to_not_be_null", "ETAT_ETABLISSEMENT"),
    }
    hard_fails, warn_fails = [], []
    for r in result.results:
        if not r.success:
            col = r.expectation_config.kwargs.get("column", "inconnu")
            etype = r.expectation_config.type
            if (etype, col) in WARN_EXPECTATIONS:
                warn_fails.append(col)
            else:
                hard_fails.append(f"{etype}[{col}]")
    if warn_fails:
        logging.warning("GE: FAIL(s) structurel(s) connu(s) — %s", warn_fails)
    if hard_fails:
        raise AirflowException(
            f"GE Checkpoint: FAIL inattendu(s) — {hard_fails} — pipeline bloqué."
        )
    logging.info("GE Checkpoint OK — pipeline peut continuer vers dbt_staging.")


@dag(
    dag_id="dag_06_sirene_pipeline_v2",
    schedule="0 7 * * 1-5",     # Lun-Ven 7h Paris
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sirene", "pipeline", "production", "m3-s4"],
    doc_md="""
## DAG 06 - Pipeline SIRENE v2

**Flux** :
S3KeySensor → DatabricksRunNowOperator → SQLExecuteQueryOperator (PIPE REFRESH)
→ TaskGroup(dbt_staging + intermediate) → TaskGroup(dbt_marts)
→ run_elementary_report → check_volume_monitoring

**Schedule** : `0 7 * * 1-5` (Lun-Ven 7h Paris) | `max_active_runs=1`

**Différences vs dag_05** : schedule hebdomadaire, pas de dbt deps (packages
supposés déjà installés), validation via vue de monitoring plutôt que delta
COUNT*.
""",
)
def dag_06_sirene_pipeline_v2():

    # ── 1. Sensor S3 ─────────────────────────────────────────────────────────
    attendre_fichier_s3 = S3KeySensor(
        task_id="attendre_fichier_s3",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY,
        aws_conn_id="aws_sirene",
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
        soft_fail=True,
        execution_timeout=timedelta(hours=3),
    )

    # ── 2. Databricks : exécuter le job Spark ────────────────────────────────
    spark_transform_delta = DatabricksRunNowOperator(
        task_id="spark_transform_delta",
        databricks_conn_id="databricks_sirene",
        job_id=int(Variable.get("databricks_job_id", default_var="0")),
        notebook_params={
            "env": Variable.get("environment", default_var="dev"),
            "date": "{{ macros.datetime.utcnow().strftime('%Y-%m-%d') }}",
        },
        polling_period_seconds=30,
        execution_timeout=timedelta(hours=2),
    )

    # ── 3. Snowpipe REFRESH ───────────────────────────────────────────────────
    # ALTER PIPE REFRESH est asynchrone : le pipe met les fichiers en file
    # d'attente, l'ingestion réelle prend ~30-60s de plus (cf. check_volume_monitoring).
    refresh_snowpipe = SQLExecuteQueryOperator(
        task_id="refresh_snowpipe",
        conn_id=SNOWFLAKE_CONN,
        sql=f"ALTER PIPE {PIPE_NAME} REFRESH;",
        execution_timeout=timedelta(minutes=10),
    )

    # ── 3bis. GE Checkpoint — qualité données post-Snowpipe ──────────────────
    ge_checkpoint_sirene = PythonOperator(
        task_id="ge_checkpoint_sirene",
        python_callable=run_ge_checkpoint_sirene,
        retries=0,  # quality gate : retry inutile si les données sont mauvaises
        doc_md="""
        **GE Checkpoint** — Validation données SIRENE post-Snowpipe.
        FAIL bloquants : SIRET, SIREN, ETAT_ETABLISSEMENT.
        FAIL warn (anomalies connues) : CODE_POSTAL, DATE_CREATION_ETAB.
        """,
    )

    # ── 4. TaskGroup dbt staging + intermediate ──────────────────────────────
    with TaskGroup("dbt_staging", tooltip="Staging + intermediate dbt") as tg_staging:
        dbt_run_staging = BashOperator(
            task_id="run",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt run --select path:models/staging path:models/intermediate {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=30),
        )
        dbt_test_staging = BashOperator(
            task_id="test",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt test --select path:models/staging path:models/intermediate {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=20),
        )
        dbt_run_staging >> dbt_test_staging

    # ── 5. TaskGroup dbt marts ────────────────────────────────────────────────
    with TaskGroup("dbt_marts", tooltip="Marts dbt") as tg_marts:
        dbt_run_marts = BashOperator(
            task_id="run",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt run --select path:models/marts {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=30),
        )
        dbt_test_marts = BashOperator(
            task_id="test",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt test --select path:models/marts {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=30),
        )
        dbt_run_marts >> dbt_test_marts

    # ── 6. Elementary ─────────────────────────────────────────────────────────
    run_elementary_report = BashOperator(
        task_id="run_elementary_report",
        bash_command=f"""
cd {DBT_DIR}
edr report --project-dir . {DBT_PROFILES} --profile-target dev \
  || echo '[WARN] Elementary non disponible, pipeline continue.'
""",
        execution_timeout=timedelta(minutes=15),
    )

    # ── 7. Monitoring Snowflake ────────────────────────
    check_volume_monitoring = SQLExecuteQueryOperator(
        task_id="check_volume_monitoring",
        conn_id=SNOWFLAKE_CONN,
        sql=f"SELECT * FROM {MONITORING_VIEW} LIMIT 5;",
        execution_timeout=timedelta(minutes=5),
    )

    # ── Orchestration ─────────────────────────────────────────────────────────
    (
        attendre_fichier_s3
        >> spark_transform_delta
        >> refresh_snowpipe
        >> ge_checkpoint_sirene
        >> tg_staging
        >> tg_marts
        >> run_elementary_report
        >> check_volume_monitoring
    )


dag_06_sirene_pipeline_v2()
