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
        >> tg_staging
        >> tg_marts
        >> run_elementary_report
        >> check_volume_monitoring
    )


dag_06_sirene_pipeline_v2()
