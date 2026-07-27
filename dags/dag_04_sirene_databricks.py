"""
DAG 04 - Sirene Databricks + Snowpipe (J2)
S3KeySensor → DatabricksRunNowOperator → Snowpipe REFRESH → Validation RAW
"""
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWPIPE_NAME = "ALAN_DW.RAW.PIPE_SIRENE_ETABLISSEMENTS"

default_args = {
    "owner": "alan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_04_sirene_databricks",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["sirene", "databricks", "snowflake", "m3-s3"],
    doc_md="""
## DAG 04 - Sirene Databricks + Snowpipe

**Flux** :
`S3KeySensor` → `DatabricksRunNowOperator` → `count_before_refresh`
→ `SQLExecuteQueryOperator (REFRESH)` → `validate_raw_data_loaded`

Validation par delta de COUNT(*) (avant/après refresh) : RAW.SIRENE_ETABLISSEMENTS
n'a pas de colonne de traçabilité par run, donc on ne peut pas filtrer sur une
partition - on vérifie juste qu'au moins une ligne de plus est arrivée.

**Connections** : `aws_sirene`, `databricks_sirene`, `snowflake_sirene`
**Variables** : `databricks_job_id`, `environment`
""",
)
def dag_04_sirene_databricks():

    wait_for_sirene_s3 = S3KeySensor(
        task_id="wait_for_sirene_s3",
        bucket_name="alan-data-lake-fr",
        bucket_key="raw/sirene/annee=2024/mois=01/data.csv",
        aws_conn_id="aws_sirene",
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
        soft_fail=True,
    )

    run_sirene_spark_transform = DatabricksRunNowOperator(
        task_id="run_sirene_spark_transform",
        databricks_conn_id="databricks_sirene",
        job_id=int(Variable.get("databricks_job_id", default_var="0")),
        notebook_params={
            "env": Variable.get("environment", default_var="dev"),
            "date_partition": "2024-01",
        },
        polling_period_seconds=30,
    )

    # ANNEE/MOIS dans RAW.SIRENE_ETABLISSEMENTS sont des colonnes métier
    # (année/mois de création de l'établissement), pas un marqueur de batch
    # d'ingestion : la table n'a aucune colonne identifiant quel run a chargé
    # quelle ligne. La validation compare donc le COUNT(*) total avant/après
    # le refresh plutôt que de filtrer sur une partition supposée.

    @task(task_id="count_before_refresh")
    def count_before_refresh() -> int:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_sirene")
        result = hook.get_first("SELECT COUNT(*) FROM ALAN_DW.RAW.SIRENE_ETABLISSEMENTS")
        return int(result[0])

    count_before_task = count_before_refresh()

    refresh_snowpipe = SQLExecuteQueryOperator(
        task_id="refresh_snowpipe",
        conn_id="snowflake_sirene",
        sql=f"ALTER PIPE {SNOWPIPE_NAME} REFRESH;",
        doc_md=f"Force l'ingestion des fichiers S3 en attente. Pipe : {SNOWPIPE_NAME}",
    )

    @task(task_id="validate_raw_data_loaded")
    def validate_raw_data_loaded(count_before: int) -> int:
        """
        Vérifie que RAW.SIRENE_ETABLISSEMENTS a reçu de nouvelles lignes.
        Lève ValueError si le COUNT(*) total n'a pas augmenté depuis
        count_before_refresh (Snowpipe n'a rien chargé de nouveau).

        ALTER PIPE ... REFRESH ne fait que mettre le fichier en file d'attente :
        le COPY effectif tourne de façon asynchrone (compute serverless) et
        peut finir quelques secondes après le retour de refresh_snowpipe.
        On poll donc le COUNT(*) plusieurs fois avant de conclure à un échec.
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_sirene")
        max_attempts = 6
        wait_seconds = 20
        count_after = count_before

        for attempt in range(1, max_attempts + 1):
            result = hook.get_first("SELECT COUNT(*) FROM ALAN_DW.RAW.SIRENE_ETABLISSEMENTS")
            count_after = int(result[0])
            if count_after > count_before:
                break
            if attempt < max_attempts:
                print(
                    f"Tentative {attempt}/{max_attempts} : aucune nouvelle ligne pour "
                    f"l'instant (ingestion Snowpipe asynchrone), nouvelle vérification "
                    f"dans {wait_seconds}s."
                )
                time.sleep(wait_seconds)

        if count_after <= count_before:
            raise ValueError(
                "Validation RAW échouée : aucune nouvelle ligne chargée "
                f"(avant={count_before:,}, après={count_after:,}) après "
                f"{max_attempts * wait_seconds}s d'attente. "
                f"Diagnostiquer : SELECT SYSTEM$PIPE_STATUS('{SNOWPIPE_NAME}');"
            )
        print(
            f"OK : {count_after - count_before:,} nouvelles lignes chargées "
            f"({count_before:,} -> {count_after:,})."
        )
        return count_after

    validate_task = validate_raw_data_loaded(count_before_task)

    # ─────────────────────────────────────────────────────────────────────────

    (
        wait_for_sirene_s3
        >> run_sirene_spark_transform
        >> count_before_task
        >> refresh_snowpipe
        >> validate_task
    )


dag_04_sirene_databricks()
