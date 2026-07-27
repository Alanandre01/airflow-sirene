"""
DAG 05 — Pipeline SIRENE Complet (Production)
M3 S3 J4 — Fusion dag_04 + dag_05 squelette
Corrections : SQLExecuteQueryOperator, pipe réel, validation delta COUNT*, sélecteur intermediate
"""
import logging
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

PIPE_NAME      = "ALAN_DW.RAW.PIPE_SIRENE_ETABLISSEMENTS"
DBT_DIR        = "/opt/airflow/dbt/sirene_nantes"
DBT_PROFILES   = "--profiles-dir /opt/airflow/.dbt --target dev"
S3_BUCKET      = "alan-data-lake-fr"
S3_KEY         = "raw/sirene/annee=2024/mois=01/data.csv"
SNOWFLAKE_CONN = "snowflake_sirene"


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
    "retry_delay": timedelta(seconds=30),        # 30s pendant la mise au point
    "on_failure_callback": on_failure_alert,
    "email_on_failure": False,
}


@dag(
    dag_id="dag_05_sirene_pipeline_complet",
    schedule="0 5 1 * *",      # 1er du mois ~6h Paris (UTC+1 hiver)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sirene", "pipeline", "production", "m3-s3"],
    doc_md="""
## DAG 05 — Pipeline SIRENE Complet

**Flux** :
S3KeySensor → DatabricksRunNowOperator → count_before_refresh
→ SQLExecuteQueryOperator (PIPE REFRESH) → validate_raw_data_loaded
→ TaskGroup(dbt_staging + intermediate) → TaskGroup(dbt_marts) → run_elementary_report

**Schedule** : `0 5 1 * *` (1er du mois ~6h Paris) | `max_active_runs=1`

**Corrections vs guide initial** :
- `SQLExecuteQueryOperator` · `conn_id=` · pipe réel · validation delta COUNT* · sélecteur intermediate
""",
)
def dag_05_sirene_pipeline_complet():

    # ── 1. Sensor S3 ─────────────────────────────────────────────────────────
    wait_for_sirene_s3 = S3KeySensor(
        task_id="wait_for_sirene_s3",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY,
        aws_conn_id="aws_sirene",
        mode="reschedule",
        poke_interval=60,
        timeout=7200,                          # poke pendant max 2h
        soft_fail=True,
        execution_timeout=timedelta(hours=3),  # doit être > timeout pokes
    )

    # ── 2. Databricks : exécuter le job Spark ────────────────────────────────
    run_sirene_spark_transform = DatabricksRunNowOperator(
        task_id="run_sirene_spark_transform",
        databricks_conn_id="databricks_sirene",
        job_id=int(Variable.get("databricks_job_id", default_var="0")),
        notebook_params={
            "env": Variable.get("environment", default_var="dev"),
            "date_partition": "2024-01",
        },
        polling_period_seconds=30,
        execution_timeout=timedelta(hours=2),
    )

    # ── 3. COUNT avant Snowpipe ──────────────────────────────────────────────
    @task(task_id="count_before_refresh", execution_timeout=timedelta(minutes=5))
    def count_before_refresh() -> int:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        count = int(hook.get_first(
            "SELECT COUNT(*) FROM ALAN_DW.RAW.SIRENE_ETABLISSEMENTS"
        )[0])
        logger.info("Avant REFRESH : %d lignes.", count)
        return count

    # ── 4. Snowpipe REFRESH ──────────────────────────────────────────────────
    # SQLExecuteQueryOperator (provider common.sql) — conn_id= pas snowflake_conn_id=
    refresh_snowpipe = SQLExecuteQueryOperator(
        task_id="refresh_snowpipe",
        conn_id=SNOWFLAKE_CONN,
        sql=f"ALTER PIPE {PIPE_NAME} REFRESH;",
        execution_timeout=timedelta(minutes=10),
    )

    # ── 5. Validation RAW avec polling ───────────────────────────────────────
    @task(task_id="validate_raw_data_loaded", execution_timeout=timedelta(minutes=5))
    def validate_raw_data_loaded(count_before: int) -> int:
        """
        Poll 6x20s. Valide que COUNT(*) global augmente après REFRESH.
        ANNEE/MOIS ne sont PAS un marqueur de batch — delta COUNT(*) uniquement.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        sql = "SELECT COUNT(*) FROM ALAN_DW.RAW.SIRENE_ETABLISSEMENTS"

        for attempt in range(1, 7):
            count_after = int(hook.get_first(sql)[0])
            if count_after > count_before:
                delta = count_after - count_before
                logger.info(
                    "OK : %d nouvelles lignes (%d → %d).",
                    delta, count_before, count_after,
                )
                return count_after
            logger.warning(
                "Tentative %d/6 : COUNT* = %d (inchangé, attente 20s).",
                attempt, count_after,
            )
            time.sleep(20)

        # Dernier check après la 6e attente
        count_after = int(hook.get_first(sql)[0])
        if count_after <= count_before:
            raise ValueError(
                f"Validation échouée après 6 tentatives. "
                f"avant={count_before}, après={count_after}. "
                f"Diagnostiquer : SELECT SYSTEM$PIPE_STATUS('{PIPE_NAME}');"
            )
        return count_after

    count_before_task = count_before_refresh()
    validate_task     = validate_raw_data_loaded(count_before_task)

    # ── 6. TaskGroup dbt staging + intermediate ──────────────────────────────
    # Sélecteur corrigé : inclure path:models/intermediate (dossier ephemeral)
    with TaskGroup("dbt_staging", tooltip="Staging + intermediate dbt") as tg_staging:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt deps {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=10),
        )
        dbt_run_staging = BashOperator(
            task_id="dbt_run",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt run --select path:models/staging path:models/intermediate {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=30),
        )
        dbt_test_staging = BashOperator(
            task_id="dbt_test",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt test --select path:models/staging path:models/intermediate {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=20),
        )
        dbt_deps >> dbt_run_staging >> dbt_test_staging

    # ── 7. TaskGroup dbt marts ───────────────────────────────────────────────
    with TaskGroup("dbt_marts", tooltip="Marts dbt") as tg_marts:
        dbt_run_marts = BashOperator(
            task_id="dbt_run",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt run --select path:models/marts {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=30),
        )
        dbt_test_all = BashOperator(
            task_id="dbt_test",
            bash_command=f"""
set -e
cd {DBT_DIR}
dbt test {DBT_PROFILES}
""",
            execution_timeout=timedelta(minutes=30),
        )
        dbt_run_marts >> dbt_test_all

    # ── 8. Elementary ────────────────────────────────────────────────────────
    run_elementary = BashOperator(
        task_id="run_elementary_report",
        bash_command=f"""
cd {DBT_DIR}
edr report --project-dir . {DBT_PROFILES} --profile-target dev \
  || echo '[WARN] Elementary non disponible, pipeline continue.'
""",
        execution_timeout=timedelta(minutes=15),
    )

    # ── Orchestration ────────────────────────────────────────────────────────
    (
        wait_for_sirene_s3
        >> run_sirene_spark_transform
        >> count_before_task
        >> refresh_snowpipe
        >> validate_task
        >> tg_staging
        >> tg_marts
        >> run_elementary
    )


dag_05_sirene_pipeline_complet()
