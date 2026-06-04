from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

# ─── Constants ───────────────────────────────────────────────────────
DBT_DIR      = "/opt/airflow/dbt/sirene_nantes"
DBT_PROFILES = "/opt/airflow/.dbt"
DBT_TARGET   = "dev"


def _dbt(cmd: str) -> str:
    """Build a full dbt command for BashOperator."""
    return (
        f"cd {DBT_DIR} && "
        f"dbt {cmd} --profiles-dir {DBT_PROFILES} --target {DBT_TARGET}"
    )


@dag(
    dag_id="dag_03_sirene_s3_dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # Manual for now — use "0 7 * * 1-5" in prod
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sirene", "pipeline", "s1-m3"],
    doc_md="""
## DAG 03 — SIRENE S3 → dbt Pipeline v1
Orchestrates the sirene_nantes pipeline via Airflow.
**Flow:** S3KeySensor → dbt deps → dbt run → dbt test
**Source:** `alan-data-lake-fr/raw/sirene/` → `ALAN_DW.RAW` → `MARTS`
**Target:** Snowflake ALAN_DW · role TRANSFORMER
    """,
)
def sirene_s3_dbt_pipeline():

    # 1 ── Wait for the SIRENE file in S3 ─────────────────────────────
    wait_for_s3 = S3KeySensor(
        task_id="wait_for_sirene_s3",
        bucket_name="alan-data-lake-fr",
        bucket_key="raw/sirene/annee=2024/mois=01/data.csv",
        aws_conn_id="aws_sirene",
        mode="reschedule",     # releases the worker between checks
        poke_interval=60,      # check every 60s (dev) — 300 in prod
        timeout=120,           # timeout 2 min (dev) — 7200 in prod
        soft_fail=True,        # SKIPPED if absent, not FAILED
    )

    # 2 ── dbt deps ────────────────────────────────────────────────────
    # trigger_rule="all_done" = runs even if the sensor is SKIPPED
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_dbt("deps"),
        trigger_rule="all_done",
    )

    # 3 ── dbt run ─────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt("run"),
    )

    # 4 ── dbt test ────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt("test"),
    )

    # ─── Dependencies ─────────────────────────────────────────────────
    wait_for_s3 >> dbt_deps >> dbt_run >> dbt_test


sirene_s3_dbt_pipeline()
