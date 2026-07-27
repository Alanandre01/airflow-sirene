"""
DAG 05 — Squelette TaskGroups dbt (J3)
Flux : S3KeySensor -> dbt_staging -> dbt_marts -> Elementary
Databricks + Snowflake : intégrés en J4.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

DBT_DIR      = "/opt/airflow/dbt/sirene_nantes"
DBT_PROFILES = "--profiles-dir /opt/airflow/.dbt --target dev"

default_args = {
    "owner": "alan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_05_sirene_pipeline_complet",
    schedule=None,              # sera défini en J4
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,          # pipeline stateful : pas de run parallèle
    default_args=default_args,
    tags=["sirene", "dbt", "taskgroup", "m3-s3"],
    doc_md="""
## DAG 05 — Squelette TaskGroups dbt (J3)

**Flux** : `S3KeySensor` → `dbt_staging` → `dbt_marts` → `Elementary`

**Schedule** : None (CRON défini en J4)

**Auth dbt** : RSA key-pair via `/opt/airflow/.dbt/profiles.yml`

**Connections** : `aws_sirene`
""",
)
def dag_05_sirene_pipeline_complet():

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

    # ── TaskGroup staging ──────────────────────────────────────────────────
    # task_ids internes préfixés : dbt_staging.dbt_deps / .dbt_run / .dbt_test
    # (Ces préfixes serviront aux assertions pytest en J5)
    with TaskGroup("dbt_staging", tooltip="Couche staging dbt") as tg_staging:

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"""set -e
cd {DBT_DIR}
dbt deps {DBT_PROFILES}
""",
        )
        dbt_run_staging = BashOperator(
            task_id="dbt_run",
            # Le projet a un dossier models/intermediate/ (matérialisation ephemeral,
            # voir dbt_project.yml) -> inclus dans le sélecteur staging.
            bash_command=f"""set -e
cd {DBT_DIR}
dbt run --select path:models/staging path:models/intermediate {DBT_PROFILES}
""",
        )
        dbt_test_staging = BashOperator(
            task_id="dbt_test",
            bash_command=f"""set -e
cd {DBT_DIR}
dbt test --select path:models/staging path:models/intermediate {DBT_PROFILES}
""",
        )
        dbt_deps >> dbt_run_staging >> dbt_test_staging

    # ── TaskGroup marts ────────────────────────────────────────────────────
    # task_ids internes préfixés : dbt_marts.dbt_run / .dbt_test
    with TaskGroup("dbt_marts", tooltip="Couche marts dbt") as tg_marts:

        dbt_run_marts = BashOperator(
            task_id="dbt_run",
            bash_command=f"""set -e
cd {DBT_DIR}
dbt run --select path:models/marts {DBT_PROFILES}
""",
        )
        dbt_test_all = BashOperator(
            task_id="dbt_test",
            bash_command=f"""set -e
cd {DBT_DIR}
dbt test {DBT_PROFILES}
""",
        )
        dbt_run_marts >> dbt_test_all

    # ── Elementary (non bloquant) ──────────────────────────────────────────
    run_elementary = BashOperator(
        task_id="run_elementary_report",
        bash_command=f"""cd {DBT_DIR}
edr report --project-dir . {DBT_PROFILES} --profile-target dev \
  || echo '[WARN] Elementary non disponible, pipeline continue.'
""",
    )

    # ── Orchestration ──────────────────────────────────────────────────────
    wait_for_sirene_s3 >> tg_staging >> tg_marts >> run_elementary


dag_05_sirene_pipeline_complet()
