from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta


@dag(
    dag_id="dag_02_sirene_branch_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sirene", "branch", "s1"],
    doc_md="""
## DAG 02 — Conditional Branching SIRENE
Demonstrates BranchPythonOperator + Airflow Variable.

**Test both branches:**
- Variable `sirene_data_available` = `true`  → run_pipeline (green), send_alert (skipped)
- Variable `sirene_data_available` = `false` → send_alert (green), run_pipeline (skipped)
    """,
)
def dag_02_sirene_branch_demo():

    def _check_data_availability(**context) -> str:
        """
        Reads the Airflow Variable 'sirene_data_available'.
        Returns a task_id (string) — that is what BranchPythonOperator expects.
        Airflow executes the task whose task_id is returned.
        All other branches are set to 'skipped'.
        """
        available = Variable.get("sirene_data_available", default_var="true")
        print(f"[BRANCH] Variable 'sirene_data_available' = '{available}'")

        if available.lower() == "true":
            print("[BRANCH] → Data available: launching pipeline")
            return "run_pipeline"
        else:
            print("[BRANCH] → Data unavailable: sending alert")
            return "send_alert"

    def _run_pipeline(**context) -> None:
        """'Available' branch: simulates dbt run on sirene_nantes."""
        print("[PIPELINE] dbt run --select staging.* marts.* (simulated)")
        print("[PIPELINE] stg_sirene_etablissements → int_etablissements_actifs")
        print("[PIPELINE]   → fct_etablissements → mart_etablissements_par_commune")
        print("[PIPELINE] Pipeline SIRENE completed successfully")

    def _send_alert(**context) -> None:
        """'Unavailable' branch: operational warning log."""
        print("[ALERT] Data SIRENE unavailable")
        print("[ALERT] Expected file: alan-data-lake-fr/sirene/raw/sirene.parquet")
        print("[ALERT] Check: Snowpipe status + SQS Event Notification + S3 bucket")

    # ── Operator declarations ──────────────────────────────────────────────

    check = BranchPythonOperator(
        task_id="check_data_availability",
        python_callable=_check_data_availability,
    )

    pipeline = PythonOperator(
        task_id="run_pipeline",
        python_callable=_run_pipeline,
    )

    alert = PythonOperator(
        task_id="send_alert",
        python_callable=_send_alert,
    )

    # EmptyOperator runs regardless of which branch was chosen.
    # trigger_rule is required: without it, pipeline_end stays "upstream_failed"
    # because the skipped branch counts as a partial failure in the default rule.
    end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed_min_one_success",
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    check >> [pipeline, alert]
    [pipeline, alert] >> end


dag_02_sirene_branch_demo()
