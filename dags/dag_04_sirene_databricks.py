"""
DAG 04 — Sirene Databricks
S3KeySensor → DatabricksRunNowOperator
M3 S3 J1

Notes :
  - Databricks Free Edition (Azure serverless) — pas de cluster à configurer
  - Connection 'databricks_sirene' utilise un PAT (configuré en S2 J5)
  - Le fichier S3 existe déjà → sensor SUCCESS sur le premier poke (~30s)
  - Job 'sirene_spark_transform' tourne notebook 03 (Delta Lake) avec widgets
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {
    "owner": "alan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_04_sirene_databricks",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["sirene", "databricks", "m3-s3"],
    doc_md="""
## DAG 04 — Sirene + Databricks

Détecte le CSV SIRENE sur S3 et déclenche la transformation PySpark
sur Databricks Free Edition (serverless).

**Flux** : `S3KeySensor` → `DatabricksRunNowOperator`

**Connections** : `aws_sirene`, `databricks_sirene`

**Variables** : `databricks_job_id` (ID numérique), `environment`

**Notes** : `soft_fail=True` sur le sensor — si le fichier est absent à
la fin du timeout, la tâche passe en SKIPPED et Databricks est quand même
déclenché (utile pour les tests manuels).
""",
)
def dag_04_sirene_databricks():

    wait_for_sirene_s3 = S3KeySensor(
        task_id="wait_for_sirene_s3",
        bucket_name="alan-data-lake-fr",
        bucket_key="raw/sirene/annee=2024/mois=01/data.csv",
        aws_conn_id="aws_sirene",
        mode="reschedule",     # libère le worker entre les pokes
        poke_interval=30,      # vérifie toutes les 30s
        timeout=3600,          # poke pendant max 1h
        soft_fail=True,        # timeout → SKIPPED (pas FAILED), pipeline continue
        doc_md=(
            "Vérifie la présence de `raw/sirene/annee=2024/mois=01/data.csv` "
            "sur S3 (alan-data-lake-fr). Fichier confirmé présent en S2 J4 "
            "via boto3 (420 393 476 octets). Mode reschedule = non-bloquant."
        ),
    )

    run_sirene_spark_transform = DatabricksRunNowOperator(
        task_id="run_sirene_spark_transform",
        databricks_conn_id="databricks_sirene",
        # Variable.get() retourne str → int() obligatoire
        job_id=int(Variable.get("databricks_job_id", default_var="0")),
        notebook_params={
            # Override des widgets dbutils.widgets.text() du notebook 03
            "env": Variable.get("environment", default_var="dev"),
            "date_partition": "2024-01",
        },
        polling_period_seconds=30,   # polling API Databricks toutes les 30s
        doc_md=(
            "Déclenche `sirene_spark_transform` (serverless, pas de cluster). "
            "Appel : POST /api/2.1/jobs/run-now avec notebook_params. "
            "Attend TERMINATED (SUCCESS ou FAILED) avant de finir."
        ),
    )

    wait_for_sirene_s3 >> run_sirene_spark_transform


dag_04_sirene_databricks()
