"""
Tests de validation structurelle des DAGs SIRENE.
M3 S3 J5 — version corrigée vs m3_s3_guide.html

Corrections :
- dag_04 : 5 tâches (count_before_refresh inclus, ajouté en J2)
- dag_05 : retries=1/retry_delay=30s seulement (retry_exponential_backoff
           et max_retry_delay retirés en J4 — ne pas les tester)
- SQLExecuteQueryOperator remplace SnowflakeOperator depuis J2
- EXPECTED_DAGS : ajuster d'après la sortie de `airflow dags list`
"""
import pytest


# -- Ajuster si dag_01/dag_02 ont des noms différents (cf. étape 0) ---------
EXPECTED_DAGS = {
    "dag_01_sirene_etl_demo",
    "dag_02_sirene_branch_demo",
    "dag_03_sirene_s3_dbt_pipeline",
    "dag_04_sirene_databricks",
    "dag_05_sirene_pipeline_complet",
}


class TestDagLoading:
    """Vérifie que tous les DAGs se chargent sans erreur Python."""

    def test_no_import_errors(self, dagbag):
        assert not dagbag.import_errors, (
            "Erreurs d'import :\n"
            + "\n".join(
                f"  {path}: {err}"
                for path, err in dagbag.import_errors.items()
            )
        )

    def test_all_expected_dags_present(self, dagbag):
        missing = EXPECTED_DAGS - set(dagbag.dags.keys())
        assert not missing, f"DAGs manquants : {missing}"


class TestDagMetadata:
    """Bonnes pratiques communes à tous les DAGs."""

    def test_all_dags_have_tags(self, dagbag):
        for dag_id in EXPECTED_DAGS:
            dag = dagbag.get_dag(dag_id)
            if dag:
                assert dag.tags, f"{dag_id} : aucun tag défini"

    def test_all_dags_catchup_false(self, dagbag):
        for dag_id in EXPECTED_DAGS:
            dag = dagbag.get_dag(dag_id)
            if dag:
                assert not dag.catchup, f"{dag_id} : catchup=True"


class TestDag04:
    """Tests structurels de dag_04_sirene_databricks (5 tâches réelles)."""

    @pytest.fixture(autouse=True)
    def setup(self, dagbag):
        self.dag = dagbag.get_dag("dag_04_sirene_databricks")

    def test_dag_exists(self):
        assert self.dag is not None, "dag_04_sirene_databricks introuvable"

    def test_expected_task_ids(self):
        """5 tâches réelles après J2 (count_before_refresh ajouté)."""
        task_ids = {t.task_id for t in self.dag.tasks}
        expected = {
            "wait_for_sirene_s3",
            "run_sirene_spark_transform",
            "count_before_refresh",
            "refresh_snowpipe",
            "validate_raw_data_loaded",
        }
        missing = expected - task_ids
        assert not missing, f"Tâches manquantes dans dag_04 : {missing}"

    def test_databricks_connection_id(self):
        task = self.dag.get_task("run_sirene_spark_transform")
        assert task is not None
        assert task.databricks_conn_id == "databricks_sirene"

    def test_s3sensor_soft_fail(self):
        task = self.dag.get_task("wait_for_sirene_s3")
        assert task.soft_fail is True

    def test_dependency_s3_before_spark(self):
        s3_task = self.dag.get_task("wait_for_sirene_s3")
        downstream = {t.task_id for t in s3_task.downstream_list}
        assert "run_sirene_spark_transform" in downstream


class TestDag05:
    """Tests structurels de dag_05_sirene_pipeline_complet (11 tâches)."""

    @pytest.fixture(autouse=True)
    def setup(self, dagbag):
        self.dag = dagbag.get_dag("dag_05_sirene_pipeline_complet")

    def test_dag_exists(self):
        assert self.dag is not None, "dag_05_sirene_pipeline_complet introuvable"

    def test_has_cron_schedule(self):
        """Schedule CRON actif (pas NullTimetable)."""
        timetable_name = type(self.dag.timetable).__name__
        assert "Null" not in timetable_name, (
            f"dag_05 sans schedule CRON — timetable : {timetable_name}"
        )

    def test_max_active_runs_is_one(self):
        """Pipeline stateful : un seul run simultané."""
        assert self.dag.max_active_runs == 1

    def test_minimum_task_count(self):
        """11 tâches en J4 final (>= 10 pour tolérer une variation mineure)."""
        count = len(self.dag.tasks)
        assert count >= 10, f"Attendu >= 10 tâches, trouvé {count}"

    def test_has_dbt_taskgroups(self):
        """Présence des TaskGroups via préfixe des task_ids."""
        task_ids = {t.task_id for t in self.dag.tasks}
        staging = [tid for tid in task_ids if tid.startswith("dbt_staging.")]
        marts   = [tid for tid in task_ids if tid.startswith("dbt_marts.")]
        assert staging, "Aucune tâche préfixée dbt_staging.*"
        assert marts,   "Aucune tâche préfixée dbt_marts.*"

    def test_has_elementary_task(self):
        task_ids = {t.task_id for t in self.dag.tasks}
        assert "run_elementary_report" in task_ids

    def test_has_databricks_task(self):
        task = self.dag.get_task("run_sirene_spark_transform")
        assert task is not None
        assert task.databricks_conn_id == "databricks_sirene"

    # NOTE : ne pas tester retry_exponential_backoff ni max_retry_delay
    # Ces paramètres ont été retirés en J4.
    # dag_05 actuel : retries=1, retry_delay=timedelta(seconds=30) seulement.
