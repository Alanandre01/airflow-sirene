import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    """
    Charge tous les DAGs depuis /opt/airflow/dags.
    scope="session" : chargé une seule fois pour l'ensemble de la suite.

    Prérequis : chaque Variable.get() dans les DAGs doit avoir
    un default_var= pour éviter AirflowNotFoundException à l'import
    si la DB Airflow n'est pas accessible depuis ce contexte.
    """
    return DagBag(
        dag_folder="/opt/airflow/dags",
        include_examples=False,
    )
