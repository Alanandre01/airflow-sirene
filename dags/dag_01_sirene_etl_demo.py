from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id="dag_01_sirene_etl_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sirene", "xcom", "s1"],
    doc_md="""
## DAG 01 — Hello SIRENE avec XComs
Démontre le passage de données entre tâches via la TaskFlow API.
Source simulée : alan-data-lake-fr/sirene/raw/sirene.parquet
Cible simulée  : ALAN_DW.MARTS.mart_etablissements_par_commune
    """,
)
def dag_01_sirene_etl_demo():

    @task
    def extraire() -> dict:
        """
        Simule l'extraction de métadonnées depuis S3.
        La valeur retournée est stockée automatiquement en XCom
        sous la clé 'return_value' de cette tâche.
        """
        metadata = {
            "nb_lignes": 45320,
            "source": "alan-data-lake-fr/sirene/raw/sirene.parquet",
            "date_fichier": "2024-01-15",
        }
        print(f"[EXTRACT] {metadata['nb_lignes']} établissements détectés dans S3")
        return metadata  # → XCom automatique vers les tâches en aval

    @task
    def transformer(metadata: dict) -> int:
        """
        Simule la transformation dbt (filtre ETAT_ADMIN_ETAB = 'A').
        Reçoit le dict de extraire() via XCom (paramètre Python normal).
        """
        nb_total = metadata["nb_lignes"]
        # ~73 % des établissements sont actifs dans SIRENE Loire-Atlantique (44)
        nb_actifs = int(nb_total * 0.73)
        print(f"[TRANSFORM] {nb_total} lignes brutes → {nb_actifs} établissements actifs")
        print(f"[TRANSFORM] Source : {metadata['source']}")
        return nb_actifs  # → XCom vers charger()

    @task
    def charger(nb_actifs: int) -> None:
        """
        Simule le chargement dans ALAN_DW.MARTS.
        Lève une ValueError si aucun établissement actif (garde-fou).
        """
        if nb_actifs == 0:
            raise ValueError("Aucun établissement actif détecté — chargement annulé")
        print(f"[LOAD] {nb_actifs} enregistrements → ALAN_DW.MARTS.mart_etablissements_par_commune")
        print("[LOAD] Chargement simulé avec succès")

    # ── Dépendances : TaskFlow API les gère automatiquement via les paramètres ──
    metadata = extraire()
    nb = transformer(metadata)
    charger(nb)


dag_01_sirene_etl_demo()
