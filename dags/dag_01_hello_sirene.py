from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id="dag_01_hello_sirene",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sirene", "apprentissage", "m3-s1"],
    doc_md="""
    ## DAG 01 — Hello SIRENE
    Premier DAG d'apprentissage Mois 3.
    Simule les 3 étapes du pipeline SIRENE :
    extraction métadonnées → validation → journalisation.
    Les données transitent entre tâches via XComs (TaskFlow API).
    """,
)
def dag_01_hello_sirene():

    @task()
    def extraire_metadata() -> dict:
        """Simule l'extraction de métadonnées depuis ALAN_DW.RAW.SIRENE_ETABLISSEMENTS."""
        metadata = {
            "table": "ALAN_DW.RAW.SIRENE_ETABLISSEMENTS",
            "departement": "44",
            "nb_lignes_estimees": 120_000,
            "source": "data.gouv.fr / SIRENE Loire-Atlantique",
        }
        print(f"[extraire_metadata] Table       : {metadata['table']}")
        print(f"[extraire_metadata] Lignes      : {metadata['nb_lignes_estimees']:,}")
        print(f"[extraire_metadata] Département : {metadata['departement']}")
        return metadata

    @task()
    def valider_donnees(metadata: dict) -> int:
        """Valide la cohérence des métadonnées avant de lancer le pipeline réel."""
        nb   = metadata["nb_lignes_estimees"]
        dept = metadata["departement"]

        if nb < 1_000:
            raise ValueError(
                f"Nombre de lignes insuffisant : {nb}. "
                "Seuil minimum : 1 000. Pipeline interrompu."
            )
        if dept != "44":
            raise ValueError(
                f"Département inattendu : {dept}. Attendu : 44 (Loire-Atlantique)."
            )

        print(f"[valider_donnees] ✅ Validation OK — {nb:,} lignes, dpt {dept}")
        return nb

    @task()
    def journaliser_run(nb_lignes: int) -> None:
        """Journalise la fin du run."""
        print("[journaliser_run] ✅ Run terminé avec succès.")
        print(f"[journaliser_run]    Lignes validées : {nb_lignes:,}")
        print("[journaliser_run]    Prochaine étape : pipeline SIRENE complet (S3).")

    meta = extraire_metadata()
    nb   = valider_donnees(meta)
    journaliser_run(nb)


dag_01_hello_sirene()
