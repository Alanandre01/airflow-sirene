from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ══ SILVER — Nettoyage + contraintes de qualité (expectations) ══
@dp.table(
    comment="Établissements SIRENE actifs — couche silver",
    table_properties={"quality": "silver"},
)
@dp.expect("siret_non_null", "siret IS NOT NULL")
@dp.expect_or_drop("statut_valide", "statut_diffusion != 'P'")
@dp.expect_or_drop("etablissement_actif", "etat_admin_etab = 'Actif'")
def sirene_clean():
    """
    spark.readStream.table() : lecture streaming depuis Bronze.
    @dp.expect_or_drop : filtre les lignes RGPD + établissements fermés.
    """
    return (
        spark.readStream.table("sirene_raw")
        .select(
            "siret", "siren", "nic",
            "etat_admin_etab", "commune", "code_postal",
            "activite_principale_etab", "categorie_entreprise",
            "etablissement_siege",
        )
        .withColumns({
            "loaded_at": F.current_timestamp(),
            "est_siege": F.when(F.col("etablissement_siege") == "oui", True).otherwise(False)
        })
    )