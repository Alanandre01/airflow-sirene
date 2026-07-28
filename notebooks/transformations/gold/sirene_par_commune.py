from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ══ GOLD — Agrégation batch (spark.read.table(), pas readStream) ══
@dp.materialized_view(
    comment="Établissements actifs par commune — gold (agrégat)",
    table_properties={"quality": "gold"},
)
def sirene_par_commune():
    """
    spark.read.table() BATCH obligatoire pour les agrégations (pas de streaming).
    """
    return (
        spark.read.table("sirene_clean")
        .groupBy("code_postal", "commune", "categorie_entreprise")
        .agg(F.count("siret").alias("nb_etablissements"))
        .orderBy(F.desc("nb_etablissements"))
    )