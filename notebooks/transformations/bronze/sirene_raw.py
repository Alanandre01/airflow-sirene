from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ══ BRONZE — Ingestion incrémentale via Auto Loader (cloudFiles) ══
@dp.table(
    comment="Établissements SIRENE bruts — Auto Loader incrémental",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
    },
)
def sirene_raw():
    """
    Auto Loader détecte automatiquement les nouveaux fichiers.
    cloudFiles.schemaLocation : obligatoire pour l'inférence + évolution de schéma.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", ";")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/workspace/default/raw_data/schema_dlt/",
        )
        .load("/Volumes/workspace/default/raw_data/sirene/")
    )
    
    # Rename columns to match expected schema
    return df.selectExpr(
        "SIRET as siret",
        "SIREN as siren",
        "NIC as nic",
        "`Statut de diffusion de l'établissement` as statut_diffusion",
        "`Etat administratif de l'établissement` as etat_admin_etab",
        "`Code postal de l'établissement` as code_postal",
        "`Commune de l'établissement` as commune",
        "`Activité principale de l'établissement` as activite_principale_etab",
        "`Catégorie de l'entreprise` as categorie_entreprise",
        "`Etablissement siège` as etablissement_siege"
    )