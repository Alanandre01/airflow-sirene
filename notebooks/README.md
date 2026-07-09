# Notebooks PySpark

Pipeline analytique SIRENE Loire-Atlantique - PySpark + Delta Lake sur Databricks Free Edition (Azure).

## Structure

| Notebook | Rôle | Lignes en sortie |
|---|---|---|
| `01_exploration_sirene.ipynb` | Lecture CSV brut, schéma, distributions | 420 411 (brut) |
| `02_transformations_sirene.ipynb` | `clean_nd`, filtres RGPD (Art.25), colonnes dérivées | 134 661 (clean) |
| `03_delta_lake_sirene.ipynb` | WRITE, MERGE, Time Travel, Schema evolution, Widgets Databricks | 134 666 (avec upserts) |
| `04_pyspark_avance_sirene.ipynb` | S3 (boto3), Window functions, Broadcast join | 680 (grain commune×NAF) |

## Stack technique

- **PySpark 3.5** - DataFrame API, Window functions, Broadcast join
- **Delta Lake** - ACID, MERGE upsert, Time Travel, Schema evolution
- **Databricks Free Edition** - Compute serverless, Unity Catalog Volumes
- **Données** - SIRENE data.gouv.fr, Loire-Atlantique (44), 420 411 établissements

## Intégration pipeline

Le notebook `03_delta_lake_sirene` est exposé comme **Databricks Job** (`sirene_spark_transform`).
Il accepte deux paramètres Widgets injectés par Airflow (`DatabricksRunNowOperator`) :
- `env` - environnement cible (`dev` / `prod`)
- `date_partition` - partition de données à traiter (`YYYY-MM`)

## Tables Delta produites

| Table | Chemin Volumes | Partitions |
|---|---|---|
| `sirene_clean_delta` | `/Volumes/workspace/default/raw_data/sirene_clean_delta` | `categorie_entreprise` |
| `sirene_analytique_delta` | `/Volumes/workspace/default/raw_data/sirene_analytique_delta` | `code_departement` |
