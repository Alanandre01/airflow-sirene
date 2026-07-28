# Notebooks Databricks - M3 SIRENE

Notebooks PySpark / Delta Lake / Delta Live Tables / MLflow sur Databricks Free Edition (Azure, serverless).

## Contenu

| # | Fichier | Thème | Exécutable |
|---|---------|-------|------------|
| 01 | `01_exploration_sirene.ipynb` | Exploration CSV SIRENE (420 411 lignes, 107 colonnes) | ✅ |
| 02 | `02_transformations_sirene.ipynb` | Nettoyage PySpark : rename_map, clean_nd, filtres RGPD | ✅ |
| 03 | `03_delta_lake_sirene.ipynb` | Delta Lake : WRITE / MERGE / Time Travel / Schema Evolution | ✅ |
| 04 | `04_pyspark_avance_sirene.ipynb` | Window functions, broadcast join, Spark SQL | ✅ |
| 05 | `05_pyspark_optimisation_sirene.ipynb` | OPTIMIZE, ZORDER, VACUUM, .explain(), Photon, AQE | ✅ |
| 06 | `transformations/` (bronze/silver/gold) | Pipeline Delta Live Tables - Auto Loader, expectations, agrégat | ✅ Pipeline DLT |
| 07 | `07_mlflow_intro.ipynb` | MLflow experiment tracking sur `sirene_clean_delta` | ✅ |

## Notes

- Notebooks 01–05, 07 : exécutables en notebook classique (serverless compute Databricks).
- `transformations/` : pipeline **Delta Live Tables** déclaratif (Workflows → Delta Live Tables), pas un notebook - trois fichiers Python décorés `@dp.table` / `@dp.materialized_view` :
  - `bronze/sirene_raw.py` - ingestion incrémentale via Auto Loader (`cloudFiles`) depuis `/Volumes/workspace/default/raw_data/sirene/`, renommage des colonnes SIRENE.
  - `silver/sirene_clean.py` - lecture streaming de bronze, contraintes de qualité (`@dp.expect_or_drop` : dédiffusion RGPD, établissements actifs uniquement).
  - `gold/sirene_par_commune.py` - vue matérialisée batch (`spark.read.table`), agrégat nb d'établissements par commune/catégorie.
- Notebook 07 : MLflow pré-installé dans Databricks Free Edition, tracking URI automatique.

## Données source

Table Delta : `/Volumes/workspace/default/raw_data/sirene_clean_delta`
134 666 lignes · 28 colonnes · partitionné par `categorie_entreprise`
