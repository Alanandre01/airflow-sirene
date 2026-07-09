# airflow-sirene

Pipeline de données orchestré avec Apache Airflow - données SIRENE (Loire-Atlantique, 44) partitionnées dans S3, transformées via dbt sur Snowflake, avec un pipeline analytique complémentaire PySpark/Delta Lake sur Databricks.

## Stack

- **Apache Airflow 3.2.1** (Docker Compose - CeleryExecutor + Redis + PostgreSQL)
- **dbt Core** (dbt-snowflake) - transformation
- **Snowflake** - entrepôt de données (`ALAN_DW`)
- **AWS S3** - data lake (`alan-data-lake-fr`)
- **PySpark / Delta Lake sur Databricks Free Edition** - pipeline analytique complémentaire (`notebooks/`)

## Démarrer

```powershell
docker compose up -d
```

Interface web : http://localhost:8080 (`airflow` / `airflow`)

Voir `CLAUDE.md` pour les commandes courantes, la configuration détaillée et les conventions du Dockerfile.

## Structure

| Dossier | Contenu |
|---|---|
| `dags/` | DAGs Airflow |
| `plugins/` | Opérateurs/hooks custom |
| `notebooks/` | Notebooks PySpark/Delta Lake (Databricks - pipeline séparé, voir `notebooks/README.md`) |
| `config/`, `keys/`, `.env` | Configuration et secrets locaux (gitignored) |

## DAGs

| dag_id | Description |
|---|---|
| `dag_01_sirene_etl_demo` | Démo TaskFlow API / XComs (extract → transform → load simulés) |
| `dag_02_sirene_branch_demo` | Branchement conditionnel via `BranchPythonOperator` |
| `dag_03_sirene_s3_dbt_pipeline` | Pipeline réel : `S3KeySensor` → `dbt deps` → `dbt run` → `dbt test` |

## Projet source

Orchestre le pipeline dbt `sirene_nantes`.
