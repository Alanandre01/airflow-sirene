# airflow-sirene

Pipeline de données orchestré avec Apache Airflow - données SIRENE (Loire-Atlantique, 44) partitionnées dans S3, transformées via dbt sur Snowflake, avec un pipeline analytique complémentaire PySpark/Delta Lake sur Databricks.

## Stack

- **Apache Airflow 3.2.1** (Docker Compose - CeleryExecutor + Redis + PostgreSQL)
- **dbt Core** (dbt-snowflake) - transformation
- **Snowflake** - entrepôt de données (`ALAN_DW`)
- **AWS S3** - data lake (`alan-data-lake-fr`)
- **PySpark / Delta Lake sur Databricks Free Edition** - pipeline analytique complémentaire (`notebooks/`)
- **DatabricksRunNowOperator · SQLExecuteQueryOperator · SnowflakeHook · TaskGroup** - orchestration Airflow
- **pytest + DagBag** - tests de validation structurelle des DAGs

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
| `tests/` | Tests pytest de validation structurelle des DAGs (`DagBag`) |
| `notebooks/` | Notebooks PySpark/Delta Lake (Databricks - pipeline séparé, voir `notebooks/README.md`) |

## DAGs

| dag_id | Description | Schedule |
|---|---|---|
| `dag_01_sirene_etl_demo` | Démo TaskFlow API / XComs (extract → transform → load simulés) | Manuel |
| `dag_02_sirene_branch_demo` | Branchement conditionnel via `BranchPythonOperator` | Manuel |
| `dag_03_sirene_s3_dbt_pipeline` | Pipeline réel : `S3KeySensor` → `dbt deps` → `dbt run` → `dbt test` | Manuel |
| `dag_04_sirene_databricks` | S3 → Spark → Snowpipe → Validation RAW | Manuel |
| `dag_05_sirene_pipeline_complet` | Pipeline complet end-to-end + dbt + Elementary | `0 5 1 * *` |

## Tests

```bash
docker compose exec airflow-apiserver python -m pytest /opt/airflow/tests/ -v --tb=short
```

## Projet source

Orchestre le pipeline dbt `sirene_nantes`.
