# airflow-sirene

Pipeline Data Engineering orchestré - Airflow, PySpark, Snowflake, dbt, Docker

![Airflow CI](https://github.com/Alanandre01/airflow-sirene/actions/workflows/airflow_ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE)

Pipeline end-to-end orchestrant les données SIRENE (établissements actifs,
source : [data.gouv.fr](https://www.data.gouv.fr)).
Stack : **S3 → PySpark/Databricks → Snowflake → dbt → Elementary**.

---

## Architecture

```mermaid
flowchart LR
    S3[(S3\nalan-data-lake-fr)]
    S3 --> S1[S3KeySensor]
    S1 --> A[attendre_fichier_s3]
    A --> B[spark_transform_delta\nDatabricks Job]
    B --> DL[Delta Lake]
    DL --> C[refresh_snowpipe\nSnowflake]
    C --> D[dbt_staging\nrun + test]
    D --> E[dbt_marts\nrun + test]
    E --> F[elementary_report\nqualité données]
    F --> G[check_volume_monitoring\nSnowflake]
```

---

## Stack technique

| Outil | Version | Rôle |
|-------|---------|------|
| Apache Airflow | 3.2.1 | Orchestration des DAGs |
| PySpark / Databricks | 3.5 | Transformation distribuée |
| Delta Lake | - | Stockage analytique (ACID, Time Travel) |
| Snowflake | - | Data warehouse |
| dbt Core | - | Modélisation (staging + intermediate + marts) |
| Elementary | - | Qualité et observabilité des données |
| AWS S3 | eu-west-1 | Stockage fichiers source |
| Docker Desktop | - | Environnement local Airflow |
| GitHub Actions | - | CI/CD - validation syntaxe DAGs |

---

## Setup local

```powershell
git clone https://github.com/Alanandre01/airflow-sirene.git
cd airflow-sirene
cp .env.example .env   # Renseigner SNOWFLAKE_*, AWS_*, DATABRICKS_*, FERNET_KEY (voir CLAUDE.md)
docker compose up -d
# Ouvrir http://localhost:8080  (airflow / airflow)
# Déclencher dag_06_sirene_pipeline_v2 depuis l'interface Airflow
```

---

## DAGs disponibles

| DAG | Tâches | Description |
|-----|--------|-------------|
| `dag_01_sirene_etl_demo` | 3 | TaskFlow API + XComs implicites |
| `dag_02_sirene_branch_demo` | 4 | BranchPythonOperator + trigger_rule |
| `dag_03_sirene_s3_dbt_pipeline` | 4 | S3KeySensor → dbt run + test |
| `dag_04_sirene_databricks` | 5 | S3 → Databricks → Snowpipe → validation RAW |
| `dag_05_sirene_pipeline_complet` | 11 | Pipeline complet - schedule `0 5 1 * *` |
| `dag_06_sirene_pipeline_v2` | 10 | Variante allégée - schedule hebdomadaire, callbacks, `max_active_runs=1` |

---

## Great Expectations

Validation de qualité à deux niveaux via Great Expectations v1.19.1.

### En production - `dag_06`

Le Checkpoint `sirene_checkpoint_dag06` est intégré dans `dag_06_sirene_pipeline_v2`,
entre la tâche `refresh_snowpipe` et les transformations dbt.

- **Mode :** `file` - le répertoire `gx/` est monté en volume dans les conteneurs Airflow
- **Action :** `UpdateDataDocsAction` - les Data Docs sont régénérés automatiquement à chaque run
- **Résultats :** `gx/uncommitted/data_docs/local_site/index.html`

### En CI - GitHub Actions

Un Checkpoint distinct en mode `ephemeral` s'exécute via `scripts/run_ge_ci.py`
sur une fixture CSV versionnée (`tests/fixtures/sirene_fixture.csv`, ≈ 50 lignes).

- **Objectif :** valider la logique des expectations sans connexion Snowflake
- **CI entièrement offline** : aucun secret GitHub requis pour cette étape
- **`continue-on-error: true`** : les échecs GE sont remontés sans bloquer la CI

> Ces deux mécanismes sont intentionnellement distincts :
> le mode production valide la qualité réelle des données Snowflake,
> le mode CI valide la logique des expectations en isolation.

---

## RGPD

- **Filtre SIRENE** : `statut_diffusion = 'O'` - ne conserve que les établissements
  diffusables publiquement (Art. 21 RGPD).
- **Rôles Snowflake séparés** (voir `GOVERNANCE.md` du repo `sirene_nantes`) : `TRANSFORMER` (dbt CI/CD),
  `ANALYST` (lecture marts), `SYSADMIN` (administration).
- **Rétention** : `DATA_RETENTION_TIME_IN_DAYS` par table - voir
  `GOVERNANCE.md` (repo sirene_nantes).
- **Audit** : table `ALAN_DW.RAW.RGPD_AUDIT_LOG` +
  procédure `ANONYMISER_ETABLISSEMENT`.

---

## Tests

```bash
docker compose exec airflow-apiserver python -m pytest /opt/airflow/tests/ -v --tb=short
```

40 tests pytest de validation structurelle (`DagBag`) : absence d'erreurs d'import, DAGs
attendus, tags, task_ids, dépendances, `max_active_runs`, TaskGroups dbt.

---

## Infrastructure as Code (Terraform)

L'infrastructure est décrite via Terraform v1.15.8 dans `terraform/`.

### Ressources gérées

| Ressource | Type Terraform | Identifiant |
|-----------|---------------|-------------|
| Bucket S3 | `aws_s3_bucket` | `alan-data-lake-fr` (eu-west-1) |
| Warehouse Snowflake | `snowflake_warehouse` | `ALAN_WH` (X-Small) |

> **Note :** ces ressources existaient avant l'introduction de Terraform.
> Elles ont été importées dans le state (`terraform import`) -
> aucune ressource n'a été créée par Terraform à ce stade.

### Structure

    terraform/
    ├── main.tf                  # Providers : hashicorp/aws ~> 5.0, Snowflake-Labs/snowflake ~> 0.94
    ├── variables.tf             # aws_region, snowflake_account, snowflake_username, snowflake_role
    ├── outputs.tf               # s3_bucket_id, s3_bucket_arn, snowflake_warehouse_name/size
    ├── backend.tf               # State local
    ├── s3.tf                    # aws_s3_bucket.alan_data_lake_fr
    ├── snowflake.tf             # snowflake_warehouse.alan_wh
    └── terraform.tfvars.example # Template de configuration (sans valeurs réelles)

### Commandes

```bash
# Initialiser les providers
terraform init

# Vérifier les changements avant toute modification
terraform plan -var-file="terraform.tfvars"

# Appliquer (idempotent si le state est synchronisé)
terraform apply -var-file="terraform.tfvars"

# Consulter les outputs enregistrés dans le state
terraform output
```

