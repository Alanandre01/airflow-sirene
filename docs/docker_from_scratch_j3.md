# Test from-scratch — J3

Objectif : vérifier qu'un environnement peut être reconstruit uniquement à partir du repo + `.env.example` + secrets externes (RSA key, credentials AWS/Snowflake/Databricks), et documenter précisément ce qui doit être refait manuellement après un `docker compose down -v`.

## Procédure exécutée

```powershell
docker compose down -v          # supprime les containers + le volume nommé postgres-db-volume
cp .env.example .env            # puis compléter les placeholders avec les vraies valeurs
docker compose up -d
# attente ~35s : tous les services healthy
docker compose exec airflow-apiserver python -m pytest /opt/airflow/tests/ -v --tb=short
# 40 passed
```

**Temps total** (down -v → stack healthy + suite pytest verte) : **~3 min 30s**.

## Ce qui est réellement détruit par `docker compose down -v`

Un seul volume nommé existe dans `docker-compose.yaml` : `postgres-db-volume`. C'est le **seul** artefact supprimé par le `-v`. Tous les autres montages (`dags/`, `logs/`, `config/`, `plugins/`, `gx/`, `keys/`, `tests/`, le projet dbt `sirene_nantes`, `.dbt`) sont des **bind mounts** vers le filesystem hôte — ils ne sont jamais affectés par `down -v`.

Correction par rapport au guide générique suivi initialement : **`keys/rsa_key.p8` n'a pas besoin d'être recréée**. Vérifié après le `up -d` — la clé RSA et le projet `sirene_nantes` sont toujours présents et montés correctement, sans aucune action.

## Actions manuelles résiduelles (perdues avec le volume Postgres)

La perte du volume Postgres efface toutes les métadonnées Airflow : connexions, variables, historique des DAG runs. L'utilisateur admin web (`airflow`/`airflow`) est recréé automatiquement par `airflow-init` (`_AIRFLOW_WWW_USER_CREATE: 'true'`) — pas d'action requise sur ce point.

À recréer manuellement après chaque `down -v` :

### Connexions

```bash
airflow connections add snowflake_sirene \
  --conn-type snowflake \
  --conn-login ALANANDRE19 \
  --conn-extra '{"account": "XQDSVIL-TT19138", "warehouse": "ALAN_WH", "database": "ALAN_DW", "role": "SYSADMIN", "private_key_file": "/opt/airflow/keys/rsa_key.p8"}'

airflow connections add databricks_sirene \
  --conn-type databricks \
  --conn-host https://dbc-56faa73b-9332.cloud.databricks.com \
  --conn-password <DATABRICKS_TOKEN>
```

### Variables

```bash
airflow variables set databricks_job_id 453432341513798
airflow variables set environment dev
airflow variables set sirene_data_available true
```

## Complétude de `.env.example`

Toutes les valeurs nécessaires pour reconstruire un `.env` fonctionnel avaient un placeholder correspondant dans `.env.example` (`AIRFLOW_UID`, `FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`/`JWT_ISSUER`, `AWS_ACCESS_KEY_ID`/`SECRET`, `SNOWFLAKE_*`, `DBT_HOME`, `SIRENE_NANTES_DIR`, `DATABRICKS_HOST`/`TOKEN`) — aucune variable manquante détectée. `SNOWFLAKE_PASSWORD` en est volontairement absent (variable morte, voir `docker_audit_j2.md` et le commit associé) : laissée en commentaire explicite plutôt que documentée comme requise.

## Résultat

Stack saine, 40/40 tests pytest passés, aucune erreur d'import DAG après reconstruction complète from-scratch.
