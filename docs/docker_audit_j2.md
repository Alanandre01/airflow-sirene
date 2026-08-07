# Docker Compose — points à durcir (J3)

Audit effectué le 2026-08-07 (lecture complète de `docker-compose.yaml`, cf. commande
`docker compose config --quiet` + `docker compose ps` — config valide, 7/7 healthy).

Le planning J2 supposait des health checks et restart policies absents. **Ce n'est pas
le cas** — les deux sont déjà en place sur tous les services long-lived. Les vrais gaps
sont ailleurs (voir plus bas).

## Health checks

- [x] postgres              : présent — `pg_isready -U airflow`
- [x] redis                 : présent — `redis-cli ping`
- [x] airflow-apiserver     : présent — `curl --fail http://localhost:8080/api/v2/monitor/health`
- [x] airflow-scheduler     : présent — `curl --fail http://localhost:8974/health`
- [x] airflow-dag-processor : présent — `airflow jobs check --job-type DagProcessorJob`
- [x] airflow-worker        : présent — `celery ... inspect ping`
- [x] airflow-triggerer     : présent — `airflow jobs check --job-type TriggererJob`
- [x] flower (profil `flower`) : présent — `curl --fail http://localhost:5555/`
- [ ] airflow-init (one-shot) : pas de healthcheck — normal, service à exécution unique (`service_completed_successfully`)
- [ ] airflow-cli (profil `debug`) : pas de healthcheck — normal, exécution à la demande

**Rien à ajouter ici en J3.**

## Restart policies

- [x] `restart: always` présent sur les 8 mêmes services que les health checks
      (postgres, redis, airflow-apiserver, airflow-scheduler, airflow-dag-processor,
      airflow-worker, airflow-triggerer, flower)
- [ ] **Point à trancher en J3** : `always` vs `unless-stopped`. `always` relance les
      containers même après un `docker stop <container>` manuel (seul `docker compose down`
      les arrête pour de bon) — potentiellement surprenant en dev local. `unless-stopped`
      respecte un arrêt manuel explicite. Décider si c'est le comportement voulu ou si
      `unless-stopped` est plus adapté à ce projet (dev local, pas un déploiement prod).
- [ ] airflow-init / airflow-cli : pas de restart policy — normal (one-shot / à la demande)

## Variables d'environnement

- [x] `.env.example` présent (pas absent comme supposé), mais **incomplet** :
  - [ ] `SNOWFLAKE_PASSWORD` manquante dans `.env.example` alors que
        `docker-compose.yaml` l'injecte dans l'environnement de tous les services
        Airflow (`environment.SNOWFLAKE_PASSWORD: ${SNOWFLAKE_PASSWORD}`). Présente
        dans le `.env` local. Probable variable vestige d'avant le passage à l'auth
        RSA (CLAUDE.md ne documente que l'auth par clé, aucune connexion/profil dbt
        n'utilise de password). **Décision à prendre en J3** : documenter dans
        `.env.example`, ou supprimer la ligne de `docker-compose.yaml` si confirmée
        morte.
  - [ ] `AIRFLOW__API_AUTH__JWT_ISSUER` non listée dans `.env.example` (a un défaut
        dans `docker-compose.yaml` via `${AIRFLOW__API_AUTH__JWT_ISSUER:-airflow}`,
        donc pas bloquant, mais absente de la doc).

## Volumes

- [x] `gx/` monté : `${AIRFLOW_PROJ_DIR:-.}/gx:/opt/airflow/gx` (hérité par tous les
      services via l'ancre `x-airflow-common`), existe bien localement
      (`gx/checkpoints/`).
- [ ] **Chemin Windows en dur** (nouveau gap, absent du planning initial) : ligne
      `C:/Users/alana/Documents/de-leaning/sirene_nantes:/opt/airflow/dbt/sirene_nantes`
      — contrairement aux autres volumes (`${AIRFLOW_PROJ_DIR:-.}`, `${DBT_HOME}`),
      ce chemin n'est paramétrable par aucune variable d'env. Bloque la
      reproductibilité du setup sur une autre machine/OS. À paramétrer en J3
      (ex. `${SIRENE_NANTES_DIR}`).
- [ ] Tester from-scratch en J3 : `docker compose down -v` → `up` → noter les étapes
      manuelles nécessaires (créer `.env` depuis `.env.example`, `keys/rsa_key.p8`,
      `gx/` déjà versionné ou pas, etc.)

## Autre gap relevé

- [ ] `airflow-cli` (profil `debug`) ne dépend pas de `airflow-init` dans son
      `depends_on` (contrairement à tous les autres services) — un
      `docker compose --profile debug run airflow-cli ...` pourrait s'exécuter avant
      que la DB soit migrée / l'utilisateur admin créé. À corriger en J3 si le profil
      debug est réellement utilisé.

## Plan J3

1. Trancher `restart: always` vs `unless-stopped`.
2. `.env.example` : ajouter `SNOWFLAKE_PASSWORD` (ou la retirer de
   `docker-compose.yaml` si morte) et `AIRFLOW__API_AUTH__JWT_ISSUER`.
3. Paramétrer le chemin en dur de `sirene_nantes` via une variable d'env.
4. Ajouter `airflow-init` à `depends_on` de `airflow-cli`.
5. Test from-scratch documenté dans ce fichier (`docker compose down -v` → `up`).
