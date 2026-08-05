import os

import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

# Clé RSA — même configuration que profiles.yml de sirene_nantes (pas de passphrase, MFA oblige l'auth par clé)
key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

with open(key_path, "rb") as f:
    p_key = serialization.load_pem_private_key(
        f.read(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="DBT_DEV_STAGING",
    role=os.environ["SNOWFLAKE_ROLE"],
)

# Vue staging dbt (stg_sirene_etablissements) — colonnes renommées/nettoyées par rapport au RAW,
# voir models/staging/stg_sirene_etablissements.sql dans le projet dbt sirene_nantes.
# date_creation_etab vient de DATE_CREATION_ETAB_PARSED (NULL attendu élevé) ;
# etat_etablissement NULL ~70% (artefact de schema evolution Delta, documenté en warn dans le .yml dbt).
query = """
SELECT
    SIRET, SIREN, NIC,
    ETAT_ETABLISSEMENT, DATE_CREATION_ETAB,
    CODE_POSTAL, COMMUNE,
    STATUT_DIFFUSION, EST_SIEGE, EST_EMPLOYEUR
FROM alan_dw.dbt_dev_staging.stg_sirene_etablissements
ORDER BY RANDOM()
LIMIT 10000
"""

print("[INFO] Connexion Snowflake (SYSADMIN / ALAN_WH / DBT_DEV_STAGING)...")
df = pd.read_sql(query, conn)
conn.close()

os.makedirs("data", exist_ok=True)
df.to_csv("data/sirene_sample.csv", index=False)

print(f"[OK]  {len(df):,} lignes exportées → data/sirene_sample.csv")
print(f"\nColonnes retournées (noms exacts à utiliser dans create_suite.py) :")
print(df.columns.tolist())
print(f"\nTaux de NULL par colonne :")
print(df.isnull().sum().to_string())
