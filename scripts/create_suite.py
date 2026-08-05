import great_expectations as gx
import pandas as pd

# 1. Charger le CSV exporté
df = pd.read_csv("data/sirene_sample.csv", dtype=str)
print(f"[OK]  {len(df):,} lignes — colonnes : {df.columns.tolist()}\n")

# 2. Contexte GE (lit gx/ créé par ge_init.py)
context = gx.get_context()

# 3. Source de données Pandas in-memory
ds = context.data_sources.add_pandas(name="sirene_pandas")
asset = ds.add_dataframe_asset(name="sirene_etablissements")
batch_def = asset.add_batch_definition_whole_dataframe("batch_complet")

# 4. Expectation Suite
suite = context.suites.add(gx.ExpectationSuite(name="sirene_etablissements_suite"))

# --- Identité : colonnes structurantes ---

# SIRET : exactement 14 caractères (filtre déjà appliqué par la vue staging)
suite.add_expectation(
    gx.expectations.ExpectColumnValueLengthsToEqual(column="SIRET", value=14)
)
# SIREN : identifiant entreprise — ne peut pas être null
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="SIREN")
)
# ETAT_ETABLISSEMENT (staging renomme ETAT_ADMIN_ETAB) : valeurs réelles
# 'Actif'/'Fermé' (confirmées sur l'export) — pas les codes 'A'/'F' bruts
# INSEE ; la vue dbt fait un passthrough direct, aucun recodage.
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="ETAT_ETABLISSEMENT", value_set=["Actif", "Fermé"]
    )
)

# --- Qualité : anomalie NULL chiffrée à ~75% en J1 ---
# mostly=0.25 = au moins 25% des valeurs doivent être non-null pour passer
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="ETAT_ETABLISSEMENT", mostly=0.25
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="CODE_POSTAL", mostly=0.25
    )
)

# --- Expectation STRICTE intentionnelle (doit échouer) ---
# DATE_CREATION_ETAB (staging renomme DATE_CREATION_ETAB_PARSED) est à 100%
# NULL : TRY_TO_DATE échoue sur le format epoch µs brut de DATE_CREATION_ETAB
# côté RAW — problème silencieux jusqu'ici
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="DATE_CREATION_ETAB"
    )
)

print("[INFO] Suite sirene_etablissements_suite créée — 6 expectations\n")

# 5. Validation Definition (lie la Suite au batch de données)
vd = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="sirene_validation_j2",
        data=batch_def,
        suite=suite,
    )
)

# 6. Lancer la validation
print("[INFO] Validation en cours...")
result = vd.run(batch_parameters={"dataframe": df})

# 7. Afficher les résultats
print("\n=== RÉSULTATS ===")
for r in result.results:
    col = r.expectation_config.kwargs.get("column", "?")
    etype = r.expectation_config.type
    ok = "PASS" if r.success else "FAIL"
    line = f"  [{ok}]  {col}  —  {etype}"
    if not r.success:
        pct = r.result.get("unexpected_percent", None)
        if pct is not None:
            line += f"  ({pct:.1f}% de valeurs inattendues)"
    print(line)

bilan = "PASS" if result.success else "ECHEC PARTIEL — attendu pour DATE_CREATION_ETAB"
print(f"\nBilan : {bilan}")
print("Suite sauvegardée → gx/expectations/sirene_etablissements_suite.json")
