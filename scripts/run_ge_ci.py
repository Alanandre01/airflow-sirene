"""
scripts/run_ge_ci.py
Validation GE offline pour GitHub Actions.
Mode ephemeral — pas de gx/ requis, pas de connexion Snowflake.
Suite et WARN_EXPECTATIONS = mirroir exact de run_ge_checkpoint_sirene
(dags/dag_06_sirene_pipeline_v2.py) : SIRET/SIREN/value-set = bloquant,
null-checks CODE_POSTAL/DATE_CREATION_ETAB/ETAT_ETABLISSEMENT = warn.
"""
import sys
import great_expectations as gx
import pandas as pd

FIXTURE = "tests/fixtures/sirene_fixture.csv"

WARN_EXPECTATIONS = {
    ("expect_column_values_to_not_be_null", "ETAT_ETABLISSEMENT"),
    ("expect_column_values_to_not_be_null", "CODE_POSTAL"),
    ("expect_column_values_to_not_be_null", "DATE_CREATION_ETAB"),
}

df = pd.read_csv(FIXTURE, dtype=str).where(pd.notnull, other=None)
print(f"[GE-CI] {len(df)} lignes depuis {FIXTURE}")

ctx = gx.get_context(mode="ephemeral")
ds   = ctx.data_sources.add_pandas("pandas_ci")
asset = ds.add_dataframe_asset("fixture")
batch_def = asset.add_batch_definition_whole_dataframe("whole")

suite = ctx.suites.add(gx.ExpectationSuite(name="sirene_ci_suite"))

suite.add_expectation(
    gx.expectations.ExpectColumnValueLengthsToEqual(column="SIRET", value=14))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="SIREN"))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="ETAT_ETABLISSEMENT", value_set=["Actif", "Fermé"]))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="ETAT_ETABLISSEMENT", mostly=0.25))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="CODE_POSTAL", mostly=0.25))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="DATE_CREATION_ETAB"))

vd = gx.ValidationDefinition(data=batch_def, suite=suite, name="sirene_ci_vd")
ctx.validation_definitions.add(vd)
result = vd.run(batch_parameters={"dataframe": df})

crit, warn = [], []
for r in result.results:
    if r.success:
        continue
    key = (r.expectation_config.type,
           r.expectation_config.kwargs.get("column", ""))
    (warn if key in WARN_EXPECTATIONS else crit).append(r)

print(f"[GE-CI] OK={sum(1 for r in result.results if r.success)} "
      f"WARN={len(warn)} CRIT={len(crit)}")
for f in warn:
    print(f"  [WARN] {f.expectation_config.type}"
          f"({f.expectation_config.kwargs.get('column','')})")
for f in crit:
    print(f"  [CRIT] {f.expectation_config.type}"
          f"({f.expectation_config.kwargs.get('column','')})")

if crit:
    print("[GE-CI] ECHEC — expectations critiques non satisfaites.")
    sys.exit(1)
print("[GE-CI] OK — toutes les expectations critiques satisfaites.")
sys.exit(0)
