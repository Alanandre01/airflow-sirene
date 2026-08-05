"""
J3 - M4-S1 — Great Expectations Checkpoint + Data Docs
Crée sirene_checkpoint sur la Suite J2, l'exécute, génère les Data Docs.
"""
import os, sys
import pandas as pd
import great_expectations as gx
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────
PROJECT_ROOT   = Path(__file__).parent.parent  # airflow-sirene/
CSV_PATH       = PROJECT_ROOT / "data" / "sirene_sample.csv"
CHECKPOINT     = "sirene_checkpoint"
VALIDATION_DEF = "sirene_validation_j2"

# ── 1. Vérification CSV ────────────────────────────────────────────
print("=== [1] CSV ===")
if not CSV_PATH.exists():
    print(f"[ERREUR] {CSV_PATH} introuvable → lancer export_sample.py")
    sys.exit(1)
print(f"[OK] {CSV_PATH.name} ({CSV_PATH.stat().st_size / 1024:.0f} KB)")

# ── 2. Data Context ────────────────────────────────────────────────
print("\n=== [2] Data Context ===")
context = gx.get_context(mode="file", project_root_dir=str(PROJECT_ROOT))
print("[OK] Context chargé")

# ── 3. Validation Definition J2 — get_, PAS add_ ───────────────────
print(f"\n=== [3] Validation Definition '{VALIDATION_DEF}' ===")
try:
    vd = context.validation_definitions.get(VALIDATION_DEF)
    print(f"[OK] {vd.name}")
except Exception as e:
    print(f"[ERREUR] {e}")
    print("→ Relancer scripts/create_suite.py pour recréer la Validation Definition.")
    sys.exit(1)

# ── 4. UpdateDataDocsAction (optionnelle — GX 1.x) ─────────────────
print("\n=== [4] Actions ===")
try:
    from great_expectations.checkpoint.actions import UpdateDataDocsAction
    actions = [UpdateDataDocsAction(name="update_data_docs")]
    print("[OK] UpdateDataDocsAction disponible — Data Docs auto à chaque run")
except Exception as e:
    actions = []
    print(f"[INFO] UpdateDataDocsAction absent ({e}) — build_data_docs() manuel")

# ── 5. Checkpoint — get si existant, add sinon ─────────────────────
print(f"\n=== [5] Checkpoint '{CHECKPOINT}' ===")
try:
    checkpoint = context.checkpoints.get(CHECKPOINT)
    print("[INFO] Déjà existant — réutilisé")
except Exception:
    checkpoint = gx.Checkpoint(
        name=CHECKPOINT,
        validation_definitions=[vd],
        actions=actions,
    )
    context.checkpoints.add(checkpoint)
    print(f"[OK] Créé → gx/checkpoints/{CHECKPOINT}.json")

# ── 6. Chargement CSV ──────────────────────────────────────────────
print("\n=== [6] Données ===")
df = pd.read_csv(CSV_PATH, dtype=str)
print(f"[OK] {len(df):,} lignes · {len(df.columns)} colonnes")

# ── 7. Exécution ───────────────────────────────────────────────────
print("\n=== [7] Exécution du Checkpoint ===")
result = checkpoint.run(batch_parameters={"dataframe": df})

# ── 8. Analyse des résultats ───────────────────────────────────────
print("\n=== [8] Résultats ===")
print(f"Succès global : {'✓ PASS' if result.success else '✗ FAIL — 2 FAILs attendus (J2)'}")
try:
    for vr in result.run_results.values():
        s = vr.statistics
        print(f"\n  Évaluées  : {s['evaluated_expectations']}")
        print(f"  ✓ Succès  : {s['successful_expectations']}")
        print(f"  ✗ Échecs  : {s['unsuccessful_expectations']}")
        print(f"  Taux      : {s['success_percent']:.1f}%\n")
        for r in vr.results:
            ok  = "✓ PASS" if r.success else "✗ FAIL"
            col = r.expectation_config.kwargs.get("column", "—")
            et  = r.expectation_config.type
            print(f"  {ok:8s}  {et:<48s}  [{col}]")
            if not r.success:
                obs = r.result.get("observed_value", "")
                print(f"            → observed: {obs}")
except AttributeError:
    print(f"[DEBUG] type={type(result)} — affichage brut :")
    print(result)

# ── 9. Data Docs ───────────────────────────────────────────────────
print("\n=== [9] Data Docs ===")
try:
    context.build_data_docs()
    print("[OK] Data Docs générés")
except Exception as e:
    print(f"[INFO] build_data_docs() : {e}")

docs = (PROJECT_ROOT / "gx" / "uncommitted" / "data_docs"
        / "local_site" / "index.html")
if docs.exists():
    print(f"[OK] {docs}")
    try:
        context.open_data_docs()
        print("[OK] Navigateur ouvert")
    except Exception:
        os.startfile(str(docs))
        print("[OK] Ouvert via os.startfile")
else:
    print(f"[WARN] Data Docs introuvables : {docs}")

print("\n=== J3 terminé ===")
print("Attendu : 4 PASS · 2 FAIL (CODE_POSTAL mostly + DATE_CREATION_ETAB strict)")
