"""
Job Quality Check des données — version pandas (léger, sans Spark).
Vérifie la qualité des données ingérées depuis les fichiers Parquet.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import os
import sys
import glob
from datetime import datetime, timezone

import pandas as pd
import psycopg2

print("=" * 80)
print("🔍 DÉMARRAGE DU JOB QUALITY CHECK (pandas)")
print("=" * 80)

BASE_PATH   = os.getenv("AIRFLOW_HOME", "/opt/airflow")
PARQUET_DIR = f"{BASE_PATH}/data/raw_parquet/reclamations"

DB_CONN = {
    "host":     os.getenv("POSTGRES_HOST",     "reclamations-postgres"),
    "port":     os.getenv("POSTGRES_PORT",     "5432"),
    "dbname":   os.getenv("POSTGRES_DB",       "reclamations_db"),
    "user":     os.getenv("POSTGRES_USER",     "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow_local_dev"),
}

VALID_TYPES   = {"COUPURE_ELECTRIQUE", "COMPTEUR_LINKY", "FACTURATION",
                 "RACCORDEMENT_RESEAU", "INTERVENTION_TECHNIQUE"}
VALID_STATUTS = {"OUVERT", "EN_COURS", "CLOTURE", "ESCALADE"}
VALID_PRIOS   = {"CRITIQUE", "HAUTE", "NORMALE", "BASSE"}

# ──────────────────────────────────────────────────────────────────────────────
# 1. CHARGEMENT
# ──────────────────────────────────────────────────────────────────────────────
print(f"\n📂 Chargement depuis {PARQUET_DIR} ...")

parquet_files = glob.glob(f"{PARQUET_DIR}/**/*.parquet", recursive=True) + \
                glob.glob(f"{PARQUET_DIR}/*.parquet")

if not parquet_files:
    print("❌ Aucun fichier Parquet trouvé — vérifiez que l'ingestion a bien tourné.")
    sys.exit(1)

df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
total = len(df)
print(f"   ✓ {total:,} lignes chargées depuis {len(parquet_files)} fichier(s)")

# ──────────────────────────────────────────────────────────────────────────────
# 2. COMPLÉTUDE
# ──────────────────────────────────────────────────────────────────────────────
print("\n🔍 Vérification complétude...")
required_cols = ["id_reclamation", "client_id", "type_reclamation",
                 "statut", "priorite", "region", "date_creation"]

null_counts = {c: int(df[c].isna().sum()) for c in required_cols if c in df.columns}
total_nulls = sum(null_counts.values())
completeness_score = round((1 - total_nulls / (total * len(required_cols))) * 100, 2)

for col, n in null_counts.items():
    if n > 0:
        print(f"   ⚠️  {col} : {n} valeurs nulles")
print(f"   ✓ Score complétude : {completeness_score:.2f}%")

# ──────────────────────────────────────────────────────────────────────────────
# 3. UNICITÉ
# ──────────────────────────────────────────────────────────────────────────────
print("\n🔍 Vérification unicité...")
duplicates = int(df["id_reclamation"].duplicated().sum())
uniqueness_score = round((1 - duplicates / total) * 100, 2) if total > 0 else 100.0
print(f"   ✓ Doublons : {duplicates} | Score unicité : {uniqueness_score:.2f}%")

# ──────────────────────────────────────────────────────────────────────────────
# 4. VALIDITÉ
# ──────────────────────────────────────────────────────────────────────────────
print("\n🔍 Vérification validité...")
invalid_types   = int((~df["type_reclamation"].isin(VALID_TYPES)).sum())   if "type_reclamation" in df.columns else 0
invalid_statuts = int((~df["statut"].isin(VALID_STATUTS)).sum())           if "statut"           in df.columns else 0
invalid_prios   = int((~df["priorite"].isin(VALID_PRIOS)).sum())           if "priorite"         in df.columns else 0
invalid_records = invalid_types + invalid_statuts + invalid_prios
validity_score  = round((1 - invalid_records / (total * 3)) * 100, 2) if total > 0 else 100.0

if invalid_types:   print(f"   ⚠️  Types invalides : {invalid_types}")
if invalid_statuts: print(f"   ⚠️  Statuts invalides : {invalid_statuts}")
if invalid_prios:   print(f"   ⚠️  Priorités invalides : {invalid_prios}")
print(f"   ✓ Score validité : {validity_score:.2f}%")

# ──────────────────────────────────────────────────────────────────────────────
# 5. COHÉRENCE
# ──────────────────────────────────────────────────────────────────────────────
print("\n🔍 Vérification cohérence...")
inconsistent = 0
if "date_cloture" in df.columns and "date_creation" in df.columns:
    mask = df["date_cloture"].notna() & (df["date_cloture"] < df["date_creation"])
    inconsistent = int(mask.sum())
    if inconsistent:
        print(f"   ⚠️  Dates incohérentes (clôture < création) : {inconsistent}")
consistency_score = round((1 - inconsistent / total) * 100, 2) if total > 0 else 100.0
print(f"   ✓ Score cohérence : {consistency_score:.2f}%")

# ──────────────────────────────────────────────────────────────────────────────
# 6. SCORE GLOBAL
# ──────────────────────────────────────────────────────────────────────────────
overall_score = round(
    completeness_score * 0.3 +
    uniqueness_score   * 0.3 +
    validity_score     * 0.2 +
    consistency_score  * 0.2,
    2
)
passed = overall_score >= 90.0

print("\n" + "=" * 80)
print("📊 RÉSUMÉ DES CONTRÔLES QUALITÉ")
print("=" * 80)
print(f"Score de complétude  : {completeness_score:.2f}%")
print(f"Score d'unicité      : {uniqueness_score:.2f}%")
print(f"Score de validité    : {validity_score:.2f}%")
print(f"Score de cohérence   : {consistency_score:.2f}%")
print(f"\n{'✅' if passed else '❌'} Score global : {overall_score:.2f}%")
print("=" * 80)

# ──────────────────────────────────────────────────────────────────────────────
# 7. SAUVEGARDE POSTGRESQL
# ──────────────────────────────────────────────────────────────────────────────
print("\n💾 Sauvegarde métriques dans PostgreSQL...")
try:
    conn = psycopg2.connect(**DB_CONN)
    cur  = conn.cursor()
    now  = datetime.now(timezone.utc)

    cur.execute("""
        INSERT INTO reclamations.data_quality_metrics (
            date_controle, job_name, table_name,
            total_records, records_with_nulls, completeness_score,
            duplicate_records, uniqueness_score,
            invalid_records, validity_score,
            inconsistent_records, consistency_score,
            overall_quality_score, passed
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        now, "quality_check", "reclamations",
        total, total_nulls, completeness_score,
        duplicates, uniqueness_score,
        invalid_records, validity_score,
        inconsistent, consistency_score,
        overall_score, passed
    ))
    conn.commit()
    cur.close()
    conn.close()
    print("   ✅ Métriques sauvegardées")
except Exception as e:
    print(f"   ⚠️  Impossible de sauvegarder les métriques : {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 8. SORTIE
# ──────────────────────────────────────────────────────────────────────────────
if passed:
    print("\n✅ Contrôles qualité RÉUSSIS - Données acceptables")
    sys.exit(0)
else:
    print("\n⚠️  Contrôles qualité ÉCHOUÉS - Qualité insuffisante")
    sys.exit(1)
