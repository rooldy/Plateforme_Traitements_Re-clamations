#!/usr/bin/env python3
from pyspark.sql import SparkSession
import subprocess
import os
import glob
import shutil

print("\n🚀 EXPORT GOLD → CSV → POSTGRESQL\n")

# Spark session
spark = SparkSession.builder.appName("Export_PostgreSQL_CSV").getOrCreate()
base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

# 1. Charger Gold
print("📂 Chargement Gold Layer...")
df_enriched = spark.read.parquet(f"{base_path}/data/gold/reclamations_enriched")
df_kpis = spark.read.parquet(f"{base_path}/data/gold/kpis_daily")

print(f"   ✓ Reclamations enrichies : {df_enriched.count():,} lignes")
print(f"   ✓ KPIs daily            : {df_kpis.count():,} lignes")

# 2. Export CSV
print("\n📤 Export vers CSV...")
export_dir = f"{base_path}/data/exports"
os.makedirs(export_dir, exist_ok=True)

# Supprimer dossiers temporaires si existent
for folder in ["reclamations_temp", "kpis_temp"]:
    path = f"{export_dir}/{folder}"
    if os.path.exists(path):
        shutil.rmtree(path)

df_enriched.coalesce(1).write.csv(
    f"{export_dir}/reclamations_temp",
    mode="overwrite",
    header=True
)

df_kpis.coalesce(1).write.csv(
    f"{export_dir}/kpis_temp",
    mode="overwrite",
    header=True
)

spark.stop()

# 3. Renommer fichiers CSV
print("   ✓ Fichiers CSV créés")
rec_csv = glob.glob(f"{export_dir}/reclamations_temp/part-*.csv")[0]
kpi_csv = glob.glob(f"{export_dir}/kpis_temp/part-*.csv")[0]

final_rec = f"{export_dir}/reclamations_cleaned.csv"
final_kpi = f"{export_dir}/kpis_daily.csv"

if os.path.exists(final_rec):
    os.remove(final_rec)
if os.path.exists(final_kpi):
    os.remove(final_kpi)

os.rename(rec_csv, final_rec)
os.rename(kpi_csv, final_kpi)

print(f"   ✓ {final_rec}")
print(f"   ✓ {final_kpi}")

# 4. Import PostgreSQL
print("\n📥 Import PostgreSQL avec COPY...")

psql_cmd = f"""
psql postgresql://airflow:airflow_local_dev@reclamations-postgres:5432/reclamations_db << 'SQL'
\\set ON_ERROR_STOP on

-- Vider tables
TRUNCATE TABLE reclamations.reclamations_cleaned;
TRUNCATE TABLE reclamations.kpis_daily;

-- Import COPY
\\copy reclamations.reclamations_cleaned FROM '{final_rec}' DELIMITER ',' CSV HEADER
\\copy reclamations.kpis_daily FROM '{final_kpi}' DELIMITER ',' CSV HEADER

-- Vérification
SELECT 'reclamations_cleaned: ' || COUNT(*) || ' lignes' FROM reclamations.reclamations_cleaned;
SELECT 'kpis_daily: ' || COUNT(*) || ' lignes' FROM reclamations.kpis_daily;
SQL
"""

result = subprocess.run(psql_cmd, shell=True, capture_output=True, text=True)
print(result.stdout)

if result.returncode != 0:
    print("❌ ERREUR:", result.stderr)
    raise Exception("Import PostgreSQL échoué")

print("\n✅ EXPORT POSTGRESQL TERMINÉ !\n")
