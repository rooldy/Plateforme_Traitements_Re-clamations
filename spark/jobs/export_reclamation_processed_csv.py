#!/usr/bin/env python3
"""
Export Processed → CSV → PostgreSQL
Lit les données produites par transformation.py (data/processed/reclamations)
et les exporte vers PostgreSQL via COPY (même pattern que export_postgres_csv.py).

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from pyspark.sql import SparkSession
import subprocess
import os
import glob
import shutil

print("\n🚀 EXPORT PROCESSED → CSV → POSTGRESQL\n")

# Session Spark avec jar local (pas de Maven)
spark = SparkSession.builder \
    .appName("Export_Reclamation_PostgreSQL_CSV") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

# 1. Charger données processed (sortie de transformation.py)
print("📂 Chargement données processed...")
df_processed = spark.read.parquet(f"{base_path}/data/processed/reclamations")

# Supprimer colonnes de partition Spark (annee/mois) inutiles pour l'export
partition_cols = [c for c in ["annee", "mois"] if c in df_processed.columns]
if partition_cols:
    df_processed = df_processed.drop(*partition_cols)

count = df_processed.count()
print(f"   ✓ Réclamations traitées : {count:,} lignes")

# 2. Export CSV
print("\n📤 Export vers CSV...")
export_dir = f"{base_path}/data/exports"
os.makedirs(export_dir, exist_ok=True)

temp_path = f"{export_dir}/reclamations_processed_temp"
if os.path.exists(temp_path):
    shutil.rmtree(temp_path)

df_processed.coalesce(1).write.csv(
    temp_path,
    mode="overwrite",
    header=True
)

spark.stop()

# 3. Renommer fichier CSV
csv_file = glob.glob(f"{temp_path}/part-*.csv")[0]
final_csv = f"{export_dir}/reclamations_processed.csv"

if os.path.exists(final_csv):
    os.remove(final_csv)
os.rename(csv_file, final_csv)
print(f"   ✓ {final_csv}")

# 4. Import PostgreSQL via COPY
print("\n📥 Import PostgreSQL avec COPY...")

psql_cmd = f"""
psql postgresql://airflow:airflow_local_dev@reclamations-postgres:5432/reclamations_db << 'SQL'
\\set ON_ERROR_STOP on

-- Vider la table
TRUNCATE TABLE reclamations.reclamations_processed;

-- Import COPY (rapide et fiable)
\\copy reclamations.reclamations_processed FROM '{final_csv}' DELIMITER ',' CSV HEADER;

-- Vérification
SELECT 'reclamations_cleaned: ' || COUNT(*) || ' lignes' FROM reclamations.reclamations_processed;
SQL
"""

result = subprocess.run(psql_cmd, shell=True, capture_output=True, text=True)
print(result.stdout)

if result.returncode != 0:
    print("❌ ERREUR:", result.stderr)
    raise Exception("Import PostgreSQL échoué")

print("\n✅ EXPORT RECLAMATION POSTGRESQL TERMINÉ !\n")
