from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Export_CSV").getOrCreate()
base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

print("📤 Export Gold → CSV...")

# Charger Gold
df_enriched = spark.read.parquet(f"{base_path}/data/gold/reclamations_enriched")
df_kpis = spark.read.parquet(f"{base_path}/data/gold/kpis_daily")

# Export CSV (1 seul fichier avec coalesce)
df_enriched.coalesce(1).write.csv(
    f"{base_path}/data/exports/reclamations_cleaned",
    mode="overwrite",
    header=True
)

df_kpis.coalesce(1).write.csv(
    f"{base_path}/data/exports/kpis_daily",
    mode="overwrite", 
    header=True
)

print(f"✅ reclamations_cleaned: {df_enriched.count():,} lignes")
print(f"✅ kpis_daily: {df_kpis.count():,} lignes")

spark.stop()
