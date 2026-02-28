"""
Job Spark : Export Gold → PostgreSQL
Utilisé par le DAG medallion_pipeline_daily avec SparkSubmitOperator
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os


def main():
    # Session Spark avec driver PostgreSQL
    spark = SparkSession.builder \
        .appName("Export_Gold_PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .getOrCreate()
    
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    
    # Charger données Gold
    print("📊 Chargement données GOLD...")
    df_enriched = spark.read.parquet(f"{base_path}/data/gold/reclamations_enriched")
    df_kpis = spark.read.parquet(f"{base_path}/data/gold/kpis_daily")
    
    # Dédupliquer KPIs
    df_kpis_dedup = df_kpis.dropDuplicates(["date_calcul", "region", "type_reclamation"])
    
    print(f"   Reclamations enriched : {df_enriched.count():,} lignes")
    print(f"   KPIs daily            : {df_kpis_dedup.count():,} lignes")
    
    # Configuration PostgreSQL
    jdbc_url = "jdbc:postgresql://reclamations-postgres:5432/reclamations_db"
    props = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    
    # Export reclamations_cleaned
    print("\n📤 Export reclamations_cleaned...")
    df_enriched.write.jdbc(
        url=jdbc_url,
        table="reclamations.reclamations_cleaned",
        mode="overwrite",
        properties=props
    )
    print("   ✅ Exporté")
    
    # Export KPIs (avec renommage colonnes)
    print("\n📤 Export kpis_daily...")
    df_kpis_final = df_kpis_dedup \
        .withColumnRenamed("nombre_reclamations_total", "nombre_reclamations_ouvertes") \
        .withColumnRenamed("nombre_ouvertes", "nombre_reclamations_cloturees") \
        .withColumnRenamed("nombre_cloturees", "nombre_reclamations_en_cours") \
        .withColumnRenamed("duree_moyenne_traitement", "duree_moyenne_traitement_heures") \
        .withColumnRenamed("duree_mediane_traitement", "duree_mediane_traitement_heures") \
        .withColumnRenamed("delai_moyen_premiere_reponse", "delai_moyen_premiere_reponse_heures") \
        .withColumn("taux_respect_sla", lit(0.00).cast("decimal(5,2)")) \
        .withColumn("taux_reclamations_critiques", lit(0.00).cast("decimal(5,2)")) \
        .withColumn("taux_escalade", lit(0.00).cast("decimal(5,2)")) \
        .withColumn("date_insertion", current_timestamp()) \
        .select("date_calcul", "region", "type_reclamation", 
                "nombre_reclamations_ouvertes", "nombre_reclamations_cloturees", 
                "nombre_reclamations_en_cours", "duree_moyenne_traitement_heures",
                "duree_mediane_traitement_heures", "delai_moyen_premiere_reponse_heures",
                "taux_respect_sla", "taux_reclamations_critiques", "taux_escalade", 
                "date_insertion")
    
    df_kpis_final.write.jdbc(
        url=jdbc_url,
        table="reclamations.kpis_daily",
        mode="append",
        properties=props
    )
    print("   ✅ Exporté")
    
    print("\n✅ Export PostgreSQL terminé avec succès")
    spark.stop()


if __name__ == "__main__":
    main()
