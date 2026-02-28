"""
Job Spark : Couche GOLD - Enrichissements et KPIs.
Lit SILVER, applique enrichissements métier et sauvegarde en GOLD.

Architecture Médaillon - Couche GOLD
Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, datediff, 
    current_date, row_number, percentile_approx
)

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig
from utils.medallion_config import MedallionConfig, MedallionLayer


class GoldLayerJob:
    """Job d'enrichissement GOLD - Données business-ready."""
    
    def __init__(self, spark_session):
        """Initialise le job Gold."""
        self.spark = spark_session
        self.execution_date = datetime.now()
    
    def create_enriched_reclamations(self):
        """Crée les réclamations enrichies avec tous les calculs métier."""
        print("\n🌟 GOLD : Enrichissement réclamations")
        
        # Lire SILVER
        silver_path = MedallionConfig.get_path(MedallionLayer.SILVER, "reclamations_cleaned")
        df = self.spark.read.parquet(silver_path)
        
        print(f"   📊 Silver : {df.count():,} lignes")
        
        # 1. Calcul durées
        df_enriched = df.withColumn(
            "duree_traitement_heures",
            when(col("date_cloture").isNotNull(),
                 datediff(col("date_cloture"), col("date_creation")) * 24
            ).otherwise(
                 datediff(current_date(), col("date_creation")) * 24
            )
        ).withColumn(
            "delai_premiere_reponse_heures",
            when(col("date_premiere_reponse").isNotNull(),
                 datediff(col("date_premiere_reponse"), col("date_creation")) * 24
            )
        )
        
        # 2. Score priorité
        df_enriched = df_enriched.withColumn(
            "score_priorite_base",
            when(col("priorite") == "CRITIQUE", 100)
            .when(col("priorite") == "HAUTE", 75)
            .when(col("priorite") == "NORMALE", 50)
            .when(col("priorite") == "BASSE", 25)
            .otherwise(50)
        ).withColumn(
            "score_priorite",
            col("score_priorite_base") +
            when(col("statut").isin(["OUVERT", "EN_COURS"]), 20).otherwise(0) -
            when(col("delai_premiere_reponse_heures") > 24, 10).otherwise(0)
        )
        
        # 3. Détection récurrence
        window_recurrence = Window.partitionBy("client_id", "type_reclamation")
        df_enriched = df_enriched.withColumn(
            "nombre_reclamations_client",
            count("*").over(window_recurrence)
        ).withColumn(
            "est_recurrent",
            col("nombre_reclamations_client") >= 2
        )
        
        print(f"   ✅ Gold : {df_enriched.count():,} lignes enrichies")
        
        # Sauvegarder GOLD
        gold_path = MedallionConfig.get_path(MedallionLayer.GOLD, "reclamations_enriched")
        df_enriched.write \
            .mode("overwrite") \
            .partitionBy("region", "type_reclamation") \
            .parquet(gold_path)
        
        print(f"   💾 Sauvegardé dans {gold_path}")
    
    def create_kpis_daily(self):
        """Crée les KPIs quotidiens."""
        print("\n🌟 GOLD : Calcul KPIs quotidiens")
        
        # Lire réclamations enrichies
        enriched_path = MedallionConfig.get_path(MedallionLayer.GOLD, "reclamations_enriched")
        df = self.spark.read.parquet(enriched_path)
        
        # KPIs par région et type
        kpis = df.groupBy("region", "type_reclamation").agg(
            count("*").alias("nombre_reclamations_total"),
            count(when(col("statut").isin(["OUVERT", "EN_COURS"]), 1)).alias("nombre_ouvertes"),
            count(when(col("statut") == "CLOTURE", 1)).alias("nombre_cloturees"),
            avg("duree_traitement_heures").alias("duree_moyenne_traitement"),
            percentile_approx("duree_traitement_heures", 0.5).alias("duree_mediane_traitement"),
            avg("delai_premiere_reponse_heures").alias("delai_moyen_premiere_reponse"),
            count(when(col("priorite") == "CRITIQUE", 1)).alias("nb_critiques"),
            count(when(col("est_recurrent") == True, 1)).alias("nb_recurrentes")
        ).withColumn("date_calcul", current_date())
        
        print(f"   ✅ {kpis.count()} KPIs calculés")
        
        # Sauvegarder
        gold_path = MedallionConfig.get_path(MedallionLayer.GOLD, "kpis_daily")
        kpis.write \
            .mode("append") \
            .partitionBy("date_calcul") \
            .parquet(gold_path)
        
        print(f"   💾 Sauvegardé dans {gold_path}")
    
    def create_analytics_by_region(self):
        """Crée les analytics par région (pour dashboards)."""
        print("\n🌟 GOLD : Analytics par région")
        
        enriched_path = MedallionConfig.get_path(MedallionLayer.GOLD, "reclamations_enriched")
        df = self.spark.read.parquet(enriched_path)
        
        # Analytics complètes par région
        analytics = df.groupBy("region").agg(
            count("*").alias("total_reclamations"),
            avg("score_priorite").alias("score_priorite_moyen"),
            avg("duree_traitement_heures").alias("duree_moyenne_traitement"),
            (count(when(col("est_recurrent") == True, 1)) / count("*") * 100).alias("taux_recurrence_pct"),
            count(when(col("statut").isin(["OUVERT", "EN_COURS"]), 1)).alias("en_cours"),
            count(when(col("priorite") == "CRITIQUE", 1)).alias("nb_critiques")
        ).withColumn("date_snapshot", current_date())
        
        print(f"   ✅ Analytics pour {analytics.count()} régions")
        
        gold_path = MedallionConfig.get_path(MedallionLayer.GOLD, "analytics_by_region")
        analytics.write \
            .mode("append") \
            .partitionBy("date_snapshot") \
            .parquet(gold_path)
        
        print(f"   💾 Sauvegardé dans {gold_path}")
    
    def create_all_gold_tables(self):
        """Crée toutes les tables GOLD."""
        print("="*80)
        print("🥇 COUCHE GOLD : Enrichissements Business")
        print("="*80)
        print(MedallionConfig.GOLD_DESCRIPTION)
        
        self.create_enriched_reclamations()
        self.create_kpis_daily()
        self.create_analytics_by_region()
        
        print("\n" + "="*80)
        print("✅ GOLD : Enrichissements terminés")
        print("="*80)


def main():
    """Point d'entrée principal."""
    print("\n🚀 DÉMARRAGE JOB GOLD\n")
    
    spark = SparkConfig.get_spark_session("Gold_Layer_Enrichment")
    
    try:
        job = GoldLayerJob(spark)
        job.create_all_gold_tables()
        
        print("\n✅ Job GOLD terminé avec succès")
        sys.exit(0)
    
    except Exception as e:
        print(f"\n❌ ERREUR FATALE: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
