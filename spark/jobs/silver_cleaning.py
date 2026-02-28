"""
Job Spark : Couche SILVER - Nettoyage et validation.
Lit BRONZE, applique quality checks, nettoie et sauvegarde en SILVER.

Architecture Médaillon - Couche SILVER
Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace, count, isnan, isnull

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig
from utils.medallion_config import MedallionConfig, MedallionLayer


class SilverLayerJob:
    """Job de nettoyage SILVER - Données validées."""
    
    def __init__(self, spark_session):
        """Initialise le job Silver."""
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.quality_threshold = MedallionConfig.get_quality_threshold(MedallionLayer.SILVER)
    
    def clean_reclamations(self):
        """Nettoie et valide les réclamations."""
        print("\n🧹 SILVER : Nettoyage réclamations")
        
        # Lire BRONZE
        bronze_path = MedallionConfig.get_path(MedallionLayer.BRONZE, "reclamations")
        df = self.spark.read.parquet(bronze_path)
        
        print(f"   📊 Bronze : {df.count():,} lignes")
        
        # 1. Supprimer doublons stricts
        df_dedup = df.dropDuplicates(["id_reclamation"])
        
        # 2. Nettoyer valeurs nulles critiques
        df_clean = df_dedup.filter(
            col("id_reclamation").isNotNull() &
            col("client_id").isNotNull() &
            col("type_reclamation").isNotNull() &
            col("date_creation").isNotNull()
        )
        
        # 3. Nettoyer espaces
        string_cols = ["type_reclamation", "priorite", "statut", "canal", "region"]
        for col_name in string_cols:
            df_clean = df_clean.withColumn(col_name, trim(col(col_name)))
        
        # 4. Valider dates cohérentes
        df_validated = df_clean.filter(
            (col("date_premiere_reponse").isNull()) | 
            (col("date_premiere_reponse") >= col("date_creation"))
        ).filter(
            (col("date_cloture").isNull()) |
            (col("date_cloture") >= col("date_creation"))
        )
        
        # 5. Valider format Linky (si présent)
        df_validated = df_validated.withColumn(
            "reference_linky",
            when(
                col("reference_linky").rlike("^LKY[0-9]{8}$"),
                col("reference_linky")
            ).otherwise(None)
        )
        
        print(f"   ✅ Silver : {df_validated.count():,} lignes ({df.count() - df_validated.count()} filtrées)")
        
        # Sauvegarder SILVER
        silver_path = MedallionConfig.get_path(MedallionLayer.SILVER, "reclamations_cleaned")
        df_validated.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(silver_path)
        
        print(f"   💾 Sauvegardé dans {silver_path}")
    
    def clean_compteurs_linky(self):
        """Nettoie et valide les compteurs Linky."""
        print("\n🧹 SILVER : Nettoyage compteurs Linky")
        
        bronze_path = MedallionConfig.get_path(MedallionLayer.BRONZE, "compteurs_linky")
        df = self.spark.read.parquet(bronze_path)
        
        print(f"   📊 Bronze : {df.count():,} lignes")
        
        # 1. Valider format références
        df_validated = df.filter(
            col("reference_linky").rlike("^LKY[0-9]{8}$")
        )
        
        # 2. Valider puissances
        valid_powers = [3, 6, 9, 12, 15, 18, 24, 36]
        df_validated = df_validated.filter(
            col("puissance_souscrite_kva").isin(valid_powers)
        )
        
        # 3. Supprimer doublons
        df_clean = df_validated.dropDuplicates(["reference_linky"])
        
        print(f"   ✅ Silver : {df_clean.count():,} lignes")
        
        silver_path = MedallionConfig.get_path(MedallionLayer.SILVER, "compteurs_verified")
        df_clean.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(silver_path)
        
        print(f"   💾 Sauvegardé dans {silver_path}")
    
    def clean_factures(self):
        """Nettoie et valide les factures."""
        print("\n🧹 SILVER : Nettoyage factures")
        
        bronze_path = MedallionConfig.get_path(MedallionLayer.BRONZE, "factures")
        df = self.spark.read.parquet(bronze_path)
        
        print(f"   📊 Bronze : {df.count():,} lignes")
        
        # 1. Valider cohérence montants (HT + TVA = TTC avec tolérance 0.01€)
        df_validated = df.filter(
            (col("montant_ht") + col("montant_tva") - col("montant_ttc")).between(-0.01, 0.01)
        )
        
        # 2. Valider dates
        df_validated = df_validated.filter(
            col("date_echeance") >= col("date_emission")
        )
        
        # 3. Supprimer doublons
        df_clean = df_validated.dropDuplicates(["numero_facture"])
        
        print(f"   ✅ Silver : {df_clean.count():,} lignes")
        
        silver_path = MedallionConfig.get_path(MedallionLayer.SILVER, "factures_validated")
        df_clean.write \
            .mode("overwrite") \
            .parquet(silver_path)
        
        print(f"   💾 Sauvegardé dans {silver_path}")
    
    def clean_all_tables(self):
        """Nettoie toutes les tables BRONZE → SILVER."""
        print("="*80)
        print("🥈 COUCHE SILVER : Nettoyage et Validation")
        print("="*80)
        print(MedallionConfig.SILVER_DESCRIPTION)
        
        # Nettoyer tables principales
        self.clean_reclamations()
        self.clean_compteurs_linky()
        self.clean_factures()
        
        # TODO: Ajouter autres tables (incidents, clients, releves, etc.)
        
        print("\n" + "="*80)
        print("✅ SILVER : Nettoyage terminé")
        print("="*80)


def main():
    """Point d'entrée principal."""
    print("\n🚀 DÉMARRAGE JOB SILVER\n")
    
    spark = SparkConfig.get_spark_session("Silver_Layer_Cleaning")
    
    try:
        job = SilverLayerJob(spark)
        job.clean_all_tables()
        
        print("\n✅ Job SILVER terminé avec succès")
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
