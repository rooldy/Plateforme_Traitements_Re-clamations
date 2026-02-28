"""
Job Spark : Couche BRONZE - Ingestion données brutes.
Lit les CSV sources et les sauvegarde en Parquet (as-is).

Architecture Médaillon - Couche BRONZE
Auteur: Data Engineering Portfolio  
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig
from utils.spark_config_extended import (
    SCHEMA_COMPTEURS_LINKY, SCHEMA_POSTES_SOURCES,
    SCHEMA_HISTORIQUE_RELEVES, SCHEMA_HISTORIQUE_INTERVENTIONS,
    SCHEMA_FACTURES, SCHEMA_METEO_QUOTIDIENNE
)
from utils.medallion_config import MedallionConfig, MedallionLayer


class BronzeLayerJob:
    """Job d'ingestion BRONZE - Données brutes."""
    
    def __init__(self, spark_session):
        """Initialise le job Bronze."""
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.stats = {"tables_ingested": 0, "total_records": 0}
    
    def ingest_to_bronze(self, csv_path: str, table_name: str, schema=None) -> bool:
        """
        Ingère un fichier CSV vers la couche BRONZE.
        
        Args:
            csv_path: Chemin du CSV source
            table_name: Nom de la table
            schema: Schéma PySpark (optionnel)
            
        Returns:
            True si succès
        """
        try:
            print(f"\n📥 BRONZE Ingestion : {table_name}")
            
            # Lire CSV avec schéma strict
            if schema:
                df = self.spark.read \
                    .option("header", "true") \
                    .schema(schema) \
                    .csv(csv_path)
            else:
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_path)
            
            count = df.count()
            print(f"   📊 {count:,} lignes lues")
            
            # Sauvegarder en BRONZE (Parquet as-is)
            bronze_path = MedallionConfig.get_path(MedallionLayer.BRONZE, table_name)
            
            df.write \
                .mode(MedallionConfig.BRONZE_MODE) \
                .option("compression", MedallionConfig.BRONZE_COMPRESSION) \
                .parquet(bronze_path)
            
            print(f"   ✅ Sauvegardé dans {bronze_path}")
            
            self.stats["tables_ingested"] += 1
            self.stats["total_records"] += count
            
            return True
            
        except Exception as e:
            print(f"   ❌ ERREUR : {str(e)}")
            return False
    
    def run_bronze_ingestion(self, input_dir: str) -> bool:
        """
        Exécute l'ingestion de toutes les tables vers BRONZE.
        
        Args:
            input_dir: Répertoire des CSV sources
            
        Returns:
            True si succès
        """
        print("="*80)
        print("🥉 COUCHE BRONZE : Ingestion Données Brutes")
        print("="*80)
        print(MedallionConfig.BRONZE_DESCRIPTION)
        
        # Configuration des fichiers
        files_config = [
            {"name": "reclamations", "csv": f"{input_dir}/reclamations.csv", "schema": None},
            {"name": "incidents", "csv": f"{input_dir}/incidents.csv", "schema": None},
            {"name": "clients", "csv": f"{input_dir}/clients.csv", "schema": None},
            {"name": "compteurs_linky", "csv": f"{input_dir}/compteurs_linky.csv", "schema": SCHEMA_COMPTEURS_LINKY},
            {"name": "postes_sources", "csv": f"{input_dir}/postes_sources.csv", "schema": SCHEMA_POSTES_SOURCES},
            {"name": "historique_releves", "csv": f"{input_dir}/historique_releves.csv", "schema": SCHEMA_HISTORIQUE_RELEVES},
            {"name": "historique_interventions", "csv": f"{input_dir}/historique_interventions.csv", "schema": SCHEMA_HISTORIQUE_INTERVENTIONS},
            {"name": "factures", "csv": f"{input_dir}/factures.csv", "schema": SCHEMA_FACTURES},
            {"name": "meteo_quotidienne", "csv": f"{input_dir}/meteo_quotidienne.csv", "schema": SCHEMA_METEO_QUOTIDIENNE}
        ]
        
        # Ingestion
        for config in files_config:
            self.ingest_to_bronze(config["csv"], config["name"], config["schema"])
        
        # Résumé
        print("\n" + "="*80)
        print("📊 RÉSUMÉ BRONZE")
        print("="*80)
        print(f"Tables ingérées  : {self.stats['tables_ingested']}/9")
        print(f"Lignes totales   : {self.stats['total_records']:,}")
        print("="*80)
        
        return self.stats['tables_ingested'] == 9


def main():
    """Point d'entrée principal."""
    print("\n🚀 DÉMARRAGE JOB BRONZE\n")
    
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    input_dir = f"{base_path}/data/raw"
    
    spark = SparkConfig.get_spark_session("Bronze_Layer_Ingestion")
    
    try:
        job = BronzeLayerJob(spark)
        success = job.run_bronze_ingestion(input_dir)
        
        if success:
            print("\n✅ Job BRONZE terminé avec succès")
            sys.exit(0)
        else:
            print("\n⚠️  Job BRONZE terminé avec erreurs")
            sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ ERREUR FATALE: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
