"""
Job Spark : Ingestion étendue V2 - 9 fichiers CSV.
Ingère tous les fichiers sources (3 existants + 6 nouveaux).

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame

# Ajout du chemin des utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig
from utils.spark_config_extended import (
    SCHEMA_COMPTEURS_LINKY, SCHEMA_POSTES_SOURCES,
    SCHEMA_HISTORIQUE_RELEVES, SCHEMA_HISTORIQUE_INTERVENTIONS,
    SCHEMA_FACTURES, SCHEMA_METEO_QUOTIDIENNE
)


class IngestionExtendedJob:
    """Job d'ingestion des 9 fichiers sources."""
    
    def __init__(self, spark_session):
        """
        Initialise le job d'ingestion.
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.stats = {
            "files_processed": 0,
            "total_records": 0,
            "failed_files": []
        }
    
    def ingest_csv_to_parquet(self, csv_path: str, output_path: str, 
                              table_name: str, schema=None) -> bool:
        """
        Ingère un fichier CSV et le sauvegarde en Parquet.
        
        Args:
            csv_path: Chemin du fichier CSV source
            output_path: Chemin de sortie Parquet
            table_name: Nom de la table
            schema: Schéma PySpark (optionnel)
            
        Returns:
            True si succès, False sinon
        """
        try:
            print(f"\n📂 Ingestion : {table_name}")
            print(f"   Source : {csv_path}")
            
            # Lire le CSV avec schéma si fourni
            if schema:
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "false") \
                    .schema(schema) \
                    .csv(csv_path)
            else:
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_path)
            
            # Compter
            count = df.count()
            print(f"   ✅ {count:,} lignes chargées")
            
            # Sauvegarder en Parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/{table_name}")
            
            print(f"   💾 Sauvegardé dans {output_path}/{table_name}")
            
            # Stats
            self.stats["files_processed"] += 1
            self.stats["total_records"] += count
            
            return True
            
        except Exception as e:
            print(f"   ❌ ERREUR : {str(e)}")
            self.stats["failed_files"].append(table_name)
            return False
    
    def run_ingestion(self, input_dir: str, output_dir: str) -> bool:
        """
        Exécute l'ingestion de tous les fichiers.
        
        Args:
            input_dir: Répertoire des CSV sources
            output_dir: Répertoire de sortie Parquet
            
        Returns:
            True si tous les fichiers sont ingérés
        """
        print("="*80)
        print("🚀 INGESTION ÉTENDUE V2 - 9 FICHIERS")
        print("="*80)
        
        # Liste des fichiers à ingérer
        files_config = [
            # Fichiers existants (Phase 1)
            {
                "name": "reclamations",
                "csv": f"{input_dir}/reclamations.csv",
                "schema": None  # Utiliser schéma existant de spark_config
            },
            {
                "name": "incidents",
                "csv": f"{input_dir}/incidents.csv",
                "schema": None
            },
            {
                "name": "clients",
                "csv": f"{input_dir}/clients.csv",
                "schema": None
            },
            
            # Nouveaux fichiers (Phase 3)
            {
                "name": "compteurs_linky",
                "csv": f"{input_dir}/compteurs_linky.csv",
                "schema": SCHEMA_COMPTEURS_LINKY
            },
            {
                "name": "postes_sources",
                "csv": f"{input_dir}/postes_sources.csv",
                "schema": SCHEMA_POSTES_SOURCES
            },
            {
                "name": "historique_releves",
                "csv": f"{input_dir}/historique_releves.csv",
                "schema": SCHEMA_HISTORIQUE_RELEVES
            },
            {
                "name": "historique_interventions",
                "csv": f"{input_dir}/historique_interventions.csv",
                "schema": SCHEMA_HISTORIQUE_INTERVENTIONS
            },
            {
                "name": "factures",
                "csv": f"{input_dir}/factures.csv",
                "schema": SCHEMA_FACTURES
            },
            {
                "name": "meteo_quotidienne",
                "csv": f"{input_dir}/meteo_quotidienne.csv",
                "schema": SCHEMA_METEO_QUOTIDIENNE
            }
        ]
        
        # Ingestion de chaque fichier
        for file_config in files_config:
            self.ingest_csv_to_parquet(
                csv_path=file_config["csv"],
                output_path=output_dir,
                table_name=file_config["name"],
                schema=file_config["schema"]
            )
        
        # Résumé
        print("\n" + "="*80)
        print("📊 RÉSUMÉ INGESTION")
        print("="*80)
        print(f"Fichiers traités    : {self.stats['files_processed']}/9")
        print(f"Lignes totales      : {self.stats['total_records']:,}")
        print(f"Fichiers en erreur  : {len(self.stats['failed_files'])}")
        
        if self.stats['failed_files']:
            print(f"  ⚠️  Erreurs sur : {', '.join(self.stats['failed_files'])}")
        
        print("="*80)
        
        # Succès si tous les fichiers ingérés
        return len(self.stats['failed_files']) == 0
    
    def validate_parquet_output(self, output_dir: str):
        """
        Valide que les fichiers Parquet ont bien été créés.
        
        Args:
            output_dir: Répertoire de sortie Parquet
        """
        print("\n🔍 Validation des fichiers Parquet...")
        
        tables = [
            "reclamations", "incidents", "clients",
            "compteurs_linky", "postes_sources", "historique_releves",
            "historique_interventions", "factures", "meteo_quotidienne"
        ]
        
        for table in tables:
            try:
                df = self.spark.read.parquet(f"{output_dir}/{table}")
                count = df.count()
                print(f"  ✅ {table:30s} : {count:>10,} lignes")
            except Exception as e:
                print(f"  ❌ {table:30s} : ERREUR - {str(e)}")


def main():
    """Point d'entrée principal du job."""
    print("="*80)
    print("🚀 DÉMARRAGE INGESTION ÉTENDUE V2")
    print("="*80)
    
    # Configuration des chemins
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    input_dir = f"{base_path}/data/raw"
    output_dir = f"{base_path}/data/raw_parquet"
    
    # Création de la session Spark
    spark = SparkConfig.get_spark_session("Ingestion_Extended_V2")
    
    try:
        # Initialisation du job
        job = IngestionExtendedJob(spark)
        
        # Exécution de l'ingestion
        success = job.run_ingestion(input_dir, output_dir)
        
        # Validation
        if success:
            job.validate_parquet_output(output_dir)
        
        # Code de sortie
        if success:
            print("\n✅ Ingestion étendue terminée avec succès")
            sys.exit(0)
        else:
            print("\n⚠️  Ingestion terminée avec des erreurs")
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
