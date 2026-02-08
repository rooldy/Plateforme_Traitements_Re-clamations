"""
Job Spark : Ingestion des données sources brutes.
Ce job lit les fichiers CSV, valide les schémas et sauvegarde en format Parquet.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

# Ajout du chemin des utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig


class IngestionJob:
    """Job d'ingestion des données sources."""
    
    def __init__(self, spark_session):
        """
        Initialise le job d'ingestion.
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.stats = {
            "reclamations_ingested": 0,
            "incidents_ingested": 0,
            "clients_ingested": 0,
            "errors": []
        }
    
    def validate_dataframe(self, df: DataFrame, expected_schema, dataset_name: str) -> bool:
        """
        Valide qu'un DataFrame correspond au schéma attendu.
        
        Args:
            df: DataFrame à valider
            expected_schema: Schéma attendu
            dataset_name: Nom du dataset pour les logs
            
        Returns:
            True si valide, False sinon
        """
        if df is None or df.count() == 0:
            error_msg = f"❌ {dataset_name}: DataFrame vide ou None"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        
        # Vérification des colonnes
        df_columns = set(df.columns)
        expected_columns = set([field.name for field in expected_schema.fields])
        
        missing_columns = expected_columns - df_columns
        extra_columns = df_columns - expected_columns
        
        if missing_columns:
            error_msg = f"❌ {dataset_name}: Colonnes manquantes: {missing_columns}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        
        if extra_columns:
            print(f"⚠️  {dataset_name}: Colonnes supplémentaires ignorées: {extra_columns}")
        
        print(f"✅ {dataset_name}: Schéma validé ({df.count()} lignes)")
        return True
    
    def ingest_reclamations(self, input_path: str, output_path: str) -> bool:
        """
        Ingère le fichier des réclamations.
        
        Args:
            input_path: Chemin du fichier CSV source
            output_path: Chemin de sortie Parquet
            
        Returns:
            True si succès, False sinon
        """
        try:
            print(f"\n🔄 Ingestion des réclamations depuis {input_path}...")
            
            # Lecture avec schéma strict
            schema = SparkConfig.get_reclamations_schema()
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .schema(schema) \
                .csv(input_path)
            
            # Validation
            if not self.validate_dataframe(df, schema, "Réclamations"):
                return False
            
            # Ajout de métadonnées
            df = df.withColumn("ingestion_date", current_timestamp()) \
                   .withColumn("source_file", lit(os.path.basename(input_path)))
            
            # Sauvegarde en Parquet
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            self.stats["reclamations_ingested"] = df.count()
            print(f"✅ {self.stats['reclamations_ingested']} réclamations ingérées avec succès")
            
            # Affichage d'un échantillon
            print("\n📊 Échantillon de données :")
            df.select("id_reclamation", "type_reclamation", "statut", "region", "date_creation") \
              .show(5, truncate=False)
            
            return True
            
        except Exception as e:
            error_msg = f"❌ Erreur lors de l'ingestion des réclamations: {str(e)}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return False
    
    def ingest_incidents(self, input_path: str, output_path: str) -> bool:
        """
        Ingère le fichier des incidents réseau.
        
        Args:
            input_path: Chemin du fichier CSV source
            output_path: Chemin de sortie Parquet
            
        Returns:
            True si succès, False sinon
        """
        try:
            print(f"\n🔄 Ingestion des incidents depuis {input_path}...")
            
            # Lecture avec schéma strict
            schema = SparkConfig.get_incidents_schema()
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .schema(schema) \
                .csv(input_path)
            
            # Validation
            if not self.validate_dataframe(df, schema, "Incidents"):
                return False
            
            # Ajout de métadonnées
            df = df.withColumn("ingestion_date", current_timestamp()) \
                   .withColumn("source_file", lit(os.path.basename(input_path)))
            
            # Sauvegarde en Parquet
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            self.stats["incidents_ingested"] = df.count()
            print(f"✅ {self.stats['incidents_ingested']} incidents ingérés avec succès")
            
            # Affichage d'un échantillon
            print("\n📊 Échantillon de données :")
            df.select("id_incident", "type_incident", "region", "date_incident", "clients_impactes") \
              .show(5, truncate=False)
            
            return True
            
        except Exception as e:
            error_msg = f"❌ Erreur lors de l'ingestion des incidents: {str(e)}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return False
    
    def ingest_clients(self, input_path: str, output_path: str) -> bool:
        """
        Ingère le fichier des clients.
        
        Args:
            input_path: Chemin du fichier CSV source
            output_path: Chemin de sortie Parquet
            
        Returns:
            True si succès, False sinon
        """
        try:
            print(f"\n🔄 Ingestion des clients depuis {input_path}...")
            
            # Lecture avec schéma strict
            schema = SparkConfig.get_clients_schema()
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .schema(schema) \
                .csv(input_path)
            
            # Validation
            if not self.validate_dataframe(df, schema, "Clients"):
                return False
            
            # Ajout de métadonnées
            df = df.withColumn("ingestion_date", current_timestamp()) \
                   .withColumn("source_file", lit(os.path.basename(input_path)))
            
            # Sauvegarde en Parquet
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            self.stats["clients_ingested"] = df.count()
            print(f"✅ {self.stats['clients_ingested']} clients ingérés avec succès")
            
            # Affichage d'un échantillon
            print("\n📊 Échantillon de données :")
            df.select("client_id", "nom", "prenom", "email", "region") \
              .show(5, truncate=False)
            
            return True
            
        except Exception as e:
            error_msg = f"❌ Erreur lors de l'ingestion des clients: {str(e)}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return False
    
    def log_execution_stats(self):
        """Log les statistiques d'exécution dans PostgreSQL."""
        try:
            from pyspark.sql import Row
            
            log_data = Row(
                job_name="ingestion",
                execution_date=self.execution_date,
                status="SUCCESS" if not self.stats["errors"] else "FAILED",
                start_time=self.execution_date,
                end_time=datetime.now(),
                records_processed=self.stats["reclamations_ingested"] + 
                                self.stats["incidents_ingested"] + 
                                self.stats["clients_ingested"],
                records_inserted=self.stats["reclamations_ingested"] + 
                               self.stats["incidents_ingested"] + 
                               self.stats["clients_ingested"],
                error_message="; ".join(self.stats["errors"]) if self.stats["errors"] else None
            )
            
            log_df = self.spark.createDataFrame([log_data])
            SparkConfig.write_to_postgres(log_df, "reclamations.pipeline_logs", mode="append")
            
            print("\n✅ Logs d'exécution sauvegardés dans PostgreSQL")
            
        except Exception as e:
            print(f"⚠️  Impossible de sauvegarder les logs: {str(e)}")
    
    def print_summary(self):
        """Affiche le résumé de l'exécution."""
        print("\n" + "="*80)
        print("📊 RÉSUMÉ DE L'INGESTION")
        print("="*80)
        print(f"Réclamations ingérées : {self.stats['reclamations_ingested']}")
        print(f"Incidents ingérés     : {self.stats['incidents_ingested']}")
        print(f"Clients ingérés       : {self.stats['clients_ingested']}")
        print(f"Erreurs               : {len(self.stats['errors'])}")
        
        if self.stats["errors"]:
            print("\n❌ ERREURS RENCONTRÉES:")
            for error in self.stats["errors"]:
                print(f"  - {error}")
        else:
            print("\n✅ Ingestion terminée sans erreur")
        
        print("="*80)


def main():
    """Point d'entrée principal du job."""
    print("="*80)
    print("🚀 DÉMARRAGE DU JOB D'INGESTION")
    print("="*80)
    
    # Configuration des chemins
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    raw_data_path = f"{base_path}/data/raw"
    raw_parquet_path = f"{base_path}/data/raw_parquet"
    
    # Création de la session Spark
    spark = SparkConfig.get_spark_session("Ingestion_Reclamations")
    
    try:
        # Initialisation du job
        job = IngestionJob(spark)
        
        # Ingestion des trois sources
        success = True
        success &= job.ingest_reclamations(
            f"{raw_data_path}/reclamations.csv",
            f"{raw_parquet_path}/reclamations"
        )
        success &= job.ingest_incidents(
            f"{raw_data_path}/incidents.csv",
            f"{raw_parquet_path}/incidents"
        )
        success &= job.ingest_clients(
            f"{raw_data_path}/clients.csv",
            f"{raw_parquet_path}/clients"
        )
        
        # Log des statistiques
        job.log_execution_stats()
        
        # Résumé
        job.print_summary()
        
        # Code de sortie
        if success:
            print("\n✅ Job d'ingestion terminé avec succès")
            sys.exit(0)
        else:
            print("\n❌ Job d'ingestion terminé avec des erreurs")
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
