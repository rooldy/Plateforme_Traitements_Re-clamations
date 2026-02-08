"""
Job Spark : Export des données vers PostgreSQL.
Exporte les données transformées et calcule les KPIs quotidiens.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, min as spark_min,
    max as spark_max, when, percentile_approx, lit,
    expr, current_timestamp
)

# Ajout du chemin des utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig


class ExportPostgresJob:
    """Job d'export vers PostgreSQL."""
    
    def __init__(self, spark_session):
        """
        Initialise le job d'export.
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.stats = {
            "reclamations_exported": 0,
            "kpis_calculated": 0,
            "clients_updated": 0
        }
    
    def export_reclamations(self, df: DataFrame) -> bool:
        """
        Exporte les réclamations vers PostgreSQL.
        
        Args:
            df: DataFrame des réclamations transformées
            
        Returns:
            True si succès
        """
        print("\n💾 Export des réclamations vers PostgreSQL...")
        
        try:
            # Ajouter métadonnées d'export
            df = df.withColumn("date_insertion", current_timestamp()) \
                   .withColumn("date_mise_a_jour", current_timestamp())
            
            # Exporter
            SparkConfig.write_to_postgres(df, "reclamations.reclamations_cleaned", mode="append")
            
            self.stats["reclamations_exported"] = df.count()
            print(f"  ✅ {self.stats['reclamations_exported']} réclamations exportées")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Erreur export réclamations: {str(e)}")
            return False
    
    def calculate_daily_kpis(self, df: DataFrame) -> DataFrame:
        """
        Calcule les KPIs quotidiens par région et type.
        
        Args:
            df: DataFrame des réclamations
            
        Returns:
            DataFrame des KPIs
        """
        print("\n📊 Calcul des KPIs quotidiens...")
        
        # KPIs par région, date et type
        kpis_df = df.groupBy(
            expr("date(date_creation)").alias("date_calcul"),
            "region",
            "type_reclamation"
        ).agg(
            # Volumétrie
            count(when(col("statut").isin(["OUVERT", "EN_COURS"]), 1)).alias("nombre_reclamations_ouvertes"),
            count(when(col("statut") == "CLOTURE", 1)).alias("nombre_reclamations_cloturees"),
            count(when(col("statut") == "EN_COURS", 1)).alias("nombre_reclamations_en_cours"),
            
            # Temps de traitement
            avg("duree_traitement_heures").alias("duree_moyenne_traitement_heures"),
            percentile_approx("duree_traitement_heures", 0.5).alias("duree_mediane_traitement_heures"),
            avg("delai_premiere_reponse_heures").alias("delai_moyen_premiere_reponse_heures"),
            
            # Qualité de service (calculs simplifiés)
            (count(when(col("duree_traitement_heures") <= 48, 1)) * 100.0 / count("*"))
                .alias("taux_respect_sla"),
            (count(when(col("priorite") == "CRITIQUE", 1)) * 100.0 / count("*"))
                .alias("taux_reclamations_critiques"),
            (count(when(col("statut") == "ESCALADE", 1)) * 100.0 / count("*"))
                .alias("taux_escalade")
        )
        
        # Ajouter métadonnées
        kpis_df = kpis_df.withColumn("date_insertion", current_timestamp())
        
        print(f"  ✅ KPIs calculés pour {kpis_df.count()} combinaisons région/type/date")
        
        return kpis_df
    
    def export_kpis(self, kpis_df: DataFrame) -> bool:
        """
        Exporte les KPIs vers PostgreSQL.
        
        Args:
            kpis_df: DataFrame des KPIs
            
        Returns:
            True si succès
        """
        print("\n💾 Export des KPIs vers PostgreSQL...")
        
        try:
            SparkConfig.write_to_postgres(kpis_df, "reclamations.kpis_daily", mode="append")
            
            self.stats["kpis_calculated"] = kpis_df.count()
            print(f"  ✅ {self.stats['kpis_calculated']} KPIs exportés")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Erreur export KPIs: {str(e)}")
            return False
    
    def update_client_stats(self, df: DataFrame) -> bool:
        """
        Met à jour les statistiques clients.
        
        Args:
            df: DataFrame des réclamations
            
        Returns:
            True si succès
        """
        print("\n📊 Mise à jour des statistiques clients...")
        
        try:
            # Agréger par client
            client_stats = df.groupBy("client_id").agg(
                count("*").alias("nombre_total_reclamations"),
                count(when(col("statut").isin(["OUVERT", "EN_COURS"]), 1)).alias("nombre_reclamations_ouvertes"),
                avg("score_priorite").alias("score_satisfaction")
            )
            
            # Charger les clients existants
            clients_df = SparkConfig.read_from_postgres(self.spark, "reclamations.clients")
            
            # Joindre et mettre à jour
            updated_clients = clients_df.alias("c").join(
                client_stats.alias("s"),
                col("c.client_id") == col("s.client_id"),
                "left"
            ).select(
                col("c.client_id"),
                col("c.nom"),
                col("c.prenom"),
                col("c.email"),
                col("c.telephone"),
                col("c.date_premier_contact"),
                col("c.region"),
                when(col("s.nombre_total_reclamations").isNotNull(), 
                     col("s.nombre_total_reclamations")).otherwise(0).alias("nombre_total_reclamations"),
                when(col("s.nombre_reclamations_ouvertes").isNotNull(),
                     col("s.nombre_reclamations_ouvertes")).otherwise(0).alias("nombre_reclamations_ouvertes"),
                col("s.score_satisfaction"),
                current_timestamp().alias("date_mise_a_jour")
            )
            
            # Exporter (overwrite car c'est une mise à jour)
            SparkConfig.write_to_postgres(updated_clients, "reclamations.clients", mode="overwrite")
            
            self.stats["clients_updated"] = updated_clients.count()
            print(f"  ✅ {self.stats['clients_updated']} clients mis à jour")
            
            return True
            
        except Exception as e:
            print(f"  ⚠️  Erreur mise à jour clients: {str(e)}")
            # Non bloquant
            return True
    
    def refresh_materialized_views(self):
        """
        Rafraîchit les vues matérialisées PostgreSQL.
        """
        print("\n🔄 Rafraîchissement des vues matérialisées...")
        
        try:
            # Utiliser psycopg2 directement pour exécuter la fonction
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "reclamations-postgres"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                database=os.getenv("POSTGRES_DB", "reclamations_db"),
                user=os.getenv("POSTGRES_USER", "airflow"),
                password=os.getenv("POSTGRES_PASSWORD", "airflow")
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT reclamations.refresh_all_materialized_views();")
            conn.commit()
            cursor.close()
            conn.close()
            
            print("  ✅ Vues matérialisées rafraîchies")
            
        except Exception as e:
            print(f"  ⚠️  Erreur rafraîchissement vues: {str(e)}")
    
    def log_execution(self, success: bool):
        """
        Log l'exécution dans PostgreSQL.
        
        Args:
            success: Statut de l'exécution
        """
        try:
            from pyspark.sql import Row
            
            log_data = Row(
                job_name="export_postgres",
                execution_date=self.execution_date,
                status="SUCCESS" if success else "FAILED",
                start_time=self.execution_date,
                end_time=datetime.now(),
                records_processed=self.stats["reclamations_exported"],
                records_inserted=self.stats["reclamations_exported"] + self.stats["kpis_calculated"]
            )
            
            log_df = self.spark.createDataFrame([log_data])
            SparkConfig.write_to_postgres(log_df, "reclamations.pipeline_logs", mode="append")
            
            print("\n✅ Logs d'exécution sauvegardés")
            
        except Exception as e:
            print(f"⚠️  Impossible de sauvegarder les logs: {str(e)}")
    
    def run_export(self, input_path: str) -> bool:
        """
        Exécute l'export complet.
        
        Args:
            input_path: Chemin des données transformées
            
        Returns:
            True si succès
        """
        print("\n" + "="*80)
        print("💾 DÉMARRAGE DE L'EXPORT POSTGRESQL")
        print("="*80)
        
        try:
            # 1. Charger les données transformées
            print("\n📂 Chargement des données transformées...")
            df = self.spark.read.parquet(f"{input_path}/reclamations")
            print(f"  ✅ {df.count()} réclamations chargées")
            
            # 2. Export réclamations
            if not self.export_reclamations(df):
                return False
            
            # 3. Calcul et export KPIs
            kpis_df = self.calculate_daily_kpis(df)
            if not self.export_kpis(kpis_df):
                return False
            
            # 4. Mise à jour statistiques clients
            self.update_client_stats(df)
            
            # 5. Rafraîchir vues matérialisées
            self.refresh_materialized_views()
            
            return True
            
        except Exception as e:
            print(f"\n❌ Erreur durant l'export: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def print_summary(self):
        """Affiche le résumé de l'exécution."""
        print("\n" + "="*80)
        print("📊 RÉSUMÉ DE L'EXPORT")
        print("="*80)
        print(f"Réclamations exportées : {self.stats['reclamations_exported']}")
        print(f"KPIs calculés          : {self.stats['kpis_calculated']}")
        print(f"Clients mis à jour     : {self.stats['clients_updated']}")
        print("="*80)


def main():
    """Point d'entrée principal du job."""
    print("="*80)
    print("💾 DÉMARRAGE DU JOB D'EXPORT POSTGRESQL")
    print("="*80)
    
    # Configuration des chemins
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    processed_path = f"{base_path}/data/processed"
    
    # Création de la session Spark
    spark = SparkConfig.get_spark_session("Export_PostgreSQL_Reclamations")
    
    try:
        # Initialisation du job
        job = ExportPostgresJob(spark)
        
        # Exécution de l'export
        success = job.run_export(processed_path)
        
        # Log de l'exécution
        job.log_execution(success)
        
        # Résumé
        job.print_summary()
        
        # Code de sortie
        if success:
            print("\n✅ Export terminé avec succès")
            sys.exit(0)
        else:
            print("\n❌ Export terminé avec des erreurs")
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
