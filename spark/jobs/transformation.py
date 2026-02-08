"""
Job Spark : Transformations métier et enrichissements.
Enrichit les données, calcule les KPIs et détecte les anomalies.

Auteur: Data Engineering Portfolio  
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, count, when, lit, datediff, hour, minute,
    avg, stddev, abs as spark_abs, row_number,
    unix_timestamp, from_unixtime, current_timestamp,
    expr, sqrt, pow as spark_pow, dense_rank
)

# Ajout du chemin des utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig


class TransformationJob:
    """Job de transformation et enrichissement des données."""
    
    def __init__(self, spark_session):
        """
        Initialise le job de transformation.
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.stats = {
            "records_processed": 0,
            "records_enriched": 0,
            "anomalies_detected": 0
        }
    
    def calculate_durations(self, df: DataFrame) -> DataFrame:
        """
        Calcule les durées de traitement et délais de réponse.
        
        Args:
            df: DataFrame des réclamations
            
        Returns:
            DataFrame enrichi avec les durées
        """
        print("\n📊 Calcul des durées de traitement...")
        
        # Durée totale de traitement (en heures)
        df = df.withColumn(
            "duree_traitement_heures",
            when(
                col("date_cloture").isNotNull(),
                (unix_timestamp("date_cloture") - unix_timestamp("date_creation")) / 3600
            ).otherwise(None)
        )
        
        # Délai de première réponse (en heures)
        df = df.withColumn(
            "delai_premiere_reponse_heures",
            when(
                col("date_premiere_reponse").isNotNull(),
                (unix_timestamp("date_premiere_reponse") - unix_timestamp("date_creation")) / 3600
            ).otherwise(None)
        )
        
        print("  ✅ Durées calculées")
        return df
    
    def calculate_priority_score(self, df: DataFrame) -> DataFrame:
        """
        Calcule un score de priorité basé sur plusieurs facteurs.
        
        Args:
            df: DataFrame des réclamations
            
        Returns:
            DataFrame avec score de priorité
        """
        print("\n📊 Calcul des scores de priorité...")
        
        # Mapping priorité -> score de base
        df = df.withColumn(
            "score_base",
            when(col("priorite") == "CRITIQUE", 100)
            .when(col("priorite") == "HAUTE", 75)
            .when(col("priorite") == "NORMALE", 50)
            .when(col("priorite") == "BASSE", 25)
            .otherwise(50)
        )
        
        # Bonus si réclamation encore ouverte
        df = df.withColumn(
            "bonus_ouvert",
            when(col("statut").isin(["OUVERT", "EN_COURS"]), 20).otherwise(0)
        )
        
        # Malus si délai de première réponse dépassé (>24h)
        df = df.withColumn(
            "malus_delai",
            when(
                col("delai_premiere_reponse_heures") > 24,
                -10
            ).otherwise(0)
        )
        
        # Score final
        df = df.withColumn(
            "score_priorite",
            col("score_base") + col("bonus_ouvert") + col("malus_delai")
        )
        
        # Nettoyer colonnes temporaires
        df = df.drop("score_base", "bonus_ouvert", "malus_delai")
        
        print("  ✅ Scores de priorité calculés")
        return df
    
    def detect_recurrent_complaints(self, df: DataFrame) -> DataFrame:
        """
        Détecte les réclamations récurrentes (même client, même type, période récente).
        
        Args:
            df: DataFrame des réclamations
            
        Returns:
            DataFrame avec flag récurrence
        """
        print("\n📊 Détection des réclamations récurrentes...")
        
        # Window pour compter par client et type
        window_spec = Window.partitionBy("client_id", "type_reclamation") \
                            .orderBy("date_creation")
        
        # Compter le nombre de réclamations par client/type
        df = df.withColumn(
            "nombre_reclamations_client",
            count("*").over(Window.partitionBy("client_id", "type_reclamation"))
        )
        
        # Marquer comme récurrent si >= 2 réclamations du même type
        df = df.withColumn(
            "est_recurrent",
            when(col("nombre_reclamations_client") >= 2, lit(True)).otherwise(lit(False))
        )
        
        recurrent_count = df.filter(col("est_recurrent") == True).count()
        print(f"  ✅ {recurrent_count} réclamations récurrentes détectées")
        
        return df
    
    def correlate_with_incidents(self, reclamations_df: DataFrame, 
                                 incidents_df: DataFrame) -> DataFrame:
        """
        Corrèle les réclamations avec les incidents réseau géolocalisés.
        
        Args:
            reclamations_df: DataFrame des réclamations
            incidents_df: DataFrame des incidents
            
        Returns:
            DataFrame enrichi avec corrélations incidents
        """
        print("\n📊 Corrélation avec les incidents réseau...")
        
        # Jointure sur région et dates proches (±3 jours)
        # Calcul de distance géographique approximative (formule haversine simplifiée)
        correlated_df = reclamations_df.alias("r").join(
            incidents_df.alias("i"),
            (col("r.region") == col("i.region")) &
            (col("r.date_creation") >= col("i.date_incident")) &
            (col("r.date_creation") <= expr("date_add(i.date_resolution, 3)")),
            "left"
        )
        
        # Calculer distance approximative (en km) si coordonnées disponibles
        correlated_df = correlated_df.withColumn(
            "distance_incident_km",
            when(
                col("i.latitude").isNotNull() & col("r.latitude").isNotNull(),
                # Formule simplifiée : distance euclidienne * 111 km/degré
                sqrt(
                    spark_pow(col("r.latitude") - col("i.latitude"), 2) +
                    spark_pow(col("r.longitude") - col("i.longitude"), 2)
                ) * 111.0
            ).otherwise(None)
        )
        
        # Garder l'incident le plus proche si plusieurs matchent
        window_incident = Window.partitionBy("r.id_reclamation") \
                                .orderBy(col("distance_incident_km").asc_nulls_last())
        
        correlated_df = correlated_df.withColumn(
            "rn",
            row_number().over(window_incident)
        ).filter(col("rn") == 1).drop("rn")
        
        # Sélectionner les colonnes pertinentes
        final_df = correlated_df.select(
            col("r.*"),
            col("i.id_incident").alias("incident_lie"),
            col("distance_incident_km")
        )
        
        correlated_count = final_df.filter(col("incident_lie").isNotNull()).count()
        print(f"  ✅ {correlated_count} réclamations corrélées à des incidents")
        
        return final_df
    
    def detect_anomalies(self, df: DataFrame) -> list:
        """
        Détecte les anomalies statistiques (pics inhabituels).
        
        Args:
            df: DataFrame des réclamations
            
        Returns:
            Liste des anomalies détectées
        """
        print("\n📊 Détection d'anomalies statistiques...")
        
        anomalies = []
        
        # Agréger par région et jour
        daily_stats = df.groupBy("region", expr("date(date_creation)").alias("date")) \
                       .agg(count("*").alias("nb_reclamations"))
        
        # Calculer moyenne et écart-type par région
        region_stats = daily_stats.groupBy("region").agg(
            avg("nb_reclamations").alias("moyenne"),
            stddev("nb_reclamations").alias("ecart_type")
        )
        
        # Joindre avec les stats quotidiennes
        anomaly_df = daily_stats.join(region_stats, "region")
        
        # Calculer z-score
        anomaly_df = anomaly_df.withColumn(
            "z_score",
            (col("nb_reclamations") - col("moyenne")) / col("ecart_type")
        )
        
        # Détecter anomalies (|z-score| > 2)
        anomalies_detected = anomaly_df.filter(spark_abs(col("z_score")) > 2.0) \
                                      .orderBy(spark_abs(col("z_score")).desc())
        
        # Convertir en liste de dictionnaires
        for row in anomalies_detected.collect():
            anomaly = {
                "date_detection": self.execution_date,
                "type_anomalie": "PIC_RECLAMATIONS" if row["z_score"] > 0 else "BAISSE_RECLAMATIONS",
                "region": row["region"],
                "metrique": "nombre_reclamations_quotidiennes",
                "valeur_observee": float(row["nb_reclamations"]),
                "valeur_moyenne_historique": float(row["moyenne"]),
                "ecart_type_historique": float(row["ecart_type"]) if row["ecart_type"] else 0.0,
                "z_score": float(row["z_score"]),
                "severite": "CRITIQUE" if abs(row["z_score"]) > 3 else "HAUTE",
                "description": f"Pic inhabituel de réclamations détecté: {row['nb_reclamations']} réclamations " +
                              f"(moyenne: {row['moyenne']:.0f}, écart: {row['z_score']:.2f}σ)"
            }
            anomalies.append(anomaly)
        
        self.stats["anomalies_detected"] = len(anomalies)
        print(f"  ✅ {len(anomalies)} anomalies détectées")
        
        return anomalies
    
    def save_anomalies(self, anomalies: list):
        """
        Sauvegarde les anomalies détectées dans PostgreSQL.
        
        Args:
            anomalies: Liste des anomalies
        """
        if not anomalies:
            print("  ℹ️  Aucune anomalie à sauvegarder")
            return
        
        try:
            anomalies_df = self.spark.createDataFrame(anomalies)
            SparkConfig.write_to_postgres(anomalies_df, "reclamations.anomalies_detected", mode="append")
            print(f"  ✅ {len(anomalies)} anomalies sauvegardées dans PostgreSQL")
        except Exception as e:
            print(f"  ⚠️  Erreur sauvegarde anomalies: {str(e)}")
    
    def run_transformations(self, input_path: str, output_path: str, 
                          incidents_path: str) -> bool:
        """
        Exécute toutes les transformations.
        
        Args:
            input_path: Chemin données brutes Parquet
            output_path: Chemin sortie données transformées
            incidents_path: Chemin données incidents
            
        Returns:
            True si succès
        """
        print("\n" + "="*80)
        print("🔄 DÉMARRAGE DES TRANSFORMATIONS")
        print("="*80)
        
        try:
            # 1. Charger les données
            print("\n📂 Chargement des données...")
            reclamations_df = self.spark.read.parquet(f"{input_path}/reclamations")
            incidents_df = self.spark.read.parquet(f"{input_path}/incidents")
            
            initial_count = reclamations_df.count()
            self.stats["records_processed"] = initial_count
            print(f"  ✅ {initial_count} réclamations chargées")
            
            # 2. Calculs de durées
            reclamations_df = self.calculate_durations(reclamations_df)
            
            # 3. Calcul scores de priorité
            reclamations_df = self.calculate_priority_score(reclamations_df)
            
            # 4. Détection réclamations récurrentes
            reclamations_df = self.detect_recurrent_complaints(reclamations_df)
            
            # 5. Corrélation avec incidents
            reclamations_df = self.correlate_with_incidents(reclamations_df, incidents_df)
            
            self.stats["records_enriched"] = reclamations_df.count()
            
            # 6. Détection d'anomalies
            anomalies = self.detect_anomalies(reclamations_df)
            self.save_anomalies(anomalies)
            
            # 7. Sauvegarde en Parquet partitionné
            print("\n💾 Sauvegarde des données transformées...")
            reclamations_df.write \
                .partitionBy("region", expr("year(date_creation)"), expr("month(date_creation)")) \
                .mode("overwrite") \
                .parquet(f"{output_path}/reclamations")
            
            print(f"  ✅ Données sauvegardées dans {output_path}/reclamations")
            
            # Afficher un échantillon
            print("\n📊 Échantillon des données transformées:")
            reclamations_df.select(
                "id_reclamation", "type_reclamation", "score_priorite",
                "duree_traitement_heures", "est_recurrent", "incident_lie"
            ).show(5, truncate=False)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Erreur durant les transformations: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def print_summary(self):
        """Affiche le résumé de l'exécution."""
        print("\n" + "="*80)
        print("📊 RÉSUMÉ DES TRANSFORMATIONS")
        print("="*80)
        print(f"Réclamations traitées : {self.stats['records_processed']}")
        print(f"Réclamations enrichies : {self.stats['records_enriched']}")
        print(f"Anomalies détectées    : {self.stats['anomalies_detected']}")
        print("="*80)


def main():
    """Point d'entrée principal du job."""
    print("="*80)
    print("🔄 DÉMARRAGE DU JOB DE TRANSFORMATION")
    print("="*80)
    
    # Configuration des chemins
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    raw_parquet_path = f"{base_path}/data/raw_parquet"
    processed_path = f"{base_path}/data/processed"
    
    # Création de la session Spark
    spark = SparkConfig.get_spark_session("Transformation_Reclamations")
    
    try:
        # Initialisation du job
        job = TransformationJob(spark)
        
        # Exécution des transformations
        success = job.run_transformations(
            raw_parquet_path,
            processed_path,
            raw_parquet_path
        )
        
        # Résumé
        job.print_summary()
        
        # Code de sortie
        if success:
            print("\n✅ Transformations terminées avec succès")
            sys.exit(0)
        else:
            print("\n❌ Transformations terminées avec des erreurs")
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
