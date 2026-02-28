"""
Job Spark : Quality Check des données.
Vérifie la qualité des données ingérées et détecte les anomalies.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, sum as spark_sum,
    countDistinct, lit, datediff, current_timestamp
)

# Ajout du chemin des utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.spark_config import SparkConfig


class QualityCheckJob:
    """Job de vérification qualité des données."""
    
    def __init__(self, spark_session):
        """
        Initialise le job de quality check.
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.execution_date = datetime.now()
        self.quality_metrics = {}
        self.issues_detected = []
    
    def check_completeness(self, df: DataFrame, table_name: str) -> dict:
        """
        Vérifie la complétude des données (valeurs nulles).
        
        Args:
            df: DataFrame à vérifier
            table_name: Nom de la table
            
        Returns:
            Dictionnaire avec les métriques de complétude
        """
        print(f"\n🔍 Vérification de complétude pour {table_name}...")
        
        total_records = df.count()
        
        # Compter les nulls par colonne
        null_counts = {}
        for column in df.columns:
            # isnan() uniquement sur colonnes numériques (pas DATE, STRING)
            numeric_types = ['double', 'float', 'int', 'long', 'short', 'decimal']
            col_type = dict(df.dtypes).get(column, '')
            if any(t in col_type for t in numeric_types):
                null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            else:
                null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = null_count
            
            if null_count > 0:
                pct = (null_count / total_records) * 100
                print(f"  ⚠️  {column}: {null_count} nulls ({pct:.2f}%)")
        
        # Calculer le score de complétude global
        total_cells = total_records * len(df.columns)
        total_nulls = sum(null_counts.values())
        completeness_score = ((total_cells - total_nulls) / total_cells) * 100 if total_cells > 0 else 0
        
        metrics = {
            "table_name": table_name,
            "total_records": total_records,
            "total_nulls": total_nulls,
            "completeness_score": round(completeness_score, 2),
            "null_by_column": null_counts
        }
        
        print(f"  ✅ Score de complétude : {completeness_score:.2f}%")
        return metrics
    
    def check_duplicates(self, df: DataFrame, key_columns: list, table_name: str) -> dict:
        """
        Détecte les doublons basés sur des colonnes clés.
        
        Args:
            df: DataFrame à vérifier
            key_columns: Colonnes constituant la clé unique
            table_name: Nom de la table
            
        Returns:
            Dictionnaire avec les métriques de doublons
        """
        print(f"\n🔍 Détection de doublons pour {table_name}...")
        
        total_records = df.count()
        unique_records = df.dropDuplicates(key_columns).count()
        duplicate_count = total_records - unique_records
        
        uniqueness_score = (unique_records / total_records) * 100 if total_records > 0 else 0
        
        if duplicate_count > 0:
            print(f"  ⚠️  {duplicate_count} doublons détectés ({(duplicate_count/total_records)*100:.2f}%)")
            
            # Identifier les doublons
            duplicates_df = df.groupBy(key_columns).count().filter(col("count") > 1)
            print(f"  📊 Exemples de doublons :")
            duplicates_df.show(5, truncate=False)
        else:
            print(f"  ✅ Aucun doublon détecté")
        
        metrics = {
            "table_name": table_name,
            "total_records": total_records,
            "unique_records": unique_records,
            "duplicate_count": duplicate_count,
            "uniqueness_score": round(uniqueness_score, 2)
        }
        
        return metrics
    
    def check_referential_integrity(self, df: DataFrame, ref_column: str, 
                                   valid_values: list, table_name: str) -> dict:
        """
        Vérifie l'intégrité référentielle (valeurs valides).
        
        Args:
            df: DataFrame à vérifier
            ref_column: Colonne à vérifier
            valid_values: Liste des valeurs valides
            table_name: Nom de la table
            
        Returns:
            Dictionnaire avec les métriques de validité
        """
        print(f"\n🔍 Vérification d'intégrité pour {table_name}.{ref_column}...")
        
        total_records = df.count()
        invalid_records = df.filter(~col(ref_column).isin(valid_values)).count()
        
        validity_score = ((total_records - invalid_records) / total_records) * 100 if total_records > 0 else 0
        
        if invalid_records > 0:
            print(f"  ⚠️  {invalid_records} valeurs invalides ({(invalid_records/total_records)*100:.2f}%)")
            
            # Afficher les valeurs invalides
            invalid_df = df.filter(~col(ref_column).isin(valid_values)) \
                          .groupBy(ref_column).count() \
                          .orderBy(col("count").desc())
            print(f"  📊 Valeurs invalides trouvées :")
            invalid_df.show(10, truncate=False)
        else:
            print(f"  ✅ Toutes les valeurs sont valides")
        
        metrics = {
            "table_name": table_name,
            "column": ref_column,
            "total_records": total_records,
            "invalid_records": invalid_records,
            "validity_score": round(validity_score, 2)
        }
        
        return metrics
    
    def check_date_consistency(self, df: DataFrame, table_name: str) -> dict:
        """
        Vérifie la cohérence des dates.
        
        Args:
            df: DataFrame à vérifier
            table_name: Nom de la table
            
        Returns:
            Dictionnaire avec les métriques de cohérence
        """
        print(f"\n🔍 Vérification de cohérence des dates pour {table_name}...")
        
        total_records = df.count()
        
        # Vérifier que date_creation <= date_premiere_reponse
        if "date_premiere_reponse" in df.columns:
            inconsistent_dates = df.filter(
                col("date_premiere_reponse").isNotNull() &
                (col("date_creation") > col("date_premiere_reponse"))
            ).count()
            
            if inconsistent_dates > 0:
                print(f"  ⚠️  {inconsistent_dates} dates incohérentes (création > première réponse)")
        
        # Vérifier que date_creation <= date_cloture
        if "date_cloture" in df.columns:
            inconsistent_closure = df.filter(
                col("date_cloture").isNotNull() &
                (col("date_creation") > col("date_cloture"))
            ).count()
            
            if inconsistent_closure > 0:
                print(f"  ⚠️  {inconsistent_closure} dates incohérentes (création > clôture)")
        
        # Vérifier les dates futures
        future_dates = df.filter(col("date_creation") > current_timestamp()).count()
        if future_dates > 0:
            print(f"  ⚠️  {future_dates} dates dans le futur")
        
        total_inconsistent = inconsistent_dates + inconsistent_closure + future_dates if "date_premiere_reponse" in df.columns else 0
        consistency_score = ((total_records - total_inconsistent) / total_records) * 100 if total_records > 0 else 0
        
        metrics = {
            "table_name": table_name,
            "total_records": total_records,
            "inconsistent_records": total_inconsistent,
            "consistency_score": round(consistency_score, 2)
        }
        
        print(f"  ✅ Score de cohérence : {consistency_score:.2f}%")
        return metrics
    
    def check_linky_format(self, df: DataFrame) -> dict:
        """
        Vérifie le format des références Linky.
        
        Args:
            df: DataFrame contenant les références Linky
            
        Returns:
            Dictionnaire avec les métriques
        """
        print(f"\n🔍 Vérification format références Linky...")
        
        # Filtrer les lignes avec référence Linky
        linky_df = df.filter(col("reference_linky").isNotNull() & (col("reference_linky") != ""))
        total_linky = linky_df.count()
        
        if total_linky == 0:
            print("  ℹ️  Aucune référence Linky à vérifier")
            return {"valid_format": 0, "invalid_format": 0}
        
        # Format attendu : LKY suivi de 8 chiffres
        valid_format = linky_df.filter(col("reference_linky").rlike("^LKY[0-9]{8}$")).count()
        invalid_format = total_linky - valid_format
        
        if invalid_format > 0:
            print(f"  ⚠️  {invalid_format} références Linky au format invalide")
            
            # Afficher des exemples
            invalid_df = linky_df.filter(~col("reference_linky").rlike("^LKY[0-9]{8}$"))
            print(f"  📊 Exemples de références invalides :")
            invalid_df.select("reference_linky").show(5, truncate=False)
        else:
            print(f"  ✅ Toutes les références Linky sont valides")
        
        return {
            "total_linky_records": total_linky,
            "valid_format": valid_format,
            "invalid_format": invalid_format
        }
    
    def run_quality_checks(self, input_path: str) -> bool:
        """
        Exécute tous les contrôles qualité sur les réclamations.
        
        Args:
            input_path: Chemin des données Parquet brutes
            
        Returns:
            True si la qualité est acceptable, False sinon
        """
        print("\n" + "="*80)
        print("🔍 DÉMARRAGE DES CONTRÔLES QUALITÉ")
        print("="*80)
        
        # Charger les données
        df = self.spark.read.parquet(f"{input_path}/reclamations")
        
        # 1. Complétude
        completeness_metrics = self.check_completeness(df, "reclamations")
        self.quality_metrics["completeness"] = completeness_metrics
        
        # 2. Doublons
        uniqueness_metrics = self.check_duplicates(
            df, 
            ["id_reclamation"],
            "reclamations"
        )
        self.quality_metrics["uniqueness"] = uniqueness_metrics
        
        # 3. Intégrité référentielle - Type réclamation
        valid_types = [
            "RACCORDEMENT_RESEAU",
            "COMPTEUR_LINKY",
            "COUPURE_ELECTRIQUE",
            "FACTURATION",
            "INTERVENTION_TECHNIQUE"
        ]
        validity_metrics = self.check_referential_integrity(
            df,
            "type_reclamation",
            valid_types,
            "reclamations"
        )
        self.quality_metrics["validity"] = validity_metrics
        
        # 4. Cohérence des dates
        consistency_metrics = self.check_date_consistency(df, "reclamations")
        self.quality_metrics["consistency"] = consistency_metrics
        
        # 5. Format Linky
        linky_metrics = self.check_linky_format(df)
        self.quality_metrics["linky_format"] = linky_metrics
        
        # Calculer le score global
        overall_score = (
            completeness_metrics["completeness_score"] * 0.3 +
            uniqueness_metrics["uniqueness_score"] * 0.3 +
            validity_metrics["validity_score"] * 0.2 +
            consistency_metrics["consistency_score"] * 0.2
        )
        
        self.quality_metrics["overall_score"] = round(overall_score, 2)
        
        # Déterminer si la qualité est acceptable (seuil: 90%)
        passed = overall_score >= 90.0
        
        print("\n" + "="*80)
        print("📊 RÉSUMÉ DES CONTRÔLES QUALITÉ")
        print("="*80)
        print(f"Score de complétude  : {completeness_metrics['completeness_score']:.2f}%")
        print(f"Score d'unicité      : {uniqueness_metrics['uniqueness_score']:.2f}%")
        print(f"Score de validité    : {validity_metrics['validity_score']:.2f}%")
        print(f"Score de cohérence   : {consistency_metrics['consistency_score']:.2f}%")
        print(f"\n{'✅' if passed else '❌'} Score global : {overall_score:.2f}%")
        print("="*80)
        
        return passed
    
    def save_quality_metrics(self):
        """Sauvegarde les métriques qualité dans PostgreSQL."""
        try:
            from pyspark.sql import Row
            
            metrics_data = Row(
                date_controle=self.execution_date,
                job_name="quality_check",
                table_name="reclamations",
                total_records=self.quality_metrics.get("completeness", {}).get("total_records", 0),
                records_with_nulls=self.quality_metrics.get("completeness", {}).get("total_nulls", 0),
                completeness_score=self.quality_metrics.get("completeness", {}).get("completeness_score", 0),
                duplicate_records=self.quality_metrics.get("uniqueness", {}).get("duplicate_count", 0),
                uniqueness_score=self.quality_metrics.get("uniqueness", {}).get("uniqueness_score", 0),
                invalid_records=self.quality_metrics.get("validity", {}).get("invalid_records", 0),
                validity_score=self.quality_metrics.get("validity", {}).get("validity_score", 0),
                inconsistent_records=self.quality_metrics.get("consistency", {}).get("inconsistent_records", 0),
                consistency_score=self.quality_metrics.get("consistency", {}).get("consistency_score", 0),
                overall_quality_score=self.quality_metrics.get("overall_score", 0),
                passed=self.quality_metrics.get("overall_score", 0) >= 90.0
            )
            
            metrics_df = self.spark.createDataFrame([metrics_data])
            SparkConfig.write_to_postgres(metrics_df, "reclamations.data_quality_metrics", mode="append")
            
            print("\n✅ Métriques qualité sauvegardées dans PostgreSQL")
            
        except Exception as e:
            print(f"⚠️  Impossible de sauvegarder les métriques qualité: {str(e)}")


def main():
    """Point d'entrée principal du job."""
    print("="*80)
    print("🔍 DÉMARRAGE DU JOB QUALITY CHECK")
    print("="*80)
    
    # Configuration des chemins
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    raw_parquet_path = f"{base_path}/data/raw_parquet"
    
    # Création de la session Spark
    spark = SparkConfig.get_spark_session("Quality_Check_Reclamations")
    
    try:
        # Initialisation du job
        job = QualityCheckJob(spark)
        
        # Exécution des contrôles
        passed = job.run_quality_checks(raw_parquet_path)
        
        # Sauvegarde des métriques
        job.save_quality_metrics()
        
        # Code de sortie
        if passed:
            print("\n✅ Contrôles qualité RÉUSSIS - Données acceptables")
            sys.exit(0)
        else:
            print("\n⚠️  Contrôles qualité ÉCHOUÉS - Qualité insuffisante")
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
