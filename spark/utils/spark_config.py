"""
Module de configuration PySpark pour le projet de traitement de réclamations.
Fournit des fonctions utilitaires pour initialiser et configurer les sessions Spark.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
import os
from typing import Dict, Optional


class SparkConfig:
    """Configuration centralisée pour les sessions PySpark."""
    
    @staticmethod
    def get_spark_session(app_name: str, config: Optional[Dict[str, str]] = None) -> SparkSession:
        """
        Crée ou récupère une session Spark configurée.
        
        Args:
            app_name: Nom de l'application Spark
            config: Configuration additionnelle (optionnel)
            
        Returns:
            SparkSession configurée
        """
        builder = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]")  # Mode local avec tous les cores disponibles
        
        # Configuration par défaut optimisée pour environnement local
        default_config = {
            # Mémoire et ressources
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.driver.maxResultSize": "1g",
            
            # Optimisations Spark SQL
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "8",  # Réduit pour environnement local
            
            # Sérialisation
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            
            # Compression
            "spark.sql.parquet.compression.codec": "snappy",
            
            # UI et logs
            "spark.ui.showConsoleProgress": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            
            # Optimisations I/O
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
            "spark.sql.files.openCostInBytes": "4194304",      # 4MB
            
            # Timezone
            "spark.sql.session.timeZone": "Europe/Paris",
            
            # PostgreSQL JDBC
            "spark.jars.packages": "org.postgresql:postgresql:42.7.1"
        }
        
        # Merge avec config personnalisée
        if config:
            default_config.update(config)
        
        # Application de la configuration
        for key, value in default_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Configuration du niveau de log
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @staticmethod
    def get_reclamations_schema() -> StructType:
        """
        Retourne le schéma strict pour les réclamations.
        
        Returns:
            StructType définissant le schéma
        """
        return StructType([
            StructField("id_reclamation", StringType(), False),
            StructField("client_id", StringType(), False),
            StructField("type_reclamation", StringType(), False),
            StructField("description", StringType(), False),
            StructField("canal", StringType(), False),
            StructField("priorite", StringType(), False),
            StructField("statut", StringType(), False),
            StructField("region", StringType(), False),
            StructField("departement", StringType(), False),
            StructField("adresse", StringType(), True),
            StructField("code_postal", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("reference_linky", StringType(), True),
            StructField("date_creation", TimestampType(), False),
            StructField("date_premiere_reponse", TimestampType(), True),
            StructField("date_cloture", TimestampType(), True)
        ])
    
    @staticmethod
    def get_incidents_schema() -> StructType:
        """
        Retourne le schéma strict pour les incidents réseau.
        
        Returns:
            StructType définissant le schéma
        """
        return StructType([
            StructField("id_incident", StringType(), False),
            StructField("type_incident", StringType(), False),
            StructField("date_incident", TimestampType(), False),
            StructField("date_resolution", TimestampType(), True),
            StructField("region", StringType(), False),
            StructField("departement", StringType(), False),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("clients_impactes", IntegerType(), True),
            StructField("description", StringType(), True)
        ])
    
    @staticmethod
    def get_clients_schema() -> StructType:
        """
        Retourne le schéma strict pour les clients.
        
        Returns:
            StructType définissant le schéma
        """
        return StructType([
            StructField("client_id", StringType(), False),
            StructField("nom", StringType(), False),
            StructField("prenom", StringType(), False),
            StructField("email", StringType(), True),
            StructField("telephone", StringType(), True),
            StructField("date_premier_contact", TimestampType(), False),
            StructField("region", StringType(), True)
        ])
    
    @staticmethod
    def get_postgres_config() -> Dict[str, str]:
        """
        Retourne la configuration de connexion PostgreSQL.
        
        Returns:
            Dictionnaire de configuration JDBC
        """
        return {
            "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:"
                   f"{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'reclamations_db')}",
            "user": os.getenv('POSTGRES_USER', 'airflow'),
            "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
            "driver": "org.postgresql.Driver"
        }
    
    @staticmethod
    def write_to_postgres(df, table_name: str, mode: str = "append"):
        """
        Écrit un DataFrame dans PostgreSQL.
        
        Args:
            df: DataFrame à écrire
            table_name: Nom de la table (avec schéma si nécessaire)
            mode: Mode d'écriture (append, overwrite, etc.)
        """
        postgres_config = SparkConfig.get_postgres_config()
        
        df.write \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .mode(mode) \
            .save()
    
    @staticmethod
    def read_from_postgres(spark: SparkSession, table_name: str):
        """
        Lit une table depuis PostgreSQL.
        
        Args:
            spark: Session Spark
            table_name: Nom de la table à lire
            
        Returns:
            DataFrame
        """
        postgres_config = SparkConfig.get_postgres_config()
        
        return spark.read \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .load()
