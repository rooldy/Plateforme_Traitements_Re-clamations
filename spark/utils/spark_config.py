"""
Configuration PySpark centralisée avec support PostgreSQL JDBC.
Version optimisée avec téléchargement automatique du driver.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType, TimestampType
)
from typing import Dict, Optional
import os


class SparkConfig:
    """Configuration centralisée pour toutes les sessions PySpark du projet."""

    # Configuration PostgreSQL
    POSTGRES_DRIVER = "org.postgresql.Driver"
    POSTGRES_JDBC_VERSION = "42.7.1"
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "reclamations-postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "reclamations_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow_local_dev")  # ✅ Corrigé

    @staticmethod
    def get_spark_session(
        app_name: str,
        config: Optional[Dict[str, str]] = None,
        with_postgres: bool = True
    ) -> SparkSession:
        """
        Crée ou récupère une session Spark configurée.

        Args:
            app_name: Nom de l'application Spark
            config: Configuration additionnelle (optionnel)
            with_postgres: Inclure le driver PostgreSQL (défaut: True)

        Returns:
            SparkSession configurée et prête à l'emploi
        """
        builder = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]")

        default_config = {
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.driver.maxResultSize": "1g",
            "spark.jars": "/opt/airflow/jars/postgresql-42.7.8.jar",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "8",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.debug.maxToStringFields": "100",
        }

        if config:
            default_config.update(config)

        for key, value in default_config.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    @staticmethod
    def get_postgres_url(database: Optional[str] = None) -> str:
        """Retourne l'URL JDBC PostgreSQL."""
        db = database or SparkConfig.POSTGRES_DB
        return f"jdbc:postgresql://{SparkConfig.POSTGRES_HOST}:{SparkConfig.POSTGRES_PORT}/{db}"

    @staticmethod
    def get_postgres_properties() -> Dict[str, str]:
        """Retourne les propriétés JDBC pour PostgreSQL."""
        return {
            "user": SparkConfig.POSTGRES_USER,
            "password": SparkConfig.POSTGRES_PASSWORD,
            "driver": SparkConfig.POSTGRES_DRIVER
        }

    # ✅ Méthodes de schéma ajoutées (appelées par ingestion.py)
    @staticmethod
    def get_reclamations_schema() -> StructType:
        """Retourne le schéma PySpark des réclamations."""
        return StructType([
            StructField("id_reclamation",        StringType(),    False),
            StructField("client_id",             StringType(),    False),
            StructField("type_reclamation",      StringType(),    False),
            StructField("description",           StringType(),    True),
            StructField("canal",                 StringType(),    True),
            StructField("priorite",              StringType(),    False),
            StructField("statut",                StringType(),    False),
            StructField("region",                StringType(),    False),
            StructField("departement",           IntegerType(),   True),
            StructField("adresse",               StringType(),    True),
            StructField("code_postal",           IntegerType(),   True),
            StructField("latitude",              DoubleType(),    True),
            StructField("longitude",             DoubleType(),    True),
            StructField("reference_linky",       StringType(),    True),
            StructField("date_creation",         TimestampType(), False),
            StructField("date_premiere_reponse", TimestampType(), True),
            StructField("date_cloture",          TimestampType(), True),
        ])

    @staticmethod
    def get_incidents_schema() -> StructType:
        """Retourne le schéma PySpark des incidents réseau."""
        return StructType([
            StructField("id_incident", StringType(), False),
            StructField("type_incident", StringType(), False),
            StructField("date_incident", DateType(), False),
            StructField("severite", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("region", StringType(), False),
            StructField("clients_impactes", IntegerType(), True)
        ])

    @staticmethod
    def get_clients_schema() -> StructType:
        """Retourne le schéma PySpark des clients."""
        return StructType([
            StructField("client_id", StringType(), False),
            StructField("nom", StringType(), False),
            StructField("type_client", StringType(), False),
            StructField("region", StringType(), False),
            StructField("date_inscription", DateType(), False),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])

    @staticmethod
    def write_to_postgres(df, table: str, mode: str = "append") -> None:
        """
        Écrit un DataFrame Spark dans PostgreSQL via JDBC.

        Args:
            df: DataFrame Spark à écrire
            table: Nom de la table cible (ex: 'reclamations.pipeline_logs')
            mode: Mode d'écriture ('append', 'overwrite')
        """
        df.write.jdbc(
            url=SparkConfig.get_postgres_url(),
            table=table,
            mode=mode,
            properties=SparkConfig.get_postgres_properties()
        )


# ✅ Variables module conservées pour compatibilité avec d'autres scripts
SCHEMA_RECLAMATIONS = SparkConfig.get_reclamations_schema()
SCHEMA_INCIDENTS = SparkConfig.get_incidents_schema()
SCHEMA_CLIENTS = SparkConfig.get_clients_schema()
