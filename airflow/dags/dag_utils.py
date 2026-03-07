"""
dag_utils.py — Utilitaires partagés pour tous les DAGs
Plateforme Data Réclamations — Data Engineering Pôle Innovation
"""

import csv
import logging
import tempfile
import os
import psycopg2
from datetime import datetime, timezone

# ─── Configuration PostgreSQL ──────────────────────────────────────────────────
DB_CONFIG = {
    "host": "reclamations-postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# ─── SLA définis par type de réclamation ──────────────────────────────────────
SLA_HEURES = {
    "COUPURE_ELECTRIQUE": 48,
    "COMPTEUR_LINKY": 120,
    "FACTURATION": 240,
    "RACCORDEMENT_RESEAU": 360,
    "INTERVENTION_TECHNIQUE": 168,
}

# ─── Spark JAR PostgreSQL ─────────────────────────────────────────────────────
SPARK_JAR = "/opt/airflow/jars/postgresql-42.7.8.jar"

log = logging.getLogger(__name__)


# ─── Logging pipeline_runs ────────────────────────────────────────────────────
def log_pipeline_run(
    dag_id: str,
    status: str,
    rows_processed: int,
    duration_seconds: float,
    message: str = "",
) -> None:
    """Insère une ligne dans reclamations.pipeline_runs (pattern standard)."""
    sql = """
        INSERT INTO reclamations.pipeline_runs
            (dag_id, run_date, status, rows_processed, duration_seconds, message, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(sql, (
                dag_id,
                datetime.now(timezone.utc).date(),
                status,
                rows_processed,
                round(duration_seconds, 2),
                message[:1000] if message else "",
                datetime.now(timezone.utc),
            ))
        conn.commit()
        log.info("pipeline_runs → %s | %s | %d rows | %.1fs", dag_id, status, rows_processed, duration_seconds)
    except Exception as exc:
        log.error("Erreur log_pipeline_run: %s", exc)
    finally:
        if conn:
            conn.close()


# ─── Import PostgreSQL via COPY ───────────────────────────────────────────────
def copy_df_to_postgres(df_pandas, table: str, columns: list[str], truncate: bool = False) -> int:
    """
    Exporte un DataFrame pandas vers PostgreSQL via COPY (pattern CSV + psycopg2).
    Retourne le nombre de lignes importées.
    """
    conn = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            tmp_path = f.name
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(columns)
            for row in df_pandas[columns].itertuples(index=False):
                writer.writerow(row)

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {table}")
            with open(tmp_path, "r") as f:
                next(f)  # skip header
                cur.copy_from(f, table, sep=",", columns=columns, null="")
        conn.commit()
        rows = len(df_pandas)
        log.info("COPY → %s : %d lignes importées", table, rows)
        return rows
    finally:
        if conn:
            conn.close()
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ─── Suppression colonnes de partition Spark ──────────────────────────────────
def drop_partition_cols(df):
    """Supprime les colonnes de partition Spark (annee, mois, region) avant export."""
    partition_cols = [c for c in ["annee", "mois", "region"] if c in df.columns]
    if partition_cols:
        df = df.drop(*partition_cols)
    return df


# ─── Création SparkSession standard ──────────────────────────────────────────
def get_spark_session(app_name: str):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", SPARK_JAR)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
