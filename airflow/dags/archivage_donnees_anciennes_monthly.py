"""
archivage_donnees_anciennes_monthly.py
Archivage mensuel des données anciennes de production.

Responsabilités :
  1. Archive les données de plus de 12 mois dans des tables d'archive _archive
  2. Compresse et exporte les archives au format Parquet (via Spark)
  3. Supprime les données archivées des tables de production (après validation)
  4. Met à jour les statistiques PostgreSQL (ANALYZE)
  5. Génère un rapport d'archivage
  6. Log dans pipeline_runs

Schedule : 1er de chaque mois à 01:00 (avant les autres DAGs)
"""

import logging
import os
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SPARK_JAR, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "archivage_donnees_anciennes_monthly"
ARCHIVE_DIR = "/opt/airflow/data/archives"
RETENTION_MONTHS = 12  # Données de plus de 12 mois = éligibles à l'archivage

# Tables à archiver et leur colonne de date de référence
TABLES_A_ARCHIVER = [
    ("reclamations.reclamations_processed",         "date_creation",  "ingestion_date"),
    ("reclamations.reclamations_cleaned",           "date_creation",  None),
    ("reclamations.kpis_daily",                     "date_calcul",       None),
    ("reclamations.reclamations_global_consolidated","export_date",    None),
    ("reclamations.reclamations_coupures_detail",   "export_date",    None),
    ("reclamations.reclamations_facturation_detail","export_date",    None),
    ("reclamations.reclamations_raccordement_detail","export_date",   None),
    ("reclamations.anomalies_detected",             "detection_date", None),
]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
}


def create_archive_tables(**ctx):
    """Crée les tables d'archive (structure identique aux tables sources)."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for table, date_col, _ in TABLES_A_ARCHIVER:
                schema, table_name = table.split(".")
                archive_table = f"{schema}.{table_name}_archive"
                try:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {archive_table}
                        AS TABLE {table} WITH NO DATA;
                        COMMENT ON TABLE {archive_table} IS
                        'Archive de {table} — données > {RETENTION_MONTHS} mois';
                    """)
                    log.info("Table archive prête : %s", archive_table)
                except Exception as exc:
                    log.warning("Table archive %s déjà existante ou erreur : %s", archive_table, exc)
            conn.commit()
    finally:
        conn.close()


def compute_archive_scope(**ctx):
    """Calcule la date limite d'archivage et le volume de données éligibles."""
    run_dt = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).replace(tzinfo=None)
    cutoff_date = (run_dt - relativedelta(months=RETENTION_MONTHS)).date()

    log.info("Date de coupure archivage : %s (données antérieures à cette date)", cutoff_date)

    conn = psycopg2.connect(**DB_CONFIG)
    scope = {}
    try:
        with conn.cursor() as cur:
            for table, date_col, _ in TABLES_A_ARCHIVER:
                try:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE {date_col} < %s
                    """, (cutoff_date,))
                    count = cur.fetchone()[0]
                    scope[table] = {"count": count, "date_col": date_col, "cutoff": str(cutoff_date)}
                    log.info("  %s : %d lignes éligibles (avant %s)", table, count, cutoff_date)
                except Exception as exc:
                    log.warning("Impossible de compter %s : %s", table, exc)
                    scope[table] = {"count": 0, "error": str(exc)}
    finally:
        conn.close()

    ctx["ti"].xcom_push(key="archive_scope", value=scope)
    ctx["ti"].xcom_push(key="cutoff_date", value=str(cutoff_date))
    total_eligible = sum(v.get("count", 0) for v in scope.values())
    log.info("Total lignes éligibles à l'archivage : %d", total_eligible)


def archive_old_data(**ctx):
    """
    Copie les données anciennes dans les tables _archive
    puis les supprime des tables de production.
    Opération transactionnelle par table.
    """
    scope = ctx["ti"].xcom_pull(key="archive_scope", task_ids="compute_archive_scope") or {}
    cutoff_date = ctx["ti"].xcom_pull(key="cutoff_date", task_ids="compute_archive_scope")

    total_archived = 0
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        for table, date_col, _ in TABLES_A_ARCHIVER:
            table_scope = scope.get(table, {})
            eligible = table_scope.get("count", 0)

            if eligible == 0:
                log.info("Skip %s (0 lignes éligibles)", table)
                continue

            schema, table_name = table.split(".")
            archive_table = f"{schema}.{table_name}_archive"

            try:
                with conn.cursor() as cur:
                    # Copie dans la table d'archive
                    cur.execute(f"""
                        INSERT INTO {archive_table}
                        SELECT * FROM {table}
                        WHERE {date_col} < %s
                        ON CONFLICT DO NOTHING
                    """, (cutoff_date,))
                    archived = cur.rowcount

                    # Suppression de la table de production
                    cur.execute(f"""
                        DELETE FROM {table}
                        WHERE {date_col} < %s
                    """, (cutoff_date,))
                    deleted = cur.rowcount

                conn.commit()
                total_archived += archived
                log.info("✅ Archivage %s : %d copiées → %s | %d supprimées de prod",
                         table, archived, archive_table, deleted)

                if archived != deleted:
                    log.warning("Incohérence archivage %s : archived=%d vs deleted=%d",
                                table, archived, deleted)

            except Exception as exc:
                conn.rollback()
                log.error("Erreur archivage %s : %s — rollback effectué", table, exc)

    finally:
        conn.close()

    ctx["ti"].xcom_push(key="total_archived", value=total_archived)
    log.info("Archivage terminé : %d lignes archivées au total", total_archived)


def export_archives_to_parquet(**ctx):
    """Exporte les données archivées en Parquet compressé pour stockage long terme."""
    cutoff_date = ctx["ti"].xcom_pull(key="cutoff_date", task_ids="compute_archive_scope")
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")

    from dag_utils import get_spark_session
    import os

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    spark = get_spark_session("ArchiveExport")

    try:
        for table, _, _ in TABLES_A_ARCHIVER:
            schema, table_name = table.split(".")
            archive_table = f"{schema}.{table_name}_archive"
            export_path = os.path.join(ARCHIVE_DIR, table_name, run_date)

            try:
                # Lecture depuis PostgreSQL via JDBC (usage acceptable en maintenance)
                df = (
                    spark.read
                    .format("jdbc")
                    .option("url", f"jdbc:postgresql://postgres:5432/airflow")
                    .option("dbtable", archive_table)
                    .option("user", "airflow")
                    .option("password", "airflow")
                    .option("driver", "org.postgresql.Driver")
                    .load()
                )

                if df.count() > 0:
                    df.write.mode("overwrite").parquet(export_path)
                    log.info("Parquet archive : %s → %s (%d lignes)",
                             archive_table, export_path, df.count())
                else:
                    log.info("Archive vide, skip Parquet : %s", archive_table)

            except Exception as exc:
                log.warning("Export Parquet %s échoué (%s) — continuité", archive_table, exc)
    finally:
        spark.stop()


def update_statistics(**ctx):
    """ANALYZE les tables pour mettre à jour les statistiques PostgreSQL après suppression."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for table, _, _ in TABLES_A_ARCHIVER:
                try:
                    cur.execute(f"ANALYZE {table}")
                    log.info("ANALYZE OK : %s", table)
                except Exception as exc:
                    log.warning("ANALYZE échoué pour %s : %s", table, exc)
        conn.commit()
    finally:
        conn.close()


def generate_archive_report(**ctx):
    """Génère le rapport d'archivage mensuel."""
    total_archived = ctx["ti"].xcom_pull(key="total_archived", task_ids="archive_old_data") or 0
    cutoff_date = ctx["ti"].xcom_pull(key="cutoff_date", task_ids="compute_archive_scope")
    scope = ctx["ti"].xcom_pull(key="archive_scope", task_ids="compute_archive_scope") or {}

    log.info("=== Rapport Archivage Mensuel [%s] ===", (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"))
    log.info("Date de coupure : %s | Total archivé : %d lignes", cutoff_date, total_archived)
    log.info("Détail par table :")
    for table, info in scope.items():
        log.info("  %-55s : %d lignes éligibles", table, info.get("count", 0))


def notify_pipeline_run(**ctx):
    total = ctx["ti"].xcom_pull(key="total_archived", task_ids="archive_old_data") or 0
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=total,
        duration_seconds=0,
        message=f"Archivage mensuel OK [{(ctx.get('logical_date') or ctx.get('data_interval_start') or __import__('datetime').datetime.now()).strftime('%Y-%m-%d')}] — {total} lignes archivées",
    )


with DAG(
    dag_id=DAG_ID,
    description="Archivage mensuel des données de plus de 12 mois",
    schedule="0 1 1 * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "archivage", "mensuel"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_archive_tables", python_callable=create_archive_tables)
    t_scope = PythonOperator(task_id="compute_archive_scope", python_callable=compute_archive_scope)
    t_archive = PythonOperator(task_id="archive_old_data", python_callable=archive_old_data)
    t_parquet = PythonOperator(task_id="export_archives_to_parquet", python_callable=export_archives_to_parquet)
    t_analyze = PythonOperator(task_id="update_statistics", python_callable=update_statistics)
    t_report = PythonOperator(task_id="generate_archive_report", python_callable=generate_archive_report)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_scope >> t_archive >> t_parquet >> t_analyze >> t_report >> t_notify
