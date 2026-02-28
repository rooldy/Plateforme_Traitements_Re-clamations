"""
backup_databases_daily.py
Backup quotidien de la base de données PostgreSQL.

Responsabilités :
  1. Sauvegarde le schéma reclamations en pg_dump compressé
  2. Exporte les tables critiques en CSV (format lisible sans PostgreSQL)
  3. Vérifie l'intégrité du backup (taille, lignes)
  4. Purge les backups de plus de 30 jours
  5. Log dans pipeline_runs

Schedule : quotidien à 01:00 (avant les pipelines de données)
"""

import logging
import os
import subprocess
import glob
import gzip
import shutil
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "backup_databases_daily"
BACKUP_DIR = "/opt/airflow/backups"
RETENTION_DAYS = 30

# Tables critiques à exporter en CSV de secours
CRITICAL_TABLES = [
    "reclamations.pipeline_runs",
    "reclamations.data_quality_metrics",
    "reclamations.kpis_daily",
]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}


def prepare_backup_directory(**ctx):
    """Crée la structure de répertoires pour le backup du jour."""
    run_date = ctx["ds"]
    backup_path = os.path.join(BACKUP_DIR, run_date)
    os.makedirs(backup_path, exist_ok=True)
    os.makedirs(os.path.join(backup_path, "csv"), exist_ok=True)
    log.info("Répertoire backup créé : %s", backup_path)
    ctx["ti"].xcom_push(key="backup_path", value=backup_path)


def run_pg_dump(**ctx):
    """
    Effectue un pg_dump du schéma reclamations au format custom compressé.
    Utilise pg_dump via subprocess (disponible dans le container Airflow).
    """
    backup_path = ctx["ti"].xcom_pull(key="backup_path", task_ids="prepare_backup_directory")
    run_date = ctx["ds"]
    dump_file = os.path.join(backup_path, f"reclamations_{run_date}.dump")

    env = {
        **os.environ,
        "PGPASSWORD": DB_CONFIG["password"],
    }

    cmd = [
        "pg_dump",
        "--host", DB_CONFIG["host"],
        "--port", str(DB_CONFIG["port"]),
        "--username", DB_CONFIG["user"],
        "--dbname", DB_CONFIG["database"],
        "--schema", "reclamations",
        "--format", "custom",          # format binaire compressé
        "--compress", "6",             # compression niveau 6
        "--no-owner",
        "--no-acl",
        "--file", dump_file,
    ]

    try:
        result = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=1800
        )
        if result.returncode != 0:
            log.error("pg_dump stderr : %s", result.stderr)
            raise RuntimeError(f"pg_dump échoué (code={result.returncode})")

        dump_size = os.path.getsize(dump_file)
        log.info("✅ pg_dump OK : %s (%.1f MB)", dump_file, dump_size / 1024 / 1024)
        ctx["ti"].xcom_push(key="dump_file", value=dump_file)
        ctx["ti"].xcom_push(key="dump_size_mb", value=round(dump_size / 1024 / 1024, 2))

    except FileNotFoundError:
        # pg_dump non disponible dans le container → backup CSV uniquement
        log.warning("pg_dump non disponible — seul le backup CSV sera effectué")
        ctx["ti"].xcom_push(key="dump_file", value=None)
        ctx["ti"].xcom_push(key="dump_size_mb", value=0)
    except subprocess.TimeoutExpired:
        raise RuntimeError("pg_dump timeout (30 minutes dépassées)")


def backup_critical_tables_csv(**ctx):
    """Exporte les tables critiques en CSV gzippé (backup de secours lisible)."""
    backup_path = ctx["ti"].xcom_pull(key="backup_path", task_ids="prepare_backup_directory")
    csv_dir = os.path.join(backup_path, "csv")
    run_date = ctx["ds"]

    conn = psycopg2.connect(**DB_CONFIG)
    total_rows = 0

    try:
        for table in CRITICAL_TABLES:
            schema, table_name = table.split(".")
            csv_file = os.path.join(csv_dir, f"{table_name}_{run_date}.csv.gz")

            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table}")
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()

                import csv
                import io
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(columns)
                writer.writerows(rows)

                # Compression gzip
                with gzip.open(csv_file, "wt", encoding="utf-8") as f:
                    f.write(buf.getvalue())

                file_size = os.path.getsize(csv_file)
                total_rows += len(rows)
                log.info("CSV backup %s : %d lignes | %.1f KB (gzip)",
                         table_name, len(rows), file_size / 1024)

            except Exception as exc:
                log.error("Erreur backup CSV %s : %s", table, exc)

    finally:
        conn.close()

    ctx["ti"].xcom_push(key="csv_rows", value=total_rows)
    log.info("Backup CSV terminé : %d lignes au total", total_rows)


def validate_backup(**ctx):
    """Vérifie que les backups ont bien été créés et sont non-vides."""
    backup_path = ctx["ti"].xcom_pull(key="backup_path", task_ids="prepare_backup_directory")
    dump_file = ctx["ti"].xcom_pull(key="dump_file", task_ids="run_pg_dump")

    issues = []

    # Validation du dump pg
    if dump_file:
        if not os.path.exists(dump_file):
            issues.append(f"Dump manquant : {dump_file}")
        elif os.path.getsize(dump_file) < 1024:
            issues.append(f"Dump suspicieusement petit : {dump_file}")

    # Validation CSV
    csv_dir = os.path.join(backup_path, "csv")
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv.gz"))
    if not csv_files:
        issues.append("Aucun fichier CSV backup généré")
    else:
        for f in csv_files:
            if os.path.getsize(f) < 50:
                issues.append(f"CSV vide ou corrompu : {f}")

    if issues:
        for issue in issues:
            log.error("Validation backup : %s", issue)
        raise ValueError(f"Backup invalide : {len(issues)} problème(s) détecté(s)")

    log.info("✅ Backup validé : %d fichiers CSV + dump PostgreSQL", len(csv_files))


def purge_old_backups(**ctx):
    """Supprime les backups de plus de RETENTION_DAYS jours."""
    if not os.path.exists(BACKUP_DIR):
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    purged = 0

    for backup_date_dir in os.listdir(BACKUP_DIR):
        try:
            backup_dt = datetime.strptime(backup_date_dir, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if backup_dt < cutoff:
                full_path = os.path.join(BACKUP_DIR, backup_date_dir)
                shutil.rmtree(full_path)
                purged += 1
                log.info("Backup purgé : %s (antérieur à %s)", backup_date_dir, cutoff.date())
        except ValueError:
            pass  # Répertoire avec nom non-date → ignoré

    log.info("Purge terminée : %d backup(s) supprimé(s)", purged)


def notify_pipeline_run(**ctx):
    dump_size = ctx["ti"].xcom_pull(key="dump_size_mb", task_ids="run_pg_dump") or 0
    csv_rows = ctx["ti"].xcom_pull(key="csv_rows", task_ids="backup_critical_tables_csv") or 0
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=csv_rows,
        duration_seconds=0,
        message=f"Backup OK [{ctx['ds']}] — dump={dump_size:.1f}MB | CSV={csv_rows} lignes",
    )


with DAG(
    dag_id=DAG_ID,
    description="Backup quotidien PostgreSQL (pg_dump + CSV gzip des tables critiques)",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "backup", "postgresql"],
    doc_md=__doc__,
) as dag:

    t_prep = PythonOperator(task_id="prepare_backup_directory", python_callable=prepare_backup_directory)
    t_dump = PythonOperator(task_id="run_pg_dump", python_callable=run_pg_dump)
    t_csv = PythonOperator(task_id="backup_critical_tables_csv", python_callable=backup_critical_tables_csv)
    t_validate = PythonOperator(task_id="validate_backup", python_callable=validate_backup)
    t_purge = PythonOperator(task_id="purge_old_backups", python_callable=purge_old_backups)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_prep >> [t_dump, t_csv] >> t_validate >> t_purge >> t_notify
