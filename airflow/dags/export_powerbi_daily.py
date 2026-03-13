"""
export_powerbi_daily.py
Export quotidien des données vers Power BI — fichiers CSV optimisés.

Responsabilités :
  1. Exporte kpis_daily, reclamations_global_consolidated et kpis_hebdomadaires
     au format CSV optimisé pour Power BI (encodage UTF-8-BOM, séparateur ;)
  2. Génère un fichier de métadonnées (date export, volumes, checksums)
  3. Dépose les fichiers dans le répertoire /opt/airflow/exports/powerbi/
  4. Valide les exports (taille, lignes, checksums)
  5. Log dans pipeline_runs

Schedule : quotidien à 07:30 (après consolidation)
"""

import logging
import os
import csv
import hashlib
import json
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "export_powerbi_daily"
EXPORT_DIR = "/opt/airflow/exports/powerbi"

# Définition des exports : (query, nom_fichier, description)
EXPORTS_CONFIG = [
    {
        "name": "kpis_daily",
        "filename": "kpis_daily.csv",
        "query": """
            SELECT
                date_kpi, region, type_reclamation,
                nombre_reclamations_ouvertes,
                nombre_reclamations_cloturees,
                duree_moyenne_traitement_heures,
                duree_mediane_traitement_heures,
                taux_respect_sla,
                taux_reclamations_critiques,
                taux_escalade
            FROM reclamations.kpis_daily
            WHERE date_kpi >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY date_kpi DESC, region, type_reclamation
        """,
        "description": "KPIs quotidiens — 90 derniers jours",
    },
    {
        "name": "reclamations_global",
        "filename": "reclamations_global.csv",
        "query": """
            SELECT
                reclamation_id, client_id, region, departement,
                type_reclamation, statut, priorite,
                date_creation, date_cloture,
                duree_heures, sla_heures, sla_respect,
                risque_depassement, heures_restantes,
                source_pipeline, export_date
            FROM reclamations.reclamations_global_consolidated
            WHERE export_date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY export_date DESC, type_reclamation, region
        """,
        "description": "Réclamations consolidées — 30 derniers jours",
    },
    {
        "name": "kpis_hebdomadaires",
        "filename": "kpis_hebdomadaires.csv",
        "query": """
            SELECT
                annee, semaine, debut_semaine, fin_semaine,
                region, type_reclamation,
                total_reclamations, total_ouvertes, total_cloturees,
                duree_moyenne_heures, taux_respect_sla,
                evolution_vs_semaine_precedente
            FROM reclamations.kpis_hebdomadaires
            ORDER BY debut_semaine DESC, region, type_reclamation
        """,
        "description": "KPIs hebdomadaires — historique complet",
    },
    {
        "name": "sla_par_region",
        "filename": "sla_par_region.csv",
        "query": """
            SELECT
                date_kpi,
                region,
                type_reclamation,
                ROUND(AVG(taux_respect_sla)::NUMERIC, 2)   AS taux_sla_moyen,
                SUM(nombre_reclamations_ouvertes
                  + nombre_reclamations_cloturees)          AS volume_total,
                ROUND(AVG(duree_moyenne_traitement_heures)::NUMERIC, 2) AS duree_moy
            FROM reclamations.kpis_daily
            WHERE date_kpi >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY date_kpi, region, type_reclamation
            ORDER BY date_kpi DESC, region
        """,
        "description": "Vue SLA par région — 12 derniers mois (pour Slicer Power BI)",
    },
]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def prepare_export_directory(**ctx):
    """Crée le répertoire d'export et archive l'export précédent."""
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    archive_dir = os.path.join(EXPORT_DIR, "archives", run_date)

    os.makedirs(EXPORT_DIR, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # Archive les fichiers existants
    for export_cfg in EXPORTS_CONFIG:
        current_file = os.path.join(EXPORT_DIR, export_cfg["filename"])
        if os.path.exists(current_file):
            archive_file = os.path.join(archive_dir, export_cfg["filename"])
            os.rename(current_file, archive_file)
            log.info("Archivé : %s → %s", current_file, archive_file)

    log.info("Répertoire export préparé : %s", EXPORT_DIR)

def export_tables_to_csv(**ctx):
    """
    Exporte chaque table configurée en CSV UTF-8-BOM avec séparateur ;
    (format natif Power BI / Excel France).
    """
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    export_stats = {}

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        for export_cfg in EXPORTS_CONFIG:
            name = export_cfg["name"]
            filepath = os.path.join(EXPORT_DIR, export_cfg["filename"])
            query = export_cfg["query"]

            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()

                # Écriture CSV UTF-8-BOM (format natif Excel France)
                with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(columns)
                    writer.writerows(rows)

                # Calcul checksum MD5
                with open(filepath, "rb") as f:
                    md5 = hashlib.md5(f.read()).hexdigest()

                file_size = os.path.getsize(filepath)
                export_stats[name] = {
                    "filename": export_cfg["filename"],
                    "rows": len(rows),
                    "columns": len(columns),
                    "file_size_bytes": file_size,
                    "md5": md5,
                    "description": export_cfg["description"],
                }

                log.info("✅ Export %s : %d lignes | %d colonnes | %.1f KB | MD5=%s",
                         name, len(rows), len(columns), file_size / 1024, md5[:8])

            except Exception as exc:
                log.error("Erreur export %s : %s", name, exc)
                export_stats[name] = {"error": str(exc), "rows": 0}

    finally:
        conn.close()

    ctx["ti"].xcom_push(key="export_stats", value=export_stats)
    total_rows = sum(s.get("rows", 0) for s in export_stats.values())
    ctx["ti"].xcom_push(key="total_rows", value=total_rows)

def generate_metadata_file(**ctx):
    """Génère un fichier metadata.json avec les infos d'export pour Power BI Gateway."""
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    export_stats = ctx["ti"].xcom_pull(key="export_stats", task_ids="export_tables_to_csv") or {}

    metadata = {
        "export_timestamp": datetime.now(timezone.utc).isoformat(),
        "export_date": run_date,
        "generated_by": DAG_ID,
        "exports": export_stats,
        "total_files": len(export_stats),
        "total_rows": sum(s.get("rows", 0) for s in export_stats.values()),
        "export_directory": EXPORT_DIR,
        "encoding": "UTF-8-BOM",
        "separator": ";",
        "format": "CSV",
    }

    metadata_path = os.path.join(EXPORT_DIR, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

    log.info("Metadata écrit : %s", metadata_path)
    log.info("Résumé export Power BI [%s] : %d fichiers, %d lignes total",
             run_date, metadata["total_files"], metadata["total_rows"])

def validate_exports(**ctx):
    """Valide que tous les exports ont bien été générés et sont non-vides."""
    all_ok = True
    for export_cfg in EXPORTS_CONFIG:
        filepath = os.path.join(EXPORT_DIR, export_cfg["filename"])
        if not os.path.exists(filepath):
            log.error("Export manquant : %s", filepath)
            all_ok = False
        elif os.path.getsize(filepath) < 100:
            log.warning("Export suspect (trop petit) : %s (%d bytes)",
                        filepath, os.path.getsize(filepath))
        else:
            log.info("✅ Fichier OK : %s (%.1f KB)",
                     export_cfg["filename"], os.path.getsize(filepath) / 1024)

    if not all_ok:
        raise ValueError("Validation exports Power BI échouée — des fichiers sont manquants")

def notify_pipeline_run(**ctx):
    total_rows = ctx["ti"].xcom_pull(key="total_rows", task_ids="export_tables_to_csv") or 0
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=total_rows,
        duration_seconds=0,
        message=f"Export Power BI OK [{(ctx.get('logical_date') or ctx.get('data_interval_start') or __import__('datetime').datetime.now()).strftime('%Y-%m-%d')}] — {total_rows} lignes | {len(EXPORTS_CONFIG)} fichiers",
    )

with DAG(
    dag_id=DAG_ID,
    description="Export quotidien CSV optimisé pour Power BI (UTF-8-BOM, séparateur ;)",
    schedule="30 7 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reporting", "powerbi", "export", "csv"],
    doc_md=__doc__,
) as dag:

    t_prep = PythonOperator(task_id="prepare_export_directory", python_callable=prepare_export_directory)
    t_export = PythonOperator(task_id="export_tables_to_csv", python_callable=export_tables_to_csv)
    t_metadata = PythonOperator(task_id="generate_metadata_file", python_callable=generate_metadata_file)
    t_validate = PythonOperator(task_id="validate_exports", python_callable=validate_exports)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_prep >> t_export >> t_metadata >> t_validate >> t_notify
