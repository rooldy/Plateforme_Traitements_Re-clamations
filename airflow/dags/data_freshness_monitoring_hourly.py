"""
data_freshness_monitoring_hourly.py
Monitoring de la fraîcheur des données — exécution horaire.

Responsabilités :
  1. Vérifie l'âge des données dans chaque table de production
  2. Détecte les tables non mises à jour depuis plus du seuil configuré
  3. Calcule un score de fraîcheur global (0-100)
  4. Logue les alertes dans pipeline_runs et dans data_quality_metrics
  5. Émet des warnings si des tables critiques sont en retard

Schedule : toutes les heures (*  * * * *)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "data_freshness_monitoring_hourly"

# Seuils de fraîcheur par table (en heures)
FRESHNESS_THRESHOLDS = {
    "reclamations.reclamations_processed":          26,   # quotidien → alerte si >26h
    "reclamations.reclamations_cleaned":            26,
    "reclamations.kpis_daily":                      26,
    "reclamations.reclamations_linky_detailed":     26,
    "reclamations.data_quality_metrics":            26,
    "reclamations.reclamations_coupures_detail":    26,
    "reclamations.reclamations_facturation_detail": 26,
    "reclamations.reclamations_raccordement_detail":26,
    "reclamations.reclamations_global_consolidated":28,   # consolidation en dernier
    "reclamations.pipeline_runs":                    2,   # doit être mis à jour toutes les 2h
}

# Tables critiques : une alerte WARNING même pour un léger retard
CRITICAL_TABLES = {
    "reclamations.reclamations_processed",
    "reclamations.kpis_daily",
    "reclamations.pipeline_runs",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,  # horaire : pas d'email sur chaque échec
}


def check_table_freshness(**ctx):
    """
    Vérifie la fraîcheur de chaque table en interrogeant la colonne created_at / export_date.
    Retourne un dict {table: age_heures}.
    """
    now = datetime.now(timezone.utc)
    results = {}
    alerts = []

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for table, threshold_h in FRESHNESS_THRESHOLDS.items():
                schema, table_name = table.split(".")
                try:
                    # Détecte la colonne de timestamp la plus récente disponible
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                          AND column_name IN ('created_at', 'export_date', 'run_date')
                        ORDER BY CASE column_name
                            WHEN 'created_at'  THEN 1
                            WHEN 'export_date' THEN 2
                            WHEN 'run_date'    THEN 3
                        END
                        LIMIT 1
                    """, (schema, table_name))
                    col_row = cur.fetchone()

                    if not col_row:
                        log.warning("Table %s : aucune colonne temporelle trouvée", table)
                        results[table] = None
                        continue

                    ts_col = col_row[0]
                    cur.execute(f"SELECT MAX({ts_col}) FROM {table}")
                    latest = cur.fetchone()[0]

                    if latest is None:
                        age_h = float("inf")
                        log.warning("Table %s : VIDE (aucune donnée)", table)
                    else:
                        # Normalise en datetime UTC
                        if hasattr(latest, "tzinfo") and latest.tzinfo is None:
                            from datetime import timezone as tz
                            latest = latest.replace(tzinfo=tz.utc)
                        elif not hasattr(latest, "tzinfo"):
                            # date object → datetime
                            latest = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
                        age_h = (now - latest).total_seconds() / 3600

                    results[table] = round(age_h, 2)

                    if age_h > threshold_h:
                        level = "CRITIQUE" if table in CRITICAL_TABLES else "ATTENTION"
                        msg = f"[{level}] {table} — âge={age_h:.1f}h > seuil={threshold_h}h"
                        alerts.append(msg)
                        log.warning(msg)
                    else:
                        log.info("✅ %s — âge=%.1fh (seuil=%dh)", table, age_h, threshold_h)

                except Exception as exc:
                    log.error("Erreur vérification fraîcheur %s : %s", table, exc)
                    results[table] = None

    finally:
        conn.close()

    ctx["ti"].xcom_push(key="freshness_results", value=results)
    ctx["ti"].xcom_push(key="alerts", value=alerts)
    return results


def compute_freshness_score(**ctx):
    """Calcule un score de fraîcheur global (0-100) et l'enregistre."""
    results = ctx["ti"].xcom_pull(key="freshness_results", task_ids="check_table_freshness") or {}

    if not results:
        ctx["ti"].xcom_push(key="freshness_score", value=0)
        return

    total = len(results)
    ok = sum(
        1 for table, age_h in results.items()
        if age_h is not None and age_h <= FRESHNESS_THRESHOLDS.get(table, 26)
    )
    score = round(100 * ok / total, 1) if total > 0 else 0

    log.info("Score fraîcheur global : %.1f%% (%d/%d tables OK)", score, ok, total)
    ctx["ti"].xcom_push(key="freshness_score", value=score)

    if score < 80:
        log.warning("⚠️ Score fraîcheur FAIBLE : %.1f%%", score)
    elif score < 95:
        log.warning("Score fraîcheur acceptable : %.1f%%", score)
    else:
        log.info("Score fraîcheur excellent : %.1f%%", score)


def update_data_quality_metrics(**ctx):
    """Enregistre le score de fraîcheur dans data_quality_metrics."""
    score = ctx["ti"].xcom_pull(key="freshness_score", task_ids="compute_freshness_score") or 0
    alerts = ctx["ti"].xcom_pull(key="alerts", task_ids="check_table_freshness") or []
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Upsert dans data_quality_metrics
            cur.execute("""
                INSERT INTO reclamations.data_quality_metrics
                    (table_name, check_date, metric_name, metric_value, status, message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (table_name, check_date, metric_name) DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    status       = EXCLUDED.status,
                    message      = EXCLUDED.message,
                    created_at   = EXCLUDED.created_at
            """, (
                "ALL_TABLES",
                run_date,
                "freshness_score",
                score,
                "OK" if score >= 95 else ("WARNING" if score >= 80 else "ALERT"),
                f"Score fraîcheur global : {score}%. Alertes : {len(alerts)}. " +
                ("; ".join(alerts[:5]) if alerts else "Aucune alerte"),
                datetime.now(timezone.utc),
            ))
        conn.commit()
        log.info("Score fraîcheur enregistré dans data_quality_metrics : %.1f%%", score)
    finally:
        conn.close()


def notify_pipeline_run(**ctx):
    score = ctx["ti"].xcom_pull(key="freshness_score", task_ids="compute_freshness_score") or 0
    alerts = ctx["ti"].xcom_pull(key="alerts", task_ids="check_table_freshness") or []
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS" if score >= 95 else ("WARNING" if score >= 80 else "ALERT"),
        rows_processed=len(FRESHNESS_THRESHOLDS),
        duration_seconds=0,
        message=f"Fraîcheur {score:.1f}% | {len(alerts)} alertes | {ctx['ts']}",
    )


with DAG(
    dag_id=DAG_ID,
    description="Monitoring horaire de la fraîcheur des données des tables de production",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["monitoring", "qualité", "fraîcheur", "horaire"],
    doc_md=__doc__,
) as dag:

    t_freshness = PythonOperator(task_id="check_table_freshness", python_callable=check_table_freshness)
    t_score = PythonOperator(task_id="compute_freshness_score", python_callable=compute_freshness_score)
    t_metrics = PythonOperator(task_id="update_data_quality_metrics", python_callable=update_data_quality_metrics)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_freshness >> t_score >> t_metrics >> t_notify
