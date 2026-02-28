"""
pipeline_health_monitoring_hourly.py
Monitoring de la santé des pipelines Airflow — exécution horaire.

Responsabilités :
  1. Interroge pipeline_runs pour détecter les pipelines en échec ou lents
  2. Vérifie que chaque pipeline critique a bien tourné dans les dernières 26h
  3. Calcule un score de santé global des pipelines
  4. Enregistre les résultats dans data_quality_metrics
  5. Émet des alertes pour les pipelines manquants ou en erreur

Schedule : toutes les heures
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "pipeline_health_monitoring_hourly"

# Pipelines critiques à surveiller et leur fréquence maximale d'absence (heures)
PIPELINE_SCHEDULES = {
    "medallion_pipeline_daily":                  26,
    "reclamation_pipeline":                      26,
    "reclamations_linky_pipeline_daily":         26,
    "data_quality_checks_daily":                 26,
    "kpi_quotidiens_aggregation":                26,
    "sla_compliance_checks_daily":               26,
    "data_cleanup_weekly":                      200,   # hebdomadaire
    "reclamations_coupures_pipeline_daily":      26,
    "reclamations_facturation_pipeline_daily":   26,
    "reclamations_raccordement_pipeline_daily":  26,
    "reclamations_global_consolidation_daily":   26,
    "kpi_hebdomadaires_aggregation":            200,
    "kpi_mensuels_aggregation":                 750,
    "export_powerbi_daily":                      26,
    "backup_databases_daily":                    26,
    "refresh_materialized_views_daily":          26,
}

# Seuil durée anormalement longue (secondes)
SLOW_PIPELINE_THRESHOLD_S = {
    "medallion_pipeline_daily":               1800,   # 30 min
    "reclamation_pipeline":                   1800,
    "reclamations_linky_pipeline_daily":      1200,
    "kpi_quotidiens_aggregation":              600,
    "sla_compliance_checks_daily":             300,
    "reclamations_coupures_pipeline_daily":    900,
    "reclamations_facturation_pipeline_daily": 900,
    "reclamations_raccordement_pipeline_daily":900,
    "reclamations_global_consolidation_daily": 600,
    "export_powerbi_daily":                    600,
    "backup_databases_daily":                  900,
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def check_pipeline_runs(**ctx):
    """
    Vérifie dans pipeline_runs que chaque pipeline critique a bien tourné
    récemment et n'est pas en état d'erreur.
    """
    now = datetime.now(timezone.utc)
    missing = []
    failed = []
    slow = []
    ok = []

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for dag_id, max_absence_h in PIPELINE_SCHEDULES.items():
                # Dernier run connu pour ce DAG
                cur.execute("""
                    SELECT status, duration_seconds, rows_processed, created_at
                    FROM reclamations.pipeline_runs
                    WHERE dag_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (dag_id,))
                row = cur.fetchone()

                if row is None:
                    missing.append(dag_id)
                    log.warning("Pipeline ABSENT de pipeline_runs : %s", dag_id)
                    continue

                status, duration_s, rows, created_at = row

                # Normalise timezone
                if hasattr(created_at, "tzinfo") and created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)

                age_h = (now - created_at).total_seconds() / 3600

                # Vérifie si le pipeline est trop ancien
                if age_h > max_absence_h:
                    missing.append(dag_id)
                    log.warning("Pipeline EN RETARD (%.1fh > %dh) : %s", age_h, max_absence_h, dag_id)
                    continue

                # Vérifie le statut
                if status in ("FAILED", "ERROR", "ALERT"):
                    failed.append(dag_id)
                    log.error("Pipeline EN ÉCHEC : %s (status=%s)", dag_id, status)
                    continue

                # Vérifie la durée
                slow_threshold = SLOW_PIPELINE_THRESHOLD_S.get(dag_id)
                if slow_threshold and duration_s and duration_s > slow_threshold:
                    slow.append((dag_id, duration_s, slow_threshold))
                    log.warning("Pipeline LENT : %s (%.0fs > seuil %ds)", dag_id, duration_s, slow_threshold)

                ok.append(dag_id)
                log.info("✅ %s — status=%s | dur=%.0fs | rows=%d | age=%.1fh",
                         dag_id, status, duration_s or 0, rows or 0, age_h)

    finally:
        conn.close()

    ctx["ti"].xcom_push(key="missing", value=missing)
    ctx["ti"].xcom_push(key="failed", value=failed)
    ctx["ti"].xcom_push(key="slow", value=[s[0] for s in slow])
    ctx["ti"].xcom_push(key="ok", value=ok)

    log.info("Résumé santé pipelines — OK: %d | Manquants: %d | Échoués: %d | Lents: %d",
             len(ok), len(missing), len(failed), len(slow))


def compute_health_score(**ctx):
    """Calcule le score de santé global (0-100)."""
    ok = ctx["ti"].xcom_pull(key="ok", task_ids="check_pipeline_runs") or []
    missing = ctx["ti"].xcom_pull(key="missing", task_ids="check_pipeline_runs") or []
    failed = ctx["ti"].xcom_pull(key="failed", task_ids="check_pipeline_runs") or []
    slow = ctx["ti"].xcom_pull(key="slow", task_ids="check_pipeline_runs") or []

    total = len(PIPELINE_SCHEDULES)
    # Score : pipeline OK=1pt, slow=0.7pt, missing=0pt, failed=-1pt
    score_raw = (
        len(ok) * 1.0 +
        len(slow) * 0.7 +
        len(missing) * 0.0 +
        len(failed) * (-0.5)
    )
    score = max(0, min(100, round(100 * score_raw / max(total, 1), 1)))

    log.info("Score santé pipelines : %.1f/100", score)

    if score < 70:
        log.error("🚨 Score santé CRITIQUE : %.1f%%", score)
    elif score < 90:
        log.warning("⚠️ Score santé DÉGRADÉ : %.1f%%", score)
    else:
        log.info("✅ Score santé BON : %.1f%%", score)

    ctx["ti"].xcom_push(key="health_score", value=score)


def record_health_metrics(**ctx):
    """Enregistre le score de santé dans data_quality_metrics."""
    score = ctx["ti"].xcom_pull(key="health_score", task_ids="compute_health_score") or 0
    missing = ctx["ti"].xcom_pull(key="missing", task_ids="check_pipeline_runs") or []
    failed = ctx["ti"].xcom_pull(key="failed", task_ids="check_pipeline_runs") or []
    slow = ctx["ti"].xcom_pull(key="slow", task_ids="check_pipeline_runs") or []

    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
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
                "pipeline_health",
                run_date,
                "health_score",
                score,
                "OK" if score >= 90 else ("WARNING" if score >= 70 else "CRITICAL"),
                f"Score={score:.1f}% | Manquants={missing} | Échoués={failed} | Lents={slow}",
                datetime.now(timezone.utc),
            ))
        conn.commit()
    finally:
        conn.close()


def notify_pipeline_run(**ctx):
    score = ctx["ti"].xcom_pull(key="health_score", task_ids="compute_health_score") or 0
    missing = ctx["ti"].xcom_pull(key="missing", task_ids="check_pipeline_runs") or []
    failed = ctx["ti"].xcom_pull(key="failed", task_ids="check_pipeline_runs") or []

    log_pipeline_run(
        dag_id=DAG_ID,
        status="OK" if score >= 90 else ("WARNING" if score >= 70 else "CRITICAL"),
        rows_processed=len(PIPELINE_SCHEDULES),
        duration_seconds=0,
        message=f"Santé pipelines {score:.1f}% | manquants={len(missing)} | échoués={len(failed)} | {ctx['ts']}",
    )


with DAG(
    dag_id=DAG_ID,
    description="Monitoring horaire de la santé et disponibilité des pipelines",
    schedule="30 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["monitoring", "santé", "pipelines", "horaire"],
    doc_md=__doc__,
) as dag:

    t_check = PythonOperator(task_id="check_pipeline_runs", python_callable=check_pipeline_runs)
    t_score = PythonOperator(task_id="compute_health_score", python_callable=compute_health_score)
    t_record = PythonOperator(task_id="record_health_metrics", python_callable=record_health_metrics)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_check >> t_score >> t_record >> t_notify
