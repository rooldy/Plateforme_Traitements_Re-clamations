"""
anomaly_detection_daily.py
Détection d'anomalies sur les données de réclamations — quotidien.

Responsabilités :
  1. Détecte les anomalies statistiques sur les volumes (pics/creux inhabituels)
  2. Identifie les réclamations avec des durées aberrantes (outliers IQR)
  3. Détecte les doublons potentiels (même client, même type, même jour)
  4. Repère les distributions géographiques anormales
  5. Enregistre les anomalies dans reclamations.anomalies_detected
  6. Log dans pipeline_runs

Schedule : quotidien 08:00 (après la consolidation globale)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "anomaly_detection_daily"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def create_anomalies_table(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;
        CREATE TABLE IF NOT EXISTS reclamations.anomalies_detected (
            id                  SERIAL PRIMARY KEY,
            detection_date      DATE NOT NULL,
            anomaly_type        VARCHAR(100) NOT NULL,
            severity            VARCHAR(20)  NOT NULL,   -- LOW, MEDIUM, HIGH, CRITICAL
            table_name          VARCHAR(100),
            field_name          VARCHAR(100),
            description         TEXT,
            sample_values       TEXT,
            count_affected      INTEGER DEFAULT 0,
            is_resolved         BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_anomalies_date     ON reclamations.anomalies_detected(detection_date);
        CREATE INDEX IF NOT EXISTS idx_anomalies_type     ON reclamations.anomalies_detected(anomaly_type);
        CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON reclamations.anomalies_detected(severity);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.anomalies_detected prête")
    finally:
        conn.close()

def _insert_anomaly(cur, detection_date, anomaly_type, severity, table_name,
                    field_name, description, sample_values, count_affected):
    cur.execute("""
        INSERT INTO reclamations.anomalies_detected
            (detection_date, anomaly_type, severity, table_name, field_name,
             description, sample_values, count_affected)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (detection_date, anomaly_type, severity, table_name, field_name,
          description, sample_values, count_affected))

def detect_volume_anomalies(**ctx):
    """
    Détecte les pics/creux de volume en comparant le jour courant
    à la moyenne mobile des 7 derniers jours ± 2 écarts-types.
    """
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    anomalies_found = 0
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            # Calcul de la moyenne et écart-type sur les 7 derniers jours
            cur.execute("""
                WITH daily_counts AS (
                    SELECT
                        date_kpi,
                        type_reclamation,
                        SUM(nombre_reclamations_ouvertes + nombre_reclamations_cloturees) AS total
                    FROM reclamations.kpis_daily
                    WHERE date_kpi BETWEEN %s::DATE - INTERVAL '8 days'
                                       AND %s::DATE - INTERVAL '1 day'
                    GROUP BY date_kpi, type_reclamation
                ),
                stats AS (
                    SELECT
                        type_reclamation,
                        AVG(total)    AS avg_total,
                        STDDEV(total) AS std_total
                    FROM daily_counts
                    GROUP BY type_reclamation
                ),
                today AS (
                    SELECT
                        type_reclamation,
                        SUM(nombre_reclamations_ouvertes + nombre_reclamations_cloturees) AS total_today
                    FROM reclamations.kpis_daily
                    WHERE date_kpi = %s
                    GROUP BY type_reclamation
                )
                SELECT
                    t.type_reclamation,
                    t.total_today,
                    s.avg_total,
                    s.std_total,
                    ABS(t.total_today - s.avg_total) / NULLIF(s.std_total, 0) AS z_score
                FROM today t
                JOIN stats s USING (type_reclamation)
                WHERE ABS(t.total_today - s.avg_total) > 2 * NULLIF(s.std_total, 0)
            """, (run_date, run_date, run_date))

            volume_anomalies = cur.fetchall()

            for type_rec, total_today, avg, std, z_score in volume_anomalies:
                severity = "HIGH" if z_score > 3 else "MEDIUM"
                direction = "PIC" if total_today > avg else "CREUX"
                desc = (f"Volume {direction} pour {type_rec} : {total_today} réclamations "
                        f"(moyenne 7j = {avg:.0f}, z-score = {z_score:.1f})")
                log.warning(desc)
                _insert_anomaly(
                    cur, run_date, "VOLUME_ANOMALY", severity,
                    "reclamations.kpis_daily", "nombre_reclamations",
                    desc, f"total={total_today}, avg={avg:.0f}, std={std:.0f}", int(total_today)
                )
                anomalies_found += 1

        conn.commit()
        log.info("Anomalies de volume détectées : %d", anomalies_found)
        ctx["ti"].xcom_push(key="volume_anomalies", value=anomalies_found)

    finally:
        conn.close()

def detect_duration_outliers(**ctx):
    """
    Détecte les réclamations avec des durées aberrantes (méthode IQR).
    Outlier si durée > Q3 + 3*IQR ou durée négative.
    """
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    anomalies_found = 0
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH stats AS (
                    SELECT
                        type_reclamation,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY duree_traitement_heures) AS q1,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duree_traitement_heures) AS q3
                    FROM reclamations.reclamations_processed
                    WHERE date_creation >= NOW() - INTERVAL '30 days'
                      AND duree_traitement_heures IS NOT NULL
                    GROUP BY type_reclamation
                )
                SELECT
                    rp.type_reclamation,
                    COUNT(*)                                            AS outlier_count,
                    MIN(rp.duree_traitement_heures)                     AS min_dur,
                    MAX(rp.duree_traitement_heures)                     AS max_dur,
                    s.q3 + 3 * (s.q3 - s.q1)                           AS upper_bound
                FROM reclamations.reclamations_processed rp
                JOIN stats s USING (type_reclamation)
                WHERE DATE(rp.date_creation) = %s
                  AND (
                    rp.duree_traitement_heures > s.q3 + 3 * (s.q3 - s.q1)
                    OR rp.duree_traitement_heures < 0
                  )
                GROUP BY rp.type_reclamation, s.q3, s.q1
                HAVING COUNT(*) > 0
            """, (run_date,))

            outliers = cur.fetchall()

            for type_rec, count, min_dur, max_dur, upper in outliers:
                severity = "HIGH" if count > 10 else "MEDIUM"
                desc = (f"Durées aberrantes pour {type_rec} : {count} réclamations "
                        f"avec durée hors borne (borne={upper:.0f}h, "
                        f"min={min_dur:.1f}h, max={max_dur:.1f}h)")
                log.warning(desc)
                _insert_anomaly(
                    cur, run_date, "DURATION_OUTLIER", severity,
                    "reclamations.reclamations_processed", "duree_traitement_heures",
                    desc, f"count={count}, min={min_dur:.1f}h, max={max_dur:.1f}h", count
                )
                anomalies_found += 1

            # Détection durées négatives (données corrompues)
            cur.execute("""
                SELECT COUNT(*), type_reclamation
                FROM reclamations.reclamations_processed
                WHERE DATE(date_creation) = %s
                  AND duree_traitement_heures < 0
                GROUP BY type_reclamation
            """, (run_date,))
            neg_rows = cur.fetchall()
            for count, type_rec in neg_rows:
                desc = f"Durées NÉGATIVES pour {type_rec} : {count} enregistrements"
                log.error(desc)
                _insert_anomaly(
                    cur, run_date, "NEGATIVE_DURATION", "CRITICAL",
                    "reclamations.reclamations_processed", "duree_traitement_heures",
                    desc, None, count
                )
                anomalies_found += 1

        conn.commit()
        log.info("Outliers de durée détectés : %d anomalies", anomalies_found)
        ctx["ti"].xcom_push(key="duration_anomalies", value=anomalies_found)

    finally:
        conn.close()

def detect_duplicates(**ctx):
    """Détecte les doublons potentiels (même client, même type, même jour)."""
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    anomalies_found = 0
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    client_id,
                    type_reclamation,
                    DATE(date_creation)  AS creation_day,
                    COUNT(*)             AS nb_reclamations
                FROM reclamations.reclamations_processed
                WHERE DATE(date_creation) = %s
                GROUP BY client_id, type_reclamation, DATE(date_creation)
                HAVING COUNT(*) > 1
            """, (run_date,))

            duplicates = cur.fetchall()
            total_dup = sum(row[3] for row in duplicates)

            if duplicates:
                severity = "HIGH" if len(duplicates) > 50 else "MEDIUM"
                desc = (f"{len(duplicates)} groupes de doublons potentiels détectés "
                        f"({total_dup} réclamations concernées)")
                sample = str([(r[0], r[1], r[3]) for r in duplicates[:3]])
                log.warning(desc)
                _insert_anomaly(
                    cur, run_date, "POTENTIAL_DUPLICATES", severity,
                    "reclamations.reclamations_processed", "client_id + type_reclamation",
                    desc, sample[:500], total_dup
                )
                anomalies_found += 1

        conn.commit()
        log.info("Doublons potentiels : %d groupes (%d réclamations)", len(duplicates), total_dup)
        ctx["ti"].xcom_push(key="duplicate_anomalies", value=anomalies_found)

    finally:
        conn.close()

def detect_geographic_anomalies(**ctx):
    """Détecte les déséquilibres géographiques anormaux (régions sans données)."""
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    anomalies_found = 0
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            # Régions actives les 7 derniers jours mais absentes aujourd'hui
            cur.execute("""
                WITH regions_habituelles AS (
                    SELECT DISTINCT region
                    FROM reclamations.kpis_daily
                    WHERE date_kpi BETWEEN %s::DATE - INTERVAL '7 days'
                                       AND %s::DATE - INTERVAL '1 day'
                ),
                regions_today AS (
                    SELECT DISTINCT region
                    FROM reclamations.kpis_daily
                    WHERE date_kpi = %s
                )
                SELECT r.region
                FROM regions_habituelles r
                LEFT JOIN regions_today t USING (region)
                WHERE t.region IS NULL
            """, (run_date, run_date, run_date))

            missing_regions = [row[0] for row in cur.fetchall()]

            if missing_regions:
                desc = f"Régions sans données aujourd'hui (actives cette semaine) : {missing_regions}"
                log.warning(desc)
                _insert_anomaly(
                    cur, run_date, "MISSING_REGION_DATA", "MEDIUM",
                    "reclamations.kpis_daily", "region",
                    desc, str(missing_regions), len(missing_regions)
                )
                anomalies_found += 1

        conn.commit()
        log.info("Anomalies géographiques détectées : %d", anomalies_found)
        ctx["ti"].xcom_push(key="geographic_anomalies", value=anomalies_found)

    finally:
        conn.close()

def generate_anomaly_report(**ctx):
    """Génère le rapport de synthèse des anomalies du jour."""
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    anomaly_type,
                    severity,
                    COUNT(*)          AS count,
                    SUM(count_affected) AS total_affected
                FROM reclamations.anomalies_detected
                WHERE detection_date = %s AND NOT is_resolved
                GROUP BY anomaly_type, severity
                ORDER BY CASE severity
                    WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM'   THEN 3 WHEN 'LOW'  THEN 4
                END
            """, (run_date,))
            rows = cur.fetchall()

        total_anomalies = sum(r[2] for r in rows)
        log.info("=== Rapport Anomalies [%s] — %d anomalies total ===", run_date, total_anomalies)
        for anomaly_type, severity, count, affected in rows:
            log.info("  [%s] %s : %d occurrence(s), %d éléments affectés",
                     severity, anomaly_type, count, affected or 0)

        ctx["ti"].xcom_push(key="total_anomalies", value=total_anomalies)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    vol = ctx["ti"].xcom_pull(key="volume_anomalies", task_ids="detect_volume_anomalies") or 0
    dur = ctx["ti"].xcom_pull(key="duration_anomalies", task_ids="detect_duration_outliers") or 0
    dup = ctx["ti"].xcom_pull(key="duplicate_anomalies", task_ids="detect_duplicates") or 0
    geo = ctx["ti"].xcom_pull(key="geographic_anomalies", task_ids="detect_geographic_anomalies") or 0
    total = vol + dur + dup + geo

    log_pipeline_run(
        dag_id=DAG_ID,
        status="WARNING" if total > 0 else "SUCCESS",
        rows_processed=total,
        duration_seconds=0,
        message=(f"Détection anomalies [{(ctx.get('logical_date') or ctx.get('data_interval_start') or __import__('datetime').datetime.now()).strftime('%Y-%m-%d')}] : {total} total — "
                 f"volumes={vol}, durées={dur}, doublons={dup}, géo={geo}"),
    )

with DAG(
    dag_id=DAG_ID,
    description="Détection d'anomalies statistiques sur les données de réclamations",
    schedule="0 8 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["qualité", "anomalies", "monitoring"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_anomalies_table", python_callable=create_anomalies_table)
    t_volume = PythonOperator(task_id="detect_volume_anomalies", python_callable=detect_volume_anomalies)
    t_duration = PythonOperator(task_id="detect_duration_outliers", python_callable=detect_duration_outliers)
    t_duplicates = PythonOperator(task_id="detect_duplicates", python_callable=detect_duplicates)
    t_geo = PythonOperator(task_id="detect_geographic_anomalies", python_callable=detect_geographic_anomalies)
    t_report = PythonOperator(task_id="generate_anomaly_report", python_callable=generate_anomaly_report)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    (
    t_create
        >> [t_volume, t_duration, t_duplicates, t_geo]
        >> t_report
        >> t_notify
    )
