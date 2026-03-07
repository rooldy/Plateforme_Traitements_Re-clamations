"""
refresh_materialized_views_daily.py
Rafraîchissement quotidien des vues matérialisées PostgreSQL.

Responsabilités :
  1. Crée les vues matérialisées si elles n'existent pas encore
  2. Rafraîchit toutes les vues matérialisées dans le bon ordre (dépendances)
  3. Vérifie la cohérence des données après refresh
  4. Mesure les temps de rafraîchissement
  5. Log dans pipeline_runs

Schedule : quotidien à 07:45 (après export Power BI)

Vues matérialisées gérées :
  - mv_sla_summary          : synthèse SLA par région et type (pour dashboards)
  - mv_reclamations_actives : réclamations ouvertes enrichies (pour alertes)
  - mv_kpi_tendances        : tendances KPIs sur 30j (pour Power BI)
  - mv_top_problemes_linky  : top pannes Linky par région
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "refresh_materialized_views_daily"

# Définition des vues matérialisées dans l'ordre de dépendance
MATERIALIZED_VIEWS = [
    {
        "name": "reclamations.mv_sla_summary",
        "description": "Synthèse SLA par région et type (30 derniers jours)",
        "ddl": """
            CREATE MATERIALIZED VIEW IF NOT EXISTS reclamations.mv_sla_summary AS
            SELECT
                region,
                type_reclamation,
                COUNT(*)                                                    AS total_reclamations,
                COUNT(*) FILTER (WHERE statut IN ('OUVERT', 'EN_COURS'))    AS en_cours,
                ROUND(AVG(taux_respect_sla)::NUMERIC, 2)                   AS taux_sla_moyen,
                ROUND(AVG(duree_moyenne_traitement_heures)::NUMERIC, 2)    AS duree_moyenne_h,
                MAX(date_kpi)                                               AS derniere_date
            FROM reclamations.kpis_daily
            WHERE date_kpi >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY region, type_reclamation
            WITH DATA;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sla_region_type
            ON reclamations.mv_sla_summary (region, type_reclamation);
        """,
        "refresh": "REFRESH MATERIALIZED VIEW CONCURRENTLY reclamations.mv_sla_summary",
        "fallback_refresh": "REFRESH MATERIALIZED VIEW reclamations.mv_sla_summary",
        "validation_query": "SELECT COUNT(*) FROM reclamations.mv_sla_summary",
        "min_rows": 1,
    },
    {
        "name": "reclamations.mv_reclamations_actives",
        "description": "Réclamations actuellement ouvertes avec calcul de risque",
        "ddl": """
            CREATE MATERIALIZED VIEW IF NOT EXISTS reclamations.mv_reclamations_actives AS
            SELECT
                reclamation_id,
                client_id,
                region,
                type_reclamation,
                priorite,
                date_creation,
                duree_heures,
                sla_heures,
                risque_depassement,
                heures_restantes,
                CASE
                    WHEN risque_depassement THEN 'RISQUE_IMMINENT'
                    WHEN duree_heures > sla_heures THEN 'HORS_SLA'
                    WHEN heures_restantes < sla_heures * 0.2 THEN 'EN_VIGILANCE'
                    ELSE 'NOMINAL'
                END AS niveau_alerte
            FROM reclamations.reclamations_global_consolidated
            WHERE statut IN ('OUVERT', 'EN_COURS')
              AND export_date = (
                SELECT MAX(export_date) FROM reclamations.reclamations_global_consolidated
              )
            WITH DATA;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_actives_id
            ON reclamations.mv_reclamations_actives (reclamation_id);
            CREATE INDEX IF NOT EXISTS idx_mv_actives_region
            ON reclamations.mv_reclamations_actives (region);
            CREATE INDEX IF NOT EXISTS idx_mv_actives_alerte
            ON reclamations.mv_reclamations_actives (niveau_alerte);
        """,
        "refresh": "REFRESH MATERIALIZED VIEW CONCURRENTLY reclamations.mv_reclamations_actives",
        "fallback_refresh": "REFRESH MATERIALIZED VIEW reclamations.mv_reclamations_actives",
        "validation_query": "SELECT COUNT(*) FROM reclamations.mv_reclamations_actives",
        "min_rows": 0,  # Peut être vide si toutes les réclamations sont clôturées
    },
    {
        "name": "reclamations.mv_kpi_tendances",
        "description": "Tendances KPIs sur 30 jours pour graphiques Power BI",
        "ddl": """
            CREATE MATERIALIZED VIEW IF NOT EXISTS reclamations.mv_kpi_tendances AS
            SELECT
                date_kpi,
                type_reclamation,
                SUM(nombre_reclamations_ouvertes + nombre_reclamations_cloturees) AS volume_total,
                ROUND(AVG(taux_respect_sla)::NUMERIC, 2)                         AS taux_sla,
                ROUND(AVG(duree_moyenne_traitement_heures)::NUMERIC, 2)          AS duree_moy,
                -- Moyenne mobile 7 jours
                ROUND(AVG(AVG(taux_respect_sla)::NUMERIC) OVER (
                    PARTITION BY type_reclamation
                    ORDER BY date_kpi
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2) AS taux_sla_mm7j
            FROM reclamations.kpis_daily
            WHERE date_kpi >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY date_kpi, type_reclamation
            WITH DATA;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_tendances_date_type
            ON reclamations.mv_kpi_tendances (date_kpi, type_reclamation);
        """,
        "refresh": "REFRESH MATERIALIZED VIEW CONCURRENTLY reclamations.mv_kpi_tendances",
        "fallback_refresh": "REFRESH MATERIALIZED VIEW reclamations.mv_kpi_tendances",
        "validation_query": "SELECT COUNT(*) FROM reclamations.mv_kpi_tendances",
        "min_rows": 1,
    },
    {
        "name": "reclamations.mv_top_problemes_linky",
        "description": "Top 10 pannes Linky par région (basé sur reclamations_linky_detailed)",
        "ddl": """
            CREATE MATERIALIZED VIEW IF NOT EXISTS reclamations.mv_top_problemes_linky AS
            SELECT
                region,
                type_panne,
                COUNT(*)                              AS occurrences,
                ROUND(AVG(duree_heures)::NUMERIC, 1)  AS duree_moyenne_h,
                MAX(export_date)                      AS derniere_observation
            FROM reclamations.reclamations_linky_detailed
            WHERE export_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY region, type_panne
            ORDER BY occurrences DESC
            WITH DATA;
        """,
        "refresh": "REFRESH MATERIALIZED VIEW reclamations.mv_top_problemes_linky",
        "fallback_refresh": "REFRESH MATERIALIZED VIEW reclamations.mv_top_problemes_linky",
        "validation_query": "SELECT COUNT(*) FROM reclamations.mv_top_problemes_linky",
        "min_rows": 0,
    },
]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def create_materialized_views(**ctx):
    """Crée les vues matérialisées si elles n'existent pas."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        for view in MATERIALIZED_VIEWS:
            try:
                with conn.cursor() as cur:
                    cur.execute(view["ddl"])
                conn.commit()
                log.info("Vue matérialisée OK : %s", view["name"])
            except Exception as exc:
                conn.rollback()
                log.warning("Vue %s déjà existante ou erreur DDL (%s) — tentative refresh direct",
                            view["name"], exc)
    finally:
        conn.close()

def refresh_all_views(**ctx):
    """Rafraîchit toutes les vues matérialisées avec mesure des temps."""
    refresh_stats = {}
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        # Niveau d'isolation AUTOCOMMIT requis pour REFRESH MATERIALIZED VIEW CONCURRENTLY
        conn.autocommit = True

        for view in MATERIALIZED_VIEWS:
            view_name = view["name"]
            start = datetime.now(timezone.utc)

            try:
                with conn.cursor() as cur:
                    cur.execute(view["refresh"])
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                log.info("✅ REFRESH %s : %.1fs", view_name, duration)
                refresh_stats[view_name] = {"status": "OK", "duration_s": round(duration, 2)}

            except Exception as exc:
                # Fallback : REFRESH sans CONCURRENTLY (bloque les lectures mais toujours possible)
                log.warning("CONCURRENTLY échoué pour %s (%s) — fallback sans CONCURRENTLY", view_name, exc)
                try:
                    with conn.cursor() as cur:
                        cur.execute(view["fallback_refresh"])
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    log.info("✅ REFRESH (fallback) %s : %.1fs", view_name, duration)
                    refresh_stats[view_name] = {"status": "OK_FALLBACK", "duration_s": round(duration, 2)}
                except Exception as exc2:
                    log.error("Erreur REFRESH %s : %s", view_name, exc2)
                    refresh_stats[view_name] = {"status": "ERROR", "error": str(exc2)}

    finally:
        conn.autocommit = False
        conn.close()

    ctx["ti"].xcom_push(key="refresh_stats", value=refresh_stats)
    errors = [v for v in refresh_stats.values() if v.get("status") == "ERROR"]
    if errors:
        raise RuntimeError(f"{len(errors)} vue(s) matérialisée(s) non rafraîchie(s)")

def validate_views(**ctx):
    """Valide que les vues matérialisées contiennent des données cohérentes."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        for view in MATERIALIZED_VIEWS:
            try:
                with conn.cursor() as cur:
                    cur.execute(view["validation_query"])
                    count = cur.fetchone()[0]

                if count < view["min_rows"]:
                    log.warning("Vue %s : %d lignes < seuil minimum %d",
                                view["name"], count, view["min_rows"])
                else:
                    log.info("✅ Validation %s : %d lignes", view["name"], count)
            except Exception as exc:
                log.error("Erreur validation vue %s : %s", view["name"], exc)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    refresh_stats = ctx["ti"].xcom_pull(key="refresh_stats", task_ids="refresh_all_views") or {}
    ok_count = sum(1 for v in refresh_stats.values() if "OK" in v.get("status", ""))
    total_duration = sum(v.get("duration_s", 0) for v in refresh_stats.values())

    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=len(MATERIALIZED_VIEWS),
        duration_seconds=total_duration,
        message=f"Refresh vues matérialisées [{ctx['ds']}] : {ok_count}/{len(MATERIALIZED_VIEWS)} OK | {total_duration:.1f}s total",
    )

with DAG(
    dag_id=DAG_ID,
    description="Rafraîchissement quotidien des vues matérialisées PostgreSQL",
    schedule="45 7 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "postgresql", "vues-materialisées"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_materialized_views", python_callable=create_materialized_views)
    t_refresh = PythonOperator(task_id="refresh_all_views", python_callable=refresh_all_views)
    t_validate = PythonOperator(task_id="validate_views", python_callable=validate_views)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_refresh >> t_validate >> t_notify
