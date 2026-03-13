"""
kpi_hebdomadaires_aggregation.py
Agrégation des KPIs hebdomadaires — exécution le lundi à 06:00.

Responsabilités :
  1. Agrège les kpis_daily de la semaine écoulée (lundi N-1 → dimanche N-1)
  2. Calcule des métriques hebdomadaires : tendances, évolutions, benchmarks
  3. Exporte vers reclamations.kpis_hebdomadaires
  4. Génère un rapport de synthèse hebdomadaire
  5. Log dans pipeline_runs

Schedule : chaque lundi à 06:00
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "kpi_hebdomadaires_aggregation"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_target_table(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;
        CREATE TABLE IF NOT EXISTS reclamations.kpis_hebdomadaires (
            id                              SERIAL PRIMARY KEY,
            annee                           INTEGER NOT NULL,
            semaine                         INTEGER NOT NULL,
            debut_semaine                   DATE NOT NULL,
            fin_semaine                     DATE NOT NULL,
            region                          VARCHAR(100),
            type_reclamation                VARCHAR(100),
            total_reclamations              INTEGER,
            total_ouvertes                  INTEGER,
            total_cloturees                 INTEGER,
            duree_moyenne_heures            NUMERIC(10, 2),
            duree_mediane_heures            NUMERIC(10, 2),
            taux_respect_sla                NUMERIC(5, 2),
            taux_critique                   NUMERIC(5, 2),
            evolution_vs_semaine_precedente NUMERIC(8, 2),   -- % évolution volume
            created_at                      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (annee, semaine, region, type_reclamation)
        );
        CREATE INDEX IF NOT EXISTS idx_kpi_hebdo_semaine ON reclamations.kpis_hebdomadaires(annee, semaine);
        CREATE INDEX IF NOT EXISTS idx_kpi_hebdo_region  ON reclamations.kpis_hebdomadaires(region);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.kpis_hebdomadaires prête")
    finally:
        conn.close()

def aggregate_weekly_kpis(**ctx):
    """Agrège les KPIs quotidiens en KPIs hebdomadaires avec calcul d'évolution."""
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    # La semaine analysée est la semaine précédente (lundi → dimanche)
    run_dt = datetime.strptime(run_date, "%Y-%m-%d")
    # Dimanche précédent = run_dt - 1 jour (car on tourne le lundi)
    fin_semaine = (run_dt - timedelta(days=1)).date()
    debut_semaine = (run_dt - timedelta(days=7)).date()
    annee = debut_semaine.isocalendar()[0]
    semaine = debut_semaine.isocalendar()[1]

    log.info("Agrégation semaine %d/%02d : %s → %s", annee, semaine, debut_semaine, fin_semaine)

    conn = psycopg2.connect(**DB_CONFIG)
    total_rows = 0
    try:
        with conn.cursor() as cur:
            # Supprime les données existantes pour cette semaine
            cur.execute("""
                DELETE FROM reclamations.kpis_hebdomadaires
                WHERE annee = %s AND semaine = %s
            """, (annee, semaine))

            # Agrégation depuis kpis_daily
            cur.execute("""
                WITH semaine_courante AS (
                    SELECT
                        region,
                        type_reclamation,
                        SUM(nombre_ouvertes)          AS total_ouvertes,
                        SUM(nombre_cloturees)         AS total_cloturees,
                        SUM(nombre_ouvertes + nombre_cloturees) AS total,
                        AVG(duree_moyenne_traitement)       AS duree_moyenne,
                        AVG(duree_mediane_traitement)       AS duree_mediane,
                        ROUND((1 - AVG(nb_critiques::NUMERIC / NULLIF(nombre_reclamations_total,0))) * 100, 2) AS taux_sla,
                        AVG(nb_critiques)           AS taux_critique
                    FROM reclamations.kpis_daily
                    WHERE date_calcul BETWEEN %s AND %s
                    GROUP BY region, type_reclamation
                ),
                semaine_precedente AS (
                    SELECT
                        region,
                        type_reclamation,
                        SUM(nombre_ouvertes + nombre_cloturees) AS total_prec
                    FROM reclamations.kpis_daily
                    WHERE date_calcul BETWEEN %s::DATE - INTERVAL '7 days' AND %s::DATE - INTERVAL '1 day'
                    GROUP BY region, type_reclamation
                )
                INSERT INTO reclamations.kpis_hebdomadaires
                    (annee, semaine, debut_semaine, fin_semaine, region, type_reclamation,
                     total_reclamations, total_ouvertes, total_cloturees,
                     duree_moyenne_heures, duree_mediane_heures,
                     taux_respect_sla, taux_critique, evolution_vs_semaine_precedente)
                SELECT
                    %s, %s, %s, %s,
                    sc.region,
                    sc.type_reclamation,
                    sc.total,
                    sc.total_ouvertes,
                    sc.total_cloturees,
                    ROUND(sc.duree_moyenne::NUMERIC, 2),
                    ROUND(sc.duree_mediane::NUMERIC, 2),
                    ROUND(sc.taux_sla::NUMERIC, 2),
                    ROUND(sc.taux_critique::NUMERIC, 2),
                    ROUND(
                        CASE
                            WHEN sp.total_prec > 0 THEN
                                100.0 * (sc.total - sp.total_prec) / sp.total_prec
                            ELSE NULL
                        END::NUMERIC, 2
                    ) AS evolution
                FROM semaine_courante sc
                LEFT JOIN semaine_precedente sp
                    ON sc.region = sp.region
                    AND sc.type_reclamation = sp.type_reclamation
                ON CONFLICT (annee, semaine, region, type_reclamation) DO UPDATE SET
                    total_reclamations              = EXCLUDED.total_reclamations,
                    total_ouvertes                  = EXCLUDED.total_ouvertes,
                    total_cloturees                 = EXCLUDED.total_cloturees,
                    duree_moyenne_heures            = EXCLUDED.duree_moyenne_heures,
                    taux_respect_sla                = EXCLUDED.taux_respect_sla,
                    evolution_vs_semaine_precedente = EXCLUDED.evolution_vs_semaine_precedente
            """, (
                debut_semaine, fin_semaine,
                debut_semaine, debut_semaine,
                annee, semaine, debut_semaine, fin_semaine,
            ))

            total_rows = cur.rowcount
        conn.commit()
        log.info("KPIs hebdomadaires S%02d/%d : %d lignes insérées", semaine, annee, total_rows)
        ctx["ti"].xcom_push(key="rows_inserted", value=total_rows)
        ctx["ti"].xcom_push(key="semaine_label", value=f"S{semaine:02d}/{annee}")

    finally:
        conn.close()

def generate_weekly_summary(**ctx):
    """Génère et log le rapport de synthèse de la semaine."""
    semaine_label = ctx["ti"].xcom_pull(key="semaine_label", task_ids="aggregate_weekly_kpis") or "?"
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    type_reclamation,
                    SUM(total_reclamations)   AS total,
                    ROUND(AVG(taux_respect_sla)::NUMERIC, 1) AS taux_sla_moy,
                    ROUND(AVG(evolution_vs_semaine_precedente)::NUMERIC, 1) AS evolution
                FROM reclamations.kpis_hebdomadaires
                WHERE (annee, semaine) = (
                    SELECT annee, semaine FROM reclamations.kpis_hebdomadaires
                    ORDER BY debut_semaine DESC LIMIT 1
                )
                GROUP BY type_reclamation
                ORDER BY total DESC
            """)
            rows = cur.fetchall()

        log.info("=== Rapport Hebdomadaire %s ===", semaine_label)
        log.info("  %-30s %10s %12s %12s", "Type", "Total", "Taux SLA%", "Évolution%")
        for type_rec, total, sla, evol in rows:
            log.info("  %-30s %10d %11.1f%% %11.1f%%",
                     type_rec, total or 0, sla or 0, evol or 0)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    rows = ctx["ti"].xcom_pull(key="rows_inserted", task_ids="aggregate_weekly_kpis") or 0
    label = ctx["ti"].xcom_pull(key="semaine_label", task_ids="aggregate_weekly_kpis") or "?"
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=rows,
        duration_seconds=0,
        message=f"KPIs hebdo {label} OK — {rows} lignes",
    )

with DAG(
    dag_id=DAG_ID,
    description="Agrégation hebdomadaire des KPIs réclamations (lundi 06:00)",
    schedule="0 6 * * 1",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reporting", "kpi", "hebdomadaire"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_target_table", python_callable=create_target_table)
    t_aggregate = PythonOperator(task_id="aggregate_weekly_kpis", python_callable=aggregate_weekly_kpis)
    t_summary = PythonOperator(task_id="generate_weekly_summary", python_callable=generate_weekly_summary)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_aggregate >> t_summary >> t_notify
