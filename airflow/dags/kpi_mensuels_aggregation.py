"""
kpi_mensuels_aggregation.py
Agrégation des KPIs mensuels — 1er de chaque mois à 07:00.

Responsabilités :
  1. Agrège les kpis_hebdomadaires du mois écoulé
  2. Calcule les métriques mensuelles : tendances long terme, benchmarks annuels
  3. Exporte vers reclamations.kpis_mensuels
  4. Génère le rapport mensuel de conformité SLA pour le management
  5. Log dans pipeline_runs

Schedule : 1er de chaque mois à 07:00
"""

import logging
from datetime import datetime, timezone, timedelta
from calendar import monthrange

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SLA_HEURES, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "kpi_mensuels_aggregation"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,
}

def create_target_table(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;
        CREATE TABLE IF NOT EXISTS reclamations.kpis_mensuels (
            id                              SERIAL PRIMARY KEY,
            annee                           INTEGER NOT NULL,
            mois                            INTEGER NOT NULL,
            debut_mois                      DATE NOT NULL,
            fin_mois                        DATE NOT NULL,
            region                          VARCHAR(100),
            type_reclamation                VARCHAR(100),
            total_reclamations              INTEGER,
            total_ouvertes_fin_mois         INTEGER,
            total_cloturees_mois            INTEGER,
            duree_moyenne_heures            NUMERIC(10, 2),
            duree_mediane_heures            NUMERIC(10, 2),
            taux_respect_sla                NUMERIC(5, 2),
            taux_critique                   NUMERIC(5, 2),
            taux_escalade                   NUMERIC(5, 2),
            evolution_vs_mois_precedent     NUMERIC(8, 2),
            evolution_vs_meme_mois_an_dernier NUMERIC(8, 2),
            sla_cible                       INTEGER,
            created_at                      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (annee, mois, region, type_reclamation)
        );
        CREATE INDEX IF NOT EXISTS idx_kpi_mensuel_annee  ON reclamations.kpis_mensuels(annee, mois);
        CREATE INDEX IF NOT EXISTS idx_kpi_mensuel_region ON reclamations.kpis_mensuels(region);
        CREATE INDEX IF NOT EXISTS idx_kpi_mensuel_type   ON reclamations.kpis_mensuels(type_reclamation);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.kpis_mensuels prête")
    finally:
        conn.close()

def aggregate_monthly_kpis(**ctx):
    """Agrège les KPIs quotidiens sur le mois précédent avec comparaisons temporelles."""
    run_date = ctx["ds"]
    run_dt = datetime.strptime(run_date, "%Y-%m-%d")

    # Mois précédent
    if run_dt.month == 1:
        annee_mois = run_dt.year - 1
        mois = 12
    else:
        annee_mois = run_dt.year
        mois = run_dt.month - 1

    _, last_day = monthrange(annee_mois, mois)
    debut_mois = datetime(annee_mois, mois, 1).date()
    fin_mois = datetime(annee_mois, mois, last_day).date()

    # Même mois l'an dernier
    debut_mois_an_dernier = datetime(annee_mois - 1, mois, 1).date()
    _, last_day_an = monthrange(annee_mois - 1, mois)
    fin_mois_an_dernier = datetime(annee_mois - 1, mois, last_day_an).date()

    # Mois précédent-précédent pour comparaison M-1
    if mois == 1:
        annee_m1 = annee_mois - 1
        mois_m1 = 12
    else:
        annee_m1 = annee_mois
        mois_m1 = mois - 1
    _, last_day_m1 = monthrange(annee_m1, mois_m1)
    debut_mois_m1 = datetime(annee_m1, mois_m1, 1).date()
    fin_mois_m1 = datetime(annee_m1, mois_m1, last_day_m1).date()

    log.info("Agrégation mensuelle %04d-%02d : %s → %s", annee_mois, mois, debut_mois, fin_mois)

    conn = psycopg2.connect(**DB_CONFIG)
    total_rows = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM reclamations.kpis_mensuels
                WHERE annee = %s AND mois = %s
            """, (annee_mois, mois))

            cur.execute("""
                WITH mois_courant AS (
                    SELECT
                        region,
                        type_reclamation,
                        SUM(nombre_reclamations_ouvertes)   AS total_ouvertes,
                        SUM(nombre_reclamations_cloturees)  AS total_cloturees,
                        SUM(nombre_reclamations_ouvertes + nombre_reclamations_cloturees) AS total,
                        AVG(duree_moyenne_traitement_heures)    AS duree_moyenne,
                        AVG(duree_mediane_traitement_heures)    AS duree_mediane,
                        AVG(taux_respect_sla)                   AS taux_sla,
                        AVG(taux_reclamations_critiques)        AS taux_critique,
                        AVG(taux_escalade)                      AS taux_escalade
                    FROM reclamations.kpis_daily
                    WHERE date_kpi BETWEEN %s AND %s
                    GROUP BY region, type_reclamation
                ),
                mois_prec AS (
                    SELECT region, type_reclamation,
                           SUM(nombre_reclamations_ouvertes + nombre_reclamations_cloturees) AS total
                    FROM reclamations.kpis_daily
                    WHERE date_kpi BETWEEN %s AND %s
                    GROUP BY region, type_reclamation
                ),
                meme_mois_an AS (
                    SELECT region, type_reclamation,
                           SUM(nombre_reclamations_ouvertes + nombre_reclamations_cloturees) AS total
                    FROM reclamations.kpis_daily
                    WHERE date_kpi BETWEEN %s AND %s
                    GROUP BY region, type_reclamation
                )
                INSERT INTO reclamations.kpis_mensuels
                    (annee, mois, debut_mois, fin_mois, region, type_reclamation,
                     total_reclamations, total_ouvertes_fin_mois, total_cloturees_mois,
                     duree_moyenne_heures, duree_mediane_heures, taux_respect_sla,
                     taux_critique, taux_escalade, evolution_vs_mois_precedent,
                     evolution_vs_meme_mois_an_dernier, sla_cible)
                SELECT
                    %s, %s, %s, %s,
                    mc.region,
                    mc.type_reclamation,
                    mc.total,
                    mc.total_ouvertes,
                    mc.total_cloturees,
                    ROUND(mc.duree_moyenne::NUMERIC, 2),
                    ROUND(mc.duree_mediane::NUMERIC, 2),
                    ROUND(mc.taux_sla::NUMERIC, 2),
                    ROUND(mc.taux_critique::NUMERIC, 2),
                    ROUND(mc.taux_escalade::NUMERIC, 2),
                    ROUND(CASE WHEN mp.total > 0
                          THEN 100.0 * (mc.total - mp.total) / mp.total
                          ELSE NULL END::NUMERIC, 2),
                    ROUND(CASE WHEN ma.total > 0
                          THEN 100.0 * (mc.total - ma.total) / ma.total
                          ELSE NULL END::NUMERIC, 2),
                    CASE mc.type_reclamation
                        WHEN 'COUPURE_ELECTRIQUE'    THEN 48
                        WHEN 'COMPTEUR_LINKY'        THEN 120
                        WHEN 'FACTURATION'           THEN 240
                        WHEN 'RACCORDEMENT_RESEAU'   THEN 360
                        WHEN 'INTERVENTION_TECHNIQUE' THEN 168
                        ELSE NULL
                    END
                FROM mois_courant mc
                LEFT JOIN mois_prec  mp USING (region, type_reclamation)
                LEFT JOIN meme_mois_an ma USING (region, type_reclamation)
                ON CONFLICT (annee, mois, region, type_reclamation) DO UPDATE SET
                    total_reclamations              = EXCLUDED.total_reclamations,
                    taux_respect_sla                = EXCLUDED.taux_respect_sla,
                    evolution_vs_mois_precedent     = EXCLUDED.evolution_vs_mois_precedent,
                    evolution_vs_meme_mois_an_dernier = EXCLUDED.evolution_vs_meme_mois_an_dernier
            """, (
                debut_mois, fin_mois,
                debut_mois_m1, fin_mois_m1,
                debut_mois_an_dernier, fin_mois_an_dernier,
                annee_mois, mois, debut_mois, fin_mois,
            ))

            total_rows = cur.rowcount
        conn.commit()
        log.info("KPIs mensuels %04d-%02d : %d lignes", annee_mois, mois, total_rows)
        ctx["ti"].xcom_push(key="rows_inserted", value=total_rows)
        ctx["ti"].xcom_push(key="mois_label", value=f"{annee_mois:04d}-{mois:02d}")

    finally:
        conn.close()

def generate_sla_compliance_report(**ctx):
    """
    Génère le rapport mensuel de conformité SLA par type et région.
    Ce rapport est destiné au management.
    """
    mois_label = ctx["ti"].xcom_pull(key="mois_label", task_ids="aggregate_monthly_kpis") or "?"
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    type_reclamation,
                    sla_cible,
                    SUM(total_reclamations)                  AS total,
                    ROUND(AVG(taux_respect_sla)::NUMERIC, 1) AS taux_sla_moy,
                    MIN(taux_respect_sla)                    AS taux_sla_min,
                    ROUND(AVG(evolution_vs_mois_precedent)::NUMERIC, 1) AS evol_m1
                FROM reclamations.kpis_mensuels
                WHERE (annee || '-' || LPAD(mois::TEXT, 2, '0')) = %s
                GROUP BY type_reclamation, sla_cible
                ORDER BY taux_sla_moy ASC
            """, (mois_label,))
            rows = cur.fetchall()

        log.info("=== Rapport SLA Mensuel %s ===", mois_label)
        log.info("  %-30s %6s %8s %10s %10s %10s",
                 "Type", "SLA(h)", "Total", "Taux SLA%", "Min SLA%", "Évol M-1%")
        log.info("  " + "-" * 80)
        for type_rec, sla, total, taux_moy, taux_min, evol in rows:
            emoji = "✅" if (taux_moy or 0) >= 90 else "⚠️" if (taux_moy or 0) >= 75 else "🚨"
            log.info("  %s %-28s %6s %8d %9.1f%% %9.1f%% %9.1f%%",
                     emoji, type_rec, str(sla or "?"), total or 0,
                     taux_moy or 0, taux_min or 0, evol or 0)

    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    rows = ctx["ti"].xcom_pull(key="rows_inserted", task_ids="aggregate_monthly_kpis") or 0
    label = ctx["ti"].xcom_pull(key="mois_label", task_ids="aggregate_monthly_kpis") or "?"
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=rows,
        duration_seconds=0,
        message=f"KPIs mensuels {label} OK — {rows} lignes",
    )

with DAG(
    dag_id=DAG_ID,
    description="Agrégation mensuelle des KPIs réclamations (1er du mois, 07:00)",
    schedule="0 7 1 * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reporting", "kpi", "mensuel", "management"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_target_table", python_callable=create_target_table)
    t_aggregate = PythonOperator(task_id="aggregate_monthly_kpis", python_callable=aggregate_monthly_kpis)
    t_report = PythonOperator(task_id="generate_sla_compliance_report", python_callable=generate_sla_compliance_report)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_aggregate >> t_report >> t_notify
