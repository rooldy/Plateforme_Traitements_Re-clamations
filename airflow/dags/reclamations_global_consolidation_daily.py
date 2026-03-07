"""
reclamations_global_consolidation_daily.py
Consolidation globale de toutes les sources de réclamations.

Responsabilités :
  1. Attend la fin de tous les pipelines métier (reclamation, coupures, facturation, raccordement, linky)
  2. Consolide toutes les tables de détail en une vue unifiée enrichie
  3. Calcule des indicateurs cross-types (comparaisons, tendances, benchmarks)
  4. Exporte vers reclamations.reclamations_global_consolidated
  5. Met à jour les KPIs consolidés dans kpis_daily
  6. Log dans pipeline_runs

Schedule : quotidien 07:00 (après tous les pipelines métier)
"""

import logging
import io
import csv as csv_mod
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SLA_HEURES, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "reclamations_global_consolidation_daily"

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
        CREATE TABLE IF NOT EXISTS reclamations.reclamations_global_consolidated (
            reclamation_id          VARCHAR(50)  NOT NULL,
            client_id               VARCHAR(50),
            region                  VARCHAR(100),
            departement             VARCHAR(10),
            type_reclamation        VARCHAR(100),
            statut                  VARCHAR(50),
            priorite                VARCHAR(20),
            date_creation           TIMESTAMP WITH TIME ZONE,
            date_cloture            TIMESTAMP WITH TIME ZONE,
            duree_heures            NUMERIC(10, 2),
            sla_heures              INTEGER,
            sla_respect             BOOLEAN,
            risque_depassement      BOOLEAN,
            heures_restantes        NUMERIC(10, 2),
            source_pipeline         VARCHAR(100),
            export_date             DATE,
            created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (reclamation_id, export_date)
        );
        CREATE INDEX IF NOT EXISTS idx_global_type    ON reclamations.reclamations_global_consolidated(type_reclamation);
        CREATE INDEX IF NOT EXISTS idx_global_region  ON reclamations.reclamations_global_consolidated(region);
        CREATE INDEX IF NOT EXISTS idx_global_sla     ON reclamations.reclamations_global_consolidated(sla_respect);
        CREATE INDEX IF NOT EXISTS idx_global_date    ON reclamations.reclamations_global_consolidated(export_date);
        CREATE INDEX IF NOT EXISTS idx_global_statut  ON reclamations.reclamations_global_consolidated(statut);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.reclamations_global_consolidated prête")
    finally:
        conn.close()

def consolidate_all_sources(**ctx):
    """
    Consolide toutes les sources de réclamations via SQL (plus efficace que Spark
    pour une jointure de tables PostgreSQL déjà chargées).
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    total_rows = 0

    try:
        with conn.cursor() as cur:
            # Supprime les données existantes pour la date
            cur.execute(
                "DELETE FROM reclamations.reclamations_global_consolidated WHERE export_date = %s",
                (run_date,)
            )

            # Source 1 : reclamations_processed (toutes les réclamations de base)
            cur.execute("""
                INSERT INTO reclamations.reclamations_global_consolidated
                    (reclamation_id, client_id, region, departement, type_reclamation,
                     statut, priorite, date_creation, date_cloture,
                     duree_heures, sla_heures, sla_respect, risque_depassement,
                     heures_restantes, source_pipeline, export_date)
                SELECT
                    rp.reclamation_id,
                    rp.client_id,
                    rp.region,
                    rp.departement,
                    rp.type_reclamation,
                    rp.statut,
                    rp.priorite,
                    rp.date_creation,
                    rp.date_cloture,
                    rp.duree_traitement_heures                    AS duree_heures,
                    CASE rp.type_reclamation
                        WHEN 'COUPURE_ELECTRIQUE'   THEN 48
                        WHEN 'COMPTEUR_LINKY'       THEN 120
                        WHEN 'FACTURATION'          THEN 240
                        WHEN 'RACCORDEMENT_RESEAU'  THEN 360
                        WHEN 'INTERVENTION_TECHNIQUE' THEN 168
                        ELSE NULL
                    END                                           AS sla_heures,
                    -- SLA respect : calculé si clôturée
                    CASE
                        WHEN rp.date_cloture IS NOT NULL THEN
                            rp.duree_traitement_heures <= CASE rp.type_reclamation
                                WHEN 'COUPURE_ELECTRIQUE'   THEN 48
                                WHEN 'COMPTEUR_LINKY'       THEN 120
                                WHEN 'FACTURATION'          THEN 240
                                WHEN 'RACCORDEMENT_RESEAU'  THEN 360
                                WHEN 'INTERVENTION_TECHNIQUE' THEN 168
                                ELSE 9999
                            END
                        ELSE NULL
                    END                                           AS sla_respect,
                    FALSE                                         AS risque_depassement,
                    NULL::NUMERIC                                 AS heures_restantes,
                    'reclamation_pipeline'                        AS source_pipeline,
                    %s::DATE                                      AS export_date
                FROM reclamations.reclamations_processed rp
                ON CONFLICT (reclamation_id, export_date) DO UPDATE SET
                    statut              = EXCLUDED.statut,
                    date_cloture        = EXCLUDED.date_cloture,
                    duree_heures        = EXCLUDED.duree_heures,
                    sla_respect         = EXCLUDED.sla_respect,
                    source_pipeline     = EXCLUDED.source_pipeline
            """, (run_date,))
            rows_base = cur.rowcount
            log.info("Source reclamations_processed : %d lignes insérées/mises à jour", rows_base)

            # Source 2 : enrichissement depuis reclamations_coupures_detail
            # (met à jour les colonnes SLA calculées par le pipeline spécialisé)
            try:
                cur.execute("""
                    UPDATE reclamations.reclamations_global_consolidated g
                    SET
                        sla_respect        = c.sla_respect,
                        risque_depassement = c.risque_depassement,
                        heures_restantes   = c.heures_restantes,
                        source_pipeline    = 'reclamations_coupures_pipeline_daily'
                    FROM reclamations.reclamations_coupures_detail c
                    WHERE g.reclamation_id = c.reclamation_id
                      AND g.export_date    = %s
                      AND c.export_date    = %s
                """, (run_date, run_date))
                log.info("Enrichissement coupures : %d lignes mises à jour", cur.rowcount)
            except Exception as exc:
                log.warning("Table coupures non disponible (%s) — skip enrichissement", exc)

            # Source 3 : enrichissement depuis reclamations_facturation_detail
            try:
                cur.execute("""
                    UPDATE reclamations.reclamations_global_consolidated g
                    SET
                        sla_respect        = f.sla_respect,
                        risque_depassement = f.risque_depassement,
                        heures_restantes   = f.heures_restantes,
                        source_pipeline    = 'reclamations_facturation_pipeline_daily'
                    FROM reclamations.reclamations_facturation_detail f
                    WHERE g.reclamation_id = f.reclamation_id
                      AND g.export_date    = %s
                      AND f.export_date    = %s
                """, (run_date, run_date))
                log.info("Enrichissement facturation : %d lignes mises à jour", cur.rowcount)
            except Exception as exc:
                log.warning("Table facturation non disponible (%s) — skip enrichissement", exc)

            # Source 4 : enrichissement depuis reclamations_raccordement_detail
            try:
                cur.execute("""
                    UPDATE reclamations.reclamations_global_consolidated g
                    SET
                        sla_respect        = r.sla_respect,
                        risque_depassement = r.risque_depassement,
                        heures_restantes   = r.heures_restantes,
                        source_pipeline    = 'reclamations_raccordement_pipeline_daily'
                    FROM reclamations.reclamations_raccordement_detail r
                    WHERE g.reclamation_id = r.reclamation_id
                      AND g.export_date    = %s
                      AND r.export_date    = %s
                """, (run_date, run_date))
                log.info("Enrichissement raccordement : %d lignes mises à jour", cur.rowcount)
            except Exception as exc:
                log.warning("Table raccordement non disponible (%s) — skip enrichissement", exc)

            # Comptage total consolidé
            cur.execute(
                "SELECT COUNT(*) FROM reclamations.reclamations_global_consolidated WHERE export_date = %s",
                (run_date,)
            )
            total_rows = cur.fetchone()[0]

        conn.commit()
        log.info("Consolidation globale terminée : %d réclamations pour %s", total_rows, run_date)
        ctx["ti"].xcom_push(key="total_rows", value=total_rows)

    finally:
        conn.close()

def compute_cross_type_kpis(**ctx):
    """Calcule des KPIs cross-types et les insère dans kpis_daily (upsert)."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            # Calcul des KPIs agrégés par région et type depuis la vue consolidée
            cur.execute("""
                INSERT INTO reclamations.kpis_daily
                    (region, type_reclamation, date_kpi,
                     nombre_reclamations_ouvertes, nombre_reclamations_cloturees,
                     duree_moyenne_traitement_heures, taux_respect_sla,
                     taux_reclamations_critiques, source)
                SELECT
                    COALESCE(region, 'INCONNUE')                                AS region,
                    type_reclamation,
                    %s::DATE                                                     AS date_kpi,
                    COUNT(*) FILTER (WHERE statut IN ('OUVERT', 'EN_COURS'))    AS nombre_ouvertes,
                    COUNT(*) FILTER (WHERE statut = 'CLOTURE')                  AS nombre_cloturees,
                    ROUND(AVG(duree_heures)::NUMERIC, 2)                        AS duree_moyenne,
                    ROUND(
                        100.0 * COUNT(*) FILTER (WHERE sla_respect = TRUE)
                        / NULLIF(COUNT(*) FILTER (WHERE sla_respect IS NOT NULL), 0),
                        2
                    )                                                            AS taux_sla,
                    ROUND(
                        100.0 * COUNT(*) FILTER (WHERE priorite = 'CRITIQUE')
                        / NULLIF(COUNT(*), 0),
                        2
                    )                                                            AS taux_critique,
                    'reclamations_global_consolidation_daily'                   AS source
                FROM reclamations.reclamations_global_consolidated
                WHERE export_date = %s
                GROUP BY region, type_reclamation
                ON CONFLICT (region, type_reclamation, date_kpi) DO UPDATE SET
                    nombre_reclamations_ouvertes       = EXCLUDED.nombre_reclamations_ouvertes,
                    nombre_reclamations_cloturees      = EXCLUDED.nombre_reclamations_cloturees,
                    duree_moyenne_traitement_heures    = EXCLUDED.duree_moyenne_traitement_heures,
                    taux_respect_sla                   = EXCLUDED.taux_respect_sla,
                    taux_reclamations_critiques        = EXCLUDED.taux_reclamations_critiques,
                    source                             = EXCLUDED.source
            """, (run_date, run_date))

            kpis_inserted = cur.rowcount
            log.info("KPIs cross-types upsert : %d lignes dans kpis_daily", kpis_inserted)

        conn.commit()
    except Exception as exc:
        log.warning("Impossible de calculer les KPIs cross-types (%s)", exc)
        conn.rollback()
    finally:
        conn.close()

def generate_consolidation_summary(**ctx):
    """Produit un résumé de la consolidation pour validation visuelle dans les logs."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    type_reclamation,
                    COUNT(*)                                                      AS total,
                    COUNT(*) FILTER (WHERE statut IN ('OUVERT', 'EN_COURS'))     AS en_cours,
                    COUNT(*) FILTER (WHERE sla_respect = FALSE
                                      AND date_cloture IS NOT NULL)               AS hors_sla,
                    COUNT(*) FILTER (WHERE risque_depassement)                    AS en_risque,
                    ROUND(AVG(duree_heures)::NUMERIC, 1)                          AS duree_moy_h
                FROM reclamations.reclamations_global_consolidated
                WHERE export_date = %s
                GROUP BY type_reclamation
                ORDER BY total DESC
            """, (ctx["ds"],))
            rows = cur.fetchall()

        log.info("=== Résumé Consolidation Globale [%s] ===", ctx["ds"])
        log.info("  %-30s %8s %8s %8s %8s %10s",
                 "Type", "Total", "En cours", "Hors SLA", "En risque", "Durée moy")
        for type_rec, total, en_cours, hors_sla, en_risque, duree_moy in rows:
            log.info("  %-30s %8d %8d %8d %8d %9.1fh",
                     type_rec, total, en_cours, hors_sla, en_risque, duree_moy or 0)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    total_rows = ctx["ti"].xcom_pull(key="total_rows", task_ids="consolidate_all_sources") or 0
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=total_rows,
        duration_seconds=0,
        message=f"Consolidation globale OK — {total_rows} réclamations consolidées [{ctx['ds']}]",
    )

with DAG(
    dag_id=DAG_ID,
    description="Consolidation globale cross-types de toutes les réclamations",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reclamations", "métier", "consolidation", "cross-type"],
    doc_md=__doc__,
) as dag:

    # Attend les deux pipelines fondateurs
    t_create = PythonOperator(task_id="create_target_table", python_callable=create_target_table)
    t_consolidate = PythonOperator(task_id="consolidate_all_sources", python_callable=consolidate_all_sources)
    t_kpis = PythonOperator(task_id="compute_cross_type_kpis", python_callable=compute_cross_type_kpis)
    t_summary = PythonOperator(task_id="generate_consolidation_summary", python_callable=generate_consolidation_summary)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_consolidate >> t_kpis >> t_summary >> t_notify
