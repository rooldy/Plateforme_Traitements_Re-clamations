"""
pilotage_interventions_terrain_daily.py
Pilotage et dispatching des interventions techniciens terrain.

Enedis dispose de milliers de techniciens répartis sur le territoire.
Ce DAG optimise leur affectation en croisant :
  - Les réclamations non planifiées par zone géographique
  - La disponibilité des techniciens par secteur
  - L'urgence SLA de chaque réclamation
  - Les incidents systémiques en cours (priorité maximale)

Tables produites :
  - reclamations.interventions_terrain     : planning techniciens
  - reclamations.charge_techniciens        : charge par technicien/zone
  - reclamations.reclamations_non_planifiees : file d'attente dispatching

Schedule : quotidien 05:30
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "pilotage_interventions_terrain_daily"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_intervention_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        CREATE TABLE IF NOT EXISTS reclamations.interventions_terrain (
            intervention_id     VARCHAR(50) PRIMARY KEY,
            reclamation_id      VARCHAR(50),
            incident_id         VARCHAR(50),
            technicien_id       VARCHAR(50),
            zone_intervention   VARCHAR(100),
            departement         VARCHAR(10),
            type_intervention   VARCHAR(100),
            priorite            INTEGER,           -- 1=critique, 5=faible
            statut              VARCHAR(50) DEFAULT 'PLANIFIEE',
            date_planifiee      DATE,
            heure_planifiee     TIME,
            date_realisation    TIMESTAMP WITH TIME ZONE,
            duree_estimee_h     NUMERIC(4,1),
            duree_reelle_h      NUMERIC(4,1),
            sla_deadline        TIMESTAMP WITH TIME ZONE,
            heures_avant_sla    NUMERIC(8,2),
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.charge_techniciens (
            id                  SERIAL PRIMARY KEY,
            date_calcul         DATE NOT NULL,
            technicien_id       VARCHAR(50),
            zone               VARCHAR(100),
            departement         VARCHAR(10),
            nb_interventions_planifiees INTEGER DEFAULT 0,
            nb_interventions_realisees  INTEGER DEFAULT 0,
            nb_interventions_urgentes   INTEGER DEFAULT 0,
            charge_heures              NUMERIC(6,2),
            taux_charge_pct            NUMERIC(5,2),
            disponible                 BOOLEAN DEFAULT TRUE,
            created_at                 TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (date_calcul, technicien_id)
        );

        CREATE TABLE IF NOT EXISTS reclamations.reclamations_non_planifiees (
            reclamation_id      VARCHAR(50) PRIMARY KEY,
            type_reclamation    VARCHAR(100),
            region              VARCHAR(100),
            departement         VARCHAR(10),
            heures_avant_sla    NUMERIC(8,2),
            priorite_calculee   INTEGER,
            date_identification DATE,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_interventions_date   ON reclamations.interventions_terrain(date_planifiee);
        CREATE INDEX IF NOT EXISTS idx_interventions_tech   ON reclamations.interventions_terrain(technicien_id);
        CREATE INDEX IF NOT EXISTS idx_charge_date          ON reclamations.charge_techniciens(date_calcul);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

def identify_unplanned_interventions(**ctx):
    """Identifie les réclamations nécessitant une intervention terrain non encore planifiée."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    unplanned = 0

    try:
        with conn.cursor() as cur:
            # Réclamations terrain sans intervention planifiée et approchant le SLA
            cur.execute("""
                INSERT INTO reclamations.reclamations_non_planifiees
                    (reclamation_id, type_reclamation, region, departement,
                     heures_avant_sla, priorite_calculee, date_identification)
                SELECT
                    rp.reclamation_id,
                    rp.type_reclamation,
                    rp.region,
                    rp.departement,
                    -- Heures restantes avant SLA
                    CASE rp.type_reclamation
                        WHEN 'COUPURE_ELECTRIQUE'    THEN 48  - COALESCE(rp.duree_heures, 0)
                        WHEN 'RACCORDEMENT_RESEAU'   THEN 360 - COALESCE(rp.duree_heures, 0)
                        WHEN 'INTERVENTION_TECHNIQUE'THEN 168 - COALESCE(rp.duree_heures, 0)
                        WHEN 'COMPTEUR_LINKY'        THEN 120 - COALESCE(rp.duree_heures, 0)
                        ELSE 240 - COALESCE(rp.duree_heures, 0)
                    END AS heures_avant_sla,
                    -- Priorité : 1 = urgentissime, 5 = faible
                    CASE
                        WHEN rp.type_reclamation = 'COUPURE_ELECTRIQUE' THEN 1
                        WHEN rp.type_reclamation = 'RACCORDEMENT_RESEAU'
                         AND (360 - COALESCE(rp.duree_heures, 0)) < 72 THEN 2
                        WHEN rp.type_reclamation = 'INTERVENTION_TECHNIQUE' THEN 3
                        WHEN rp.type_reclamation = 'COMPTEUR_LINKY' THEN 4
                        ELSE 5
                    END AS priorite_calculee,
                    %s::DATE
                FROM reclamations.reclamations_processed rp
                LEFT JOIN reclamations.interventions_terrain it
                    ON rp.reclamation_id = it.reclamation_id
                    AND it.statut NOT IN ('ANNULEE', 'REALISEE')
                WHERE rp.statut IN ('OUVERT', 'EN_COURS')
                  AND rp.type_reclamation IN (
                      'COUPURE_ELECTRIQUE', 'RACCORDEMENT_RESEAU',
                      'INTERVENTION_TECHNIQUE', 'COMPTEUR_LINKY'
                  )
                  AND it.intervention_id IS NULL  -- pas encore planifié
                ON CONFLICT (reclamation_id) DO UPDATE SET
                    heures_avant_sla  = EXCLUDED.heures_avant_sla,
                    priorite_calculee = EXCLUDED.priorite_calculee
            """, (run_date,))
            unplanned = cur.rowcount

        conn.commit()
        log.info("Réclamations non planifiées identifiées : %d", unplanned)
        ctx["ti"].xcom_push(key="unplanned", value=unplanned)

    finally:
        conn.close()

def compute_technician_load(**ctx):
    """Calcule la charge de chaque technicien pour optimiser le dispatching."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            # Agrégation depuis les interventions planifiées existantes
            cur.execute("""
                INSERT INTO reclamations.charge_techniciens
                    (date_calcul, technicien_id, zone, departement,
                     nb_interventions_planifiees, nb_interventions_urgentes,
                     charge_heures, taux_charge_pct)
                SELECT
                    %s::DATE,
                    technicien_id,
                    zone_intervention,
                    departement,
                    COUNT(*) FILTER (WHERE statut = 'PLANIFIEE') AS nb_planifiees,
                    COUNT(*) FILTER (WHERE priorite <= 2)         AS nb_urgentes,
                    SUM(COALESCE(duree_estimee_h, 2.0))           AS charge_heures,
                    -- Taux de charge : 8h = 100% d'une journée
                    ROUND(100.0 * SUM(COALESCE(duree_estimee_h, 2.0)) / 8.0, 2)
                FROM reclamations.interventions_terrain
                WHERE date_planifiee = %s
                  AND technicien_id IS NOT NULL
                GROUP BY technicien_id, zone_intervention, departement
                ON CONFLICT (date_calcul, technicien_id) DO UPDATE SET
                    nb_interventions_planifiees = EXCLUDED.nb_interventions_planifiees,
                    charge_heures               = EXCLUDED.charge_heures,
                    taux_charge_pct             = EXCLUDED.taux_charge_pct,
                    disponible                  = (EXCLUDED.taux_charge_pct < 100)
            """, (run_date, run_date))

        conn.commit()
        log.info("Charge techniciens calculée pour %s", run_date)

    finally:
        conn.close()

def generate_dispatching_report(**ctx):
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT departement, COUNT(*), MIN(heures_avant_sla),
                       SUM(CASE WHEN priorite_calculee = 1 THEN 1 ELSE 0 END) AS urgents
                FROM reclamations.reclamations_non_planifiees
                WHERE date_identification = %s
                GROUP BY departement ORDER BY urgents DESC, MIN(heures_avant_sla) ASC
            """, (run_date,))
            rows = cur.fetchall()

        log.info("=== Rapport Dispatching Terrain [%s] ===", run_date)
        for dept, total, min_sla, urgents in rows:
            log.info("  Dept %-5s : %3d non planifiées | %2d urgentes | SLA min=%.1fh",
                     dept, total, urgents, min_sla or 0)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    unplanned = ctx["ti"].xcom_pull(key="unplanned", task_ids="identify_unplanned_interventions") or 0
    log_pipeline_run(DAG_ID, "WARNING" if unplanned > 50 else "SUCCESS", unplanned, 0,
                     f"Dispatching terrain [{ctx['ds']}] — {unplanned} réclamations sans intervention planifiée")

with DAG(
    dag_id=DAG_ID,
    description="Pilotage et dispatching des interventions techniciens terrain",
    schedule="30 5 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["terrain", "techniciens", "dispatching", "opérationnel"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_intervention_tables",         python_callable=create_intervention_tables)
    t2 = PythonOperator(task_id="identify_unplanned_interventions",   python_callable=identify_unplanned_interventions)
    t3 = PythonOperator(task_id="compute_technician_load",            python_callable=compute_technician_load)
    t4 = PythonOperator(task_id="generate_dispatching_report",        python_callable=generate_dispatching_report)
    t5 = PythonOperator(task_id="notify_pipeline_run",                python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t2 >> t3 >> t4 >> t5
