"""
reconciliation_multi_systemes_daily.py
Réconciliation des données entre les systèmes Enedis : CRM / OMEGA / SAP.

Enedis opère sur plusieurs SI qui ne se synchronisent pas toujours en temps réel :
  - CRM (Salesforce) : gestion client et réclamations
  - OMEGA            : gestion des interventions terrain et des ordres de travaux
  - SAP              : facturation, comptabilité et données financières

Des incohérences apparaissent régulièrement et créent des erreurs opérationnelles :
  - Réclamation "résolue" dans CRM mais technicien encore sur le terrain (OMEGA)
  - Facture corrigée dans SAP mais réclamation toujours ouverte dans CRM
  - Intervention OMEGA réalisée sans réclamation CRM associée

Tables produites :
  - reclamations.incoherences_systemes     : écarts détectés entre SI
  - reclamations.reconciliation_log        : journal des réconciliations

Schedule : quotidien 06:30 (après kpi_quotidiens)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "reconciliation_multi_systemes_daily"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_reconciliation_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        CREATE TABLE IF NOT EXISTS reclamations.incoherences_systemes (
            id                  SERIAL PRIMARY KEY,
            date_detection      DATE NOT NULL,
            type_incoherence    VARCHAR(100) NOT NULL,
            -- CRM_OMEGA_STATUT, SAP_CRM_FACTURE, OMEGA_SANS_CRM,
            -- DELAI_SYNC, DOUBLON_SYSTEME
            systeme_source      VARCHAR(50),
            systeme_cible       VARCHAR(50),
            reclamation_id      VARCHAR(50),
            client_id           VARCHAR(50),
            valeur_source       TEXT,
            valeur_cible        TEXT,
            ecart_description   TEXT,
            severite            VARCHAR(20) DEFAULT 'MODEREE',
            statut              VARCHAR(50) DEFAULT 'OUVERTE',
            date_resolution     DATE,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.reconciliation_log (
            id                  SERIAL PRIMARY KEY,
            date_run            DATE NOT NULL,
            type_controle       VARCHAR(100),
            nb_verifies         INTEGER,
            nb_incoherences     INTEGER,
            taux_coherence_pct  NUMERIC(5, 2),
            detail              TEXT,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_inco_date    ON reclamations.incoherences_systemes(date_detection);
        CREATE INDEX IF NOT EXISTS idx_inco_type    ON reclamations.incoherences_systemes(type_incoherence);
        CREATE INDEX IF NOT EXISTS idx_inco_statut  ON reclamations.incoherences_systemes(statut);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

def check_crm_omega_coherence(**ctx):
    """
    Vérifie la cohérence entre le statut CRM (reclamations_processed)
    et le statut des interventions terrain (interventions_terrain / OMEGA).
    Cas typique : réclamation CRM=CLOTURE mais intervention OMEGA=EN_COURS.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    incoherences = 0
    verifies = 0

    try:
        with conn.cursor() as cur:
            # Vérification : réclamation clôturée dans CRM mais intervention terrain encore ouverte
            cur.execute("""
                SELECT COUNT(*) FROM reclamations.reclamations_processed
                WHERE statut = 'CLOTURE'
                  AND type_reclamation IN ('COUPURE_ELECTRIQUE', 'RACCORDEMENT_RESEAU', 'INTERVENTION_TECHNIQUE')
            """)
            verifies = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO reclamations.incoherences_systemes
                    (date_detection, type_incoherence, systeme_source, systeme_cible,
                     reclamation_id, client_id, valeur_source, valeur_cible, ecart_description, severite)
                SELECT
                    %s::DATE,
                    'CRM_OMEGA_STATUT',
                    'CRM',
                    'OMEGA',
                    rp.reclamation_id,
                    rp.client_id,
                    'CRM:CLOTURE',
                    'OMEGA:' || it.statut,
                    'Réclamation clôturée dans CRM mais intervention terrain en statut ' || it.statut,
                    CASE WHEN it.priorite <= 2 THEN 'ELEVEE' ELSE 'MODEREE' END
                FROM reclamations.reclamations_processed rp
                JOIN reclamations.interventions_terrain it
                    ON rp.reclamation_id = it.reclamation_id
                WHERE rp.statut = 'CLOTURE'
                  AND it.statut IN ('PLANIFIEE', 'EN_COURS')
                  AND NOT EXISTS (
                    SELECT 1 FROM reclamations.incoherences_systemes i
                    WHERE i.reclamation_id = rp.reclamation_id
                      AND i.type_incoherence = 'CRM_OMEGA_STATUT'
                      AND i.statut = 'OUVERTE'
                  )
            """, (run_date,))
            incoherences += cur.rowcount

        conn.commit()
        log.info("Contrôle CRM/OMEGA : %d vérifiés | %d incohérences", verifies, incoherences)
        ctx["ti"].xcom_push(key="crm_omega", value=(verifies, incoherences))

    finally:
        conn.close()

def check_sap_crm_factures(**ctx):
    """
    Vérifie la cohérence entre les données de facturation (SAP)
    et les réclamations de facturation dans le CRM.
    Cas typique : facture corrigée dans SAP mais réclamation FACTURATION toujours ouverte.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    incoherences = 0
    verifies = 0

    try:
        with conn.cursor() as cur:
            # Réclamations FACTURATION toujours ouvertes avec durée > SLA (10j = 240h)
            # alors que la correction SAP devrait avoir déclenché la clôture
            cur.execute("""
                SELECT COUNT(*) FROM reclamations.reclamations_facturation_detail
            """)
            verifies = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO reclamations.incoherences_systemes
                    (date_detection, type_incoherence, systeme_source, systeme_cible,
                     reclamation_id, client_id, valeur_source, valeur_cible,
                     ecart_description, severite)
                SELECT
                    %s::DATE,
                    'SAP_CRM_FACTURE',
                    'SAP',
                    'CRM',
                    rfd.reclamation_id,
                    rp.client_id,
                    'SAP:correction_possible',
                    'CRM:' || rp.statut,
                    'Réclamation FACTURATION ouverte depuis ' ||
                    ROUND(COALESCE(rp.duree_heures, 0), 0) || 'h — dépasse SLA 240h sans clôture CRM',
                    'ELEVEE'
                FROM reclamations.reclamations_facturation_detail rfd
                JOIN reclamations.reclamations_processed rp
                    ON rfd.reclamation_id = rp.reclamation_id
                WHERE rp.statut IN ('OUVERT', 'EN_COURS')
                  AND COALESCE(rp.duree_heures, 0) > 240
                  AND NOT EXISTS (
                    SELECT 1 FROM reclamations.incoherences_systemes i
                    WHERE i.reclamation_id = rp.reclamation_id
                      AND i.type_incoherence = 'SAP_CRM_FACTURE'
                      AND i.statut = 'OUVERTE'
                  )
            """, (run_date,))
            incoherences += cur.rowcount

        conn.commit()
        log.info("Contrôle SAP/CRM : %d vérifiés | %d incohérences", verifies, incoherences)
        ctx["ti"].xcom_push(key="sap_crm", value=(verifies, incoherences))

    finally:
        conn.close()

def check_system_delays(**ctx):
    """
    Détecte les délais de synchronisation anormaux entre systèmes.
    Si une réclamation a été créée dans CRM il y a > 4h et n'a pas
    encore d'intervention OMEGA créée pour les types terrain, c'est une anomalie.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    incoherences = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.incoherences_systemes
                    (date_detection, type_incoherence, systeme_source, systeme_cible,
                     reclamation_id, client_id, valeur_source, valeur_cible,
                     ecart_description, severite)
                SELECT
                    %s::DATE,
                    'DELAI_SYNC_CRM_OMEGA',
                    'CRM',
                    'OMEGA',
                    rp.reclamation_id,
                    rp.client_id,
                    'CRM: réclamation créée',
                    'OMEGA: aucune intervention',
                    'Réclamation terrain créée depuis ' ||
                    ROUND(COALESCE(rp.duree_heures, 0), 1)::TEXT || 'h sans intervention OMEGA créée',
                    CASE
                        WHEN rp.type_reclamation = 'COUPURE_ELECTRIQUE' THEN 'CRITIQUE'
                        ELSE 'ELEVEE'
                    END
                FROM reclamations.reclamations_processed rp
                LEFT JOIN reclamations.interventions_terrain it
                    ON rp.reclamation_id = it.reclamation_id
                WHERE rp.statut IN ('OUVERT', 'EN_COURS')
                  AND rp.type_reclamation IN ('COUPURE_ELECTRIQUE', 'RACCORDEMENT_RESEAU', 'INTERVENTION_TECHNIQUE')
                  AND COALESCE(rp.duree_heures, 0) > 4  -- plus de 4h sans intervention
                  AND it.intervention_id IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM reclamations.incoherences_systemes i
                    WHERE i.reclamation_id = rp.reclamation_id
                      AND i.type_incoherence = 'DELAI_SYNC_CRM_OMEGA'
                      AND i.statut = 'OUVERTE'
                  )
            """, (run_date,))
            incoherences += cur.rowcount

        conn.commit()
        log.info("Délais de synchronisation détectés : %d", incoherences)
        ctx["ti"].xcom_push(key="delays", value=incoherences)

    finally:
        conn.close()

def log_reconciliation_results(**ctx):
    """Enregistre les résultats dans reconciliation_log et génère le rapport."""
    run_date = ctx["ds"]
    crm_omega = ctx["ti"].xcom_pull(key="crm_omega", task_ids="check_crm_omega_coherence") or (0, 0)
    sap_crm = ctx["ti"].xcom_pull(key="sap_crm", task_ids="check_sap_crm_factures") or (0, 0)
    delays = ctx["ti"].xcom_pull(key="delays", task_ids="check_system_delays") or 0

    conn = psycopg2.connect(**DB_CONFIG)
    total_inco = crm_omega[1] + sap_crm[1] + delays

    try:
        with conn.cursor() as cur:
            for controle, verifies, inco in [
                ("CRM_OMEGA", crm_omega[0], crm_omega[1]),
                ("SAP_CRM", sap_crm[0], sap_crm[1]),
                ("DELAI_SYNC", 0, delays),
            ]:
                cur.execute("""
                    INSERT INTO reclamations.reconciliation_log
                        (date_run, type_controle, nb_verifies, nb_incoherences,
                         taux_coherence_pct, detail)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (run_date, controle, verifies, inco,
                      round(100 * (verifies - inco) / max(verifies, 1), 2),
                      f"{inco} incohérences sur {verifies} vérifications"))
        conn.commit()

        log.info("=== Rapport Réconciliation Multi-Systèmes [%s] ===", run_date)
        log.info("  CRM/OMEGA : %d incohérences sur %d vérifications", crm_omega[1], crm_omega[0])
        log.info("  SAP/CRM   : %d incohérences sur %d vérifications", sap_crm[1], sap_crm[0])
        log.info("  Délais sync : %d anomalies", delays)
        log.info("  Total : %d incohérences", total_inco)
        ctx["ti"].xcom_push(key="total_inco", value=total_inco)

    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    total = ctx["ti"].xcom_pull(key="total_inco", task_ids="log_reconciliation_results") or 0
    log_pipeline_run(DAG_ID, "WARNING" if total > 0 else "SUCCESS", total, 0,
                     f"Réconciliation SI [{ctx['ds']}] — {total} incohérences CRM/OMEGA/SAP")

with DAG(
    dag_id=DAG_ID,
    description="Réconciliation des données entre CRM (Salesforce), OMEGA (terrain) et SAP (facturation)",
    schedule="30 6 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["réconciliation", "crm", "omega", "sap", "multi-systèmes"],
    doc_md=__doc__,
) as dag:

        t1 = PythonOperator(task_id="create_reconciliation_tables", python_callable=create_reconciliation_tables)
    t2 = PythonOperator(task_id="check_crm_omega_coherence",    python_callable=check_crm_omega_coherence)
    t3 = PythonOperator(task_id="check_sap_crm_factures",       python_callable=check_sap_crm_factures)
    t4 = PythonOperator(task_id="check_system_delays",          python_callable=check_system_delays)
    t5 = PythonOperator(task_id="log_reconciliation_results",   python_callable=log_reconciliation_results)
    t6 = PythonOperator(task_id="notify_pipeline_run",          python_callable=notify_pipeline_run, trigger_rule="all_done")

    wait >> t1 >> [t2, t3, t4] >> t5 >> t6
