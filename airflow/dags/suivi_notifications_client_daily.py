"""
suivi_notifications_client_daily.py
Suivi des SLA de communication client — obligations légales Enedis.

Enedis a des obligations légales de notification :
  - Coupure programmée : prévenir le client >= 5 jours avant (art. L.322-7)
  - Prise en charge réclamation : accusé réception sous 10 jours (loi Chatel)
  - Résolution : notification sous 48h après clôture (charte qualité Enedis)
  - Incident systémique : notification des clients affectés dans les 2h

Ce DAG mesure ces SLA de communication distincts des SLA de résolution,
et détecte les clients non notifiés dans les délais légaux.

Tables produites :
  - reclamations.notifications_client      : historique des notifications
  - reclamations.sla_communication         : suivi SLA notification par type
  - reclamations.clients_non_notifies      : clients en attente de notification

Schedule : quotidien 06:45
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "suivi_notifications_client_daily"

# ─── SLA de communication (en heures) ────────────────────────────────────────
SLA_NOTIFICATION = {
    "ACCUSE_RECEPTION":      240,   # 10 jours — loi Chatel
    "COUPURE_PROGRAMMEE":    120,   # 5 jours avant — art. L.322-7
    "RESOLUTION_RECLAMATION": 48,   # 48h après clôture — charte Enedis
    "INCIDENT_SYSTEMIQUE":     2,   # 2h — protocole crise
    "DEPASSEMENT_SLA":        24,   # 24h avant dépassement SLA prévu
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_notification_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        CREATE TABLE IF NOT EXISTS reclamations.notifications_client (
            id                  SERIAL PRIMARY KEY,
            reclamation_id      VARCHAR(50),
            client_id           VARCHAR(50) NOT NULL,
            type_notification   VARCHAR(100) NOT NULL,
            canal               VARCHAR(50),   -- SMS, EMAIL, COURRIER, APPEL
            date_echeance       TIMESTAMP WITH TIME ZONE,
            date_envoi          TIMESTAMP WITH TIME ZONE,
            statut              VARCHAR(50) DEFAULT 'EN_ATTENTE',  -- EN_ATTENTE, ENVOYE, ECHEC
            delai_reel_h        NUMERIC(8, 2),
            dans_delai_legal    BOOLEAN,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.sla_communication (
            id                  SERIAL PRIMARY KEY,
            date_calcul         DATE NOT NULL,
            type_notification   VARCHAR(100) NOT NULL,
            nb_total            INTEGER,
            nb_dans_delai       INTEGER,
            nb_hors_delai       INTEGER,
            taux_conformite_pct NUMERIC(5, 2),
            sla_heures          INTEGER,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (date_calcul, type_notification)
        );

        CREATE TABLE IF NOT EXISTS reclamations.clients_non_notifies (
            id                  SERIAL PRIMARY KEY,
            reclamation_id      VARCHAR(50),
            client_id           VARCHAR(50) NOT NULL,
            type_notification_manquante VARCHAR(100),
            echeance_depassee   BOOLEAN DEFAULT FALSE,
            heures_de_retard    NUMERIC(8, 2),
            date_identification DATE,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_notif_client ON reclamations.notifications_client(client_id);
        CREATE INDEX IF NOT EXISTS idx_notif_type   ON reclamations.notifications_client(type_notification);
        CREATE INDEX IF NOT EXISTS idx_sla_com_date ON reclamations.sla_communication(date_calcul);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

def detect_missing_acknowledgements(**ctx):
    """
    Détecte les réclamations sans accusé de réception client
    au-delà de 10 jours (loi Chatel).
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    manquants = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.clients_non_notifies
                    (reclamation_id, client_id,
                     type_notification_manquante, echeance_depassee,
                     heures_de_retard, date_identification)
                SELECT
                    rp.reclamation_id,
                    rp.client_id,
                    'ACCUSE_RECEPTION',
                    COALESCE(rp.duree_heures, 0) > %s,
                    GREATEST(0, COALESCE(rp.duree_heures, 0) - %s),
                    %s::DATE
                FROM reclamations.reclamations_processed rp
                LEFT JOIN reclamations.notifications_client nc
                    ON rp.reclamation_id = nc.reclamation_id
                    AND nc.type_notification = 'ACCUSE_RECEPTION'
                    AND nc.statut = 'ENVOYE'
                WHERE rp.statut IN ('OUVERT', 'EN_COURS')
                  AND COALESCE(rp.duree_heures, 0) > (%s * 0.8)  -- alerte à 80% du délai
                  AND nc.id IS NULL
                ON CONFLICT DO NOTHING
            """, (SLA_NOTIFICATION["ACCUSE_RECEPTION"],
                  SLA_NOTIFICATION["ACCUSE_RECEPTION"],
                  run_date,
                  SLA_NOTIFICATION["ACCUSE_RECEPTION"]))
            manquants += cur.rowcount

        conn.commit()
        log.info("Accusés de réception manquants : %d", manquants)
        ctx["ti"].xcom_push(key="ar_manquants", value=manquants)

    finally:
        conn.close()

def detect_missing_resolution_notifications(**ctx):
    """
    Vérifie que les clients ont bien été notifiés de la résolution
    dans les 48h suivant la clôture.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    manquants = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.clients_non_notifies
                    (reclamation_id, client_id,
                     type_notification_manquante, echeance_depassee,
                     heures_de_retard, date_identification)
                SELECT
                    rp.reclamation_id,
                    rp.client_id,
                    'RESOLUTION_RECLAMATION',
                    TRUE,
                    -- Heures depuis clôture : approximé par durée totale - SLA résolution
                    GREATEST(0, COALESCE(rp.duree_heures, 0)
                        - CASE rp.type_reclamation
                            WHEN 'COUPURE_ELECTRIQUE'    THEN 48
                            WHEN 'COMPTEUR_LINKY'        THEN 120
                            WHEN 'FACTURATION'           THEN 240
                            WHEN 'RACCORDEMENT_RESEAU'   THEN 360
                            ELSE 168
                          END
                        - %s),
                    %s::DATE
                FROM reclamations.reclamations_processed rp
                LEFT JOIN reclamations.notifications_client nc
                    ON rp.reclamation_id = nc.reclamation_id
                    AND nc.type_notification = 'RESOLUTION_RECLAMATION'
                    AND nc.statut = 'ENVOYE'
                WHERE rp.statut = 'CLOTURE'
                  AND DATE(rp.date_creation) >= %s::DATE - INTERVAL '3 days'
                  AND nc.id IS NULL
                ON CONFLICT DO NOTHING
            """, (SLA_NOTIFICATION["RESOLUTION_RECLAMATION"], run_date, run_date))
            manquants += cur.rowcount

        conn.commit()
        log.info("Notifications de résolution manquantes : %d", manquants)
        ctx["ti"].xcom_push(key="resol_manquants", value=manquants)

    finally:
        conn.close()

def detect_incident_notification_gaps(**ctx):
    """
    Vérifie que les clients affectés par un incident systémique
    ont bien été notifiés dans les 2h (protocole crise Enedis).
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    manquants = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.clients_non_notifies
                    (reclamation_id, client_id,
                     type_notification_manquante, echeance_depassee,
                     heures_de_retard, date_identification)
                SELECT
                    ri.reclamation_id,
                    rp.client_id,
                    'INCIDENT_SYSTEMIQUE',
                    TRUE,
                    EXTRACT(EPOCH FROM (NOW() - i.date_detection)) / 3600.0 - %s,
                    %s::DATE
                FROM reclamations.reclamations_incidents ri
                JOIN reclamations.incidents_systemiques i ON ri.incident_id = i.incident_id
                JOIN reclamations.reclamations_processed rp ON ri.reclamation_id = rp.reclamation_id
                LEFT JOIN reclamations.notifications_client nc
                    ON rp.client_id = nc.client_id
                    AND nc.type_notification = 'INCIDENT_SYSTEMIQUE'
                    AND nc.date_envoi >= i.date_detection
                WHERE DATE(i.date_detection) = %s
                  AND EXTRACT(EPOCH FROM (NOW() - i.date_detection)) / 3600.0 > %s
                  AND nc.id IS NULL
                ON CONFLICT DO NOTHING
            """, (SLA_NOTIFICATION["INCIDENT_SYSTEMIQUE"], run_date, run_date,
                  SLA_NOTIFICATION["INCIDENT_SYSTEMIQUE"]))
            manquants += cur.rowcount

        conn.commit()
        log.info("Notifications incident manquantes : %d", manquants)
        ctx["ti"].xcom_push(key="incident_manquants", value=manquants)

    finally:
        conn.close()

def compute_communication_sla(**ctx):
    """Calcule les KPIs de conformité des SLA de communication."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            for type_notif, sla_h in SLA_NOTIFICATION.items():
                cur.execute("""
                    INSERT INTO reclamations.sla_communication
                        (date_calcul, type_notification, nb_total,
                         nb_dans_delai, nb_hors_delai,
                         taux_conformite_pct, sla_heures)
                    SELECT
                        %s::DATE,
                        %s,
                        COUNT(*),
                        COUNT(*) FILTER (WHERE dans_delai_legal = TRUE),
                        COUNT(*) FILTER (WHERE dans_delai_legal = FALSE OR dans_delai_legal IS NULL),
                        ROUND(100.0 * COUNT(*) FILTER (WHERE dans_delai_legal = TRUE)
                              / NULLIF(COUNT(*), 0)::NUMERIC, 2),
                        %s
                    FROM reclamations.notifications_client
                    WHERE type_notification = %s
                      AND DATE(created_at) = %s
                    ON CONFLICT (date_calcul, type_notification) DO UPDATE SET
                        nb_total            = EXCLUDED.nb_total,
                        nb_dans_delai       = EXCLUDED.nb_dans_delai,
                        taux_conformite_pct = EXCLUDED.taux_conformite_pct
                """, (run_date, type_notif, sla_h, type_notif, run_date))

        conn.commit()
        log.info("SLA communication calculés")

    finally:
        conn.close()

def generate_notification_report(**ctx):
    run_date = ctx["ds"]
    ar = ctx["ti"].xcom_pull(key="ar_manquants", task_ids="detect_missing_acknowledgements") or 0
    resol = ctx["ti"].xcom_pull(key="resol_manquants", task_ids="detect_missing_resolution_notifications") or 0
    incident = ctx["ti"].xcom_pull(key="incident_manquants", task_ids="detect_incident_notification_gaps") or 0

    log.info("=== Rapport Notifications Client [%s] ===", run_date)
    log.info("  Accusés réception manquants  : %d", ar)
    log.info("  Notifications résolution manquantes : %d", resol)
    log.info("  Notifications incident manquantes   : %d 🚨", incident)
    log.info("  Total notifications manquantes      : %d", ar + resol + incident)
    ctx["ti"].xcom_push(key="total_manquants", value=ar + resol + incident)

def notify_pipeline_run(**ctx):
    total = ctx["ti"].xcom_pull(key="total_manquants", task_ids="generate_notification_report") or 0
    log_pipeline_run(DAG_ID, "WARNING" if total > 0 else "SUCCESS", total, 0,
                     f"Notifications client [{ctx['ds']}] — {total} notifications manquantes")

with DAG(
    dag_id=DAG_ID,
    description="Suivi des SLA de communication client (accusé réception, résolution, incident)",
    schedule="45 6 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["notifications", "communication", "sla", "légal", "client"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_notification_tables",             python_callable=create_notification_tables)
    t2 = PythonOperator(task_id="detect_missing_acknowledgements",        python_callable=detect_missing_acknowledgements)
    t3 = PythonOperator(task_id="detect_missing_resolution_notifications",python_callable=detect_missing_resolution_notifications)
    t4 = PythonOperator(task_id="detect_incident_notification_gaps",      python_callable=detect_incident_notification_gaps)
    t5 = PythonOperator(task_id="compute_communication_sla",              python_callable=compute_communication_sla)
    t6 = PythonOperator(task_id="generate_notification_report",           python_callable=generate_notification_report)
    t7 = PythonOperator(task_id="notify_pipeline_run",                    python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t5 >> t6 >> t7
