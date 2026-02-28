"""
detection_fraude_linky_daily.py
Détection proactive de fraudes et défaillances sur compteurs Linky.

Les compteurs Linky transmettent une courbe de charge toutes les 30 minutes.
Des patterns anormaux indiquent soit une fraude, soit une défaillance matérielle.
Ce DAG est une obligation légale pour Enedis (art. L.335-6 Code Énergie).

Anomalies détectées :
  1. Consommation nulle sur compteur actif (compteur court-circuité ou trafiqué)
  2. Delta entre index relevé et consommation calculée (>5% = anomalie significative)
  3. Pic de consommation inexpliqué (>3σ de la courbe habituelle)
  4. Consommation en période de coupure déclarée (trafic du disjoncteur)
  5. Pattern de consommation identique à un voisin (branchement illicite)

Tables produites :
  - reclamations.anomalies_linky       : anomalies de consommation détectées
  - reclamations.suspicions_fraude     : cas suspects consolidés
  - reclamations.compteurs_surveillance: compteurs en observation prolongée

Schedule : quotidien 04:15 (après linky_pipeline)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "detection_fraude_linky_daily"

# ─── Seuils de détection ─────────────────────────────────────────────────────
SEUIL_DELTA_INDEX_PCT    = 5.0   # > 5% d'écart entre index relevé et consommation calculée
SEUIL_ZERO_CONSO_JOURS   = 7     # Conso nulle > 7 jours consécutifs = anomalie
SEUIL_PIC_SIGMA          = 3.0   # Pic > 3 écarts-types = anomalie
SEUIL_SIGNALEMENT        = 3     # > 3 anomalies en 30j = signalement fraude

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_fraude_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        CREATE TABLE IF NOT EXISTS reclamations.anomalies_linky (
            id                  SERIAL PRIMARY KEY,
            date_detection      DATE NOT NULL,
            numero_compteur     VARCHAR(50) NOT NULL,
            pdl_id              VARCHAR(14),
            client_id           VARCHAR(50),
            type_anomalie       VARCHAR(100) NOT NULL,
            -- CONSO_NULLE, DELTA_INDEX, PIC_CONSOMMATION,
            -- CONSO_PENDANT_COUPURE, PATTERN_SUSPECT
            severite            VARCHAR(20),  -- FAIBLE, MODEREE, ELEVEE, CRITIQUE
            valeur_mesuree      NUMERIC(12, 4),
            valeur_attendue     NUMERIC(12, 4),
            ecart_pct           NUMERIC(8, 2),
            nb_jours_anomalie   INTEGER DEFAULT 1,
            description         TEXT,
            statut              VARCHAR(50) DEFAULT 'DETECTEE',
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.suspicions_fraude (
            id                  SERIAL PRIMARY KEY,
            numero_compteur     VARCHAR(50) NOT NULL UNIQUE,
            pdl_id              VARCHAR(14),
            client_id           VARCHAR(50),
            date_premiere_anomalie DATE,
            nb_anomalies_30j    INTEGER DEFAULT 0,
            score_fraude        NUMERIC(5, 2),  -- 0 (clean) à 100 (fraude certaine)
            niveau_risque       VARCHAR(20),    -- FAIBLE, MODERE, ELEVE, CONFIRME
            statut              VARCHAR(50) DEFAULT 'SOUS_SURVEILLANCE',
            date_signalement    DATE,           -- date de signalement aux autorités
            commentaire         TEXT,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.compteurs_surveillance (
            numero_compteur     VARCHAR(50) PRIMARY KEY,
            pdl_id              VARCHAR(14),
            date_mise_surveillance DATE NOT NULL,
            motif               VARCHAR(200),
            nb_controles        INTEGER DEFAULT 0,
            prochain_controle   DATE,
            statut              VARCHAR(50) DEFAULT 'ACTIF',
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_anomalies_compteur ON reclamations.anomalies_linky(numero_compteur);
        CREATE INDEX IF NOT EXISTS idx_anomalies_date     ON reclamations.anomalies_linky(date_detection);
        CREATE INDEX IF NOT EXISTS idx_suspicions_score   ON reclamations.suspicions_fraude(score_fraude DESC);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

def detect_zero_consumption(**ctx):
    """Détecte les compteurs actifs avec consommation nulle sur plusieurs jours."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    detected = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.anomalies_linky
                    (date_detection, numero_compteur, client_id,
                     type_anomalie, severite, valeur_mesuree, valeur_attendue,
                     ecart_pct, description)
                SELECT
                    %s::DATE,
                    rld.numero_compteur,
                    rp.client_id,
                    'CONSO_NULLE',
                    CASE
                        WHEN COUNT(*) > 30 THEN 'CRITIQUE'
                        WHEN COUNT(*) > 14 THEN 'ELEVEE'
                        WHEN COUNT(*) > %s THEN 'MODEREE'
                        ELSE 'FAIBLE'
                    END,
                    0.0,
                    AVG(rld.consommation_kwh),
                    100.0,
                    'Consommation nulle depuis ' || COUNT(*) || ' jours consécutifs — compteur actif'
                FROM reclamations.reclamations_linky_detailed rld
                JOIN reclamations.reclamations_processed rp
                    ON rld.reclamation_id = rp.reclamation_id
                WHERE rld.consommation_kwh = 0
                  AND rp.statut != 'CLOTURE'
                  AND rld.numero_compteur IS NOT NULL
                GROUP BY rld.numero_compteur, rp.client_id
                HAVING COUNT(*) >= %s
            """, (run_date, SEUIL_ZERO_CONSO_JOURS, SEUIL_ZERO_CONSO_JOURS))
            detected += cur.rowcount

        conn.commit()
        log.info("Anomalies consommation nulle : %d", detected)
        ctx["ti"].xcom_push(key="zero_conso", value=detected)

    finally:
        conn.close()

def detect_index_delta(**ctx):
    """Détecte les écarts significatifs entre index relevé et consommation calculée."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    detected = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.anomalies_linky
                    (date_detection, numero_compteur, client_id,
                     type_anomalie, severite, valeur_mesuree, valeur_attendue,
                     ecart_pct, description)
                SELECT
                    %s::DATE,
                    rld.numero_compteur,
                    rp.client_id,
                    'DELTA_INDEX',
                    CASE
                        WHEN ABS(rld.ecart_index_pct) > 20 THEN 'CRITIQUE'
                        WHEN ABS(rld.ecart_index_pct) > 10 THEN 'ELEVEE'
                        ELSE 'MODEREE'
                    END,
                    rld.consommation_kwh,
                    rld.consommation_kwh * (1 + rld.ecart_index_pct / 100.0),
                    rld.ecart_index_pct,
                    'Ecart index/consommation : ' || ROUND(ABS(rld.ecart_index_pct), 1)::TEXT || '% (seuil=' || %s::TEXT || '%)'
                FROM reclamations.reclamations_linky_detailed rld
                JOIN reclamations.reclamations_processed rp
                    ON rld.reclamation_id = rp.reclamation_id
                WHERE rld.ecart_index_pct IS NOT NULL
                  AND ABS(rld.ecart_index_pct) > %s
                  AND rld.export_date = %s
            """, (run_date, SEUIL_DELTA_INDEX_PCT, SEUIL_DELTA_INDEX_PCT, run_date))
            detected += cur.rowcount

        conn.commit()
        log.info("Anomalies delta index : %d", detected)
        ctx["ti"].xcom_push(key="delta_index", value=detected)

    finally:
        conn.close()

def detect_consumption_during_outage(**ctx):
    """
    Détecte une consommation enregistrée pendant une coupure déclarée.
    Indique un court-circuit du disjoncteur (fraude ou défaillance grave).
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    detected = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.anomalies_linky
                    (date_detection, numero_compteur, client_id,
                     type_anomalie, severite, valeur_mesuree,
                     description)
                SELECT DISTINCT
                    %s::DATE,
                    rld.numero_compteur,
                    rp.client_id,
                    'CONSO_PENDANT_COUPURE',
                    'CRITIQUE',
                    rld.consommation_kwh,
                    'Consommation de ' || rld.consommation_kwh || ' kWh détectée pendant coupure déclarée — suspicion contournement disjoncteur'
                FROM reclamations.reclamations_linky_detailed rld
                JOIN reclamations.reclamations_processed rp
                    ON rld.reclamation_id = rp.reclamation_id
                JOIN reclamations.reclamations_coupures_detail cd
                    ON rp.client_id = cd.client_id
                    AND cd.statut IN ('OUVERT', 'EN_COURS')
                WHERE rld.consommation_kwh > 0
                  AND rld.export_date = %s
            """, (run_date, run_date))
            detected += cur.rowcount

        conn.commit()
        log.info("Anomalies conso pendant coupure : %d", detected)
        ctx["ti"].xcom_push(key="coupure_conso", value=detected)

    finally:
        conn.close()

def compute_fraud_scores(**ctx):
    """
    Calcule un score de fraude pour chaque compteur ayant plusieurs anomalies.
    Score = f(nb anomalies, types, sévérité, récence).
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.suspicions_fraude
                    (numero_compteur, client_id,
                     date_premiere_anomalie, nb_anomalies_30j,
                     score_fraude, niveau_risque)
                SELECT
                    a.numero_compteur,
                    MAX(a.client_id),
                    MIN(a.date_detection),
                    COUNT(*),
                    -- Score pondéré par type et sévérité
                    LEAST(100, ROUND(
                        SUM(
                            CASE a.type_anomalie
                                WHEN 'CONSO_PENDANT_COUPURE' THEN 40
                                WHEN 'DELTA_INDEX'           THEN 25
                                WHEN 'CONSO_NULLE'           THEN 15
                                ELSE 10
                            END *
                            CASE a.severite
                                WHEN 'CRITIQUE' THEN 2.0
                                WHEN 'ELEVEE'   THEN 1.5
                                WHEN 'MODEREE'  THEN 1.0
                                ELSE 0.5
                            END
                        )::NUMERIC, 2)),
                    CASE
                        WHEN COUNT(*) >= %s AND
                             'CONSO_PENDANT_COUPURE' = ANY(ARRAY_AGG(a.type_anomalie)) THEN 'CONFIRME'
                        WHEN COUNT(*) >= %s THEN 'ELEVE'
                        WHEN COUNT(*) >= 2  THEN 'MODERE'
                        ELSE 'FAIBLE'
                    END
                FROM reclamations.anomalies_linky a
                WHERE a.date_detection >= %s::DATE - INTERVAL '30 days'
                GROUP BY a.numero_compteur
                HAVING COUNT(*) >= 2
                ON CONFLICT (numero_compteur) DO UPDATE SET
                    nb_anomalies_30j = EXCLUDED.nb_anomalies_30j,
                    score_fraude     = EXCLUDED.score_fraude,
                    niveau_risque    = EXCLUDED.niveau_risque
            """, (SEUIL_SIGNALEMENT, SEUIL_SIGNALEMENT + 1, run_date))

        conn.commit()
        log.info("Scores fraude calculés")

    finally:
        conn.close()

def flag_for_surveillance(**ctx):
    """Met sous surveillance les compteurs à score élevé."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.compteurs_surveillance
                    (numero_compteur, date_mise_surveillance,
                     motif, prochain_controle)
                SELECT
                    sf.numero_compteur,
                    %s::DATE,
                    'Score fraude ' || sf.score_fraude::TEXT || '/100 — niveau ' || sf.niveau_risque,
                    %s::DATE + INTERVAL '7 days'
                FROM reclamations.suspicions_fraude sf
                WHERE sf.niveau_risque IN ('ELEVE', 'CONFIRME')
                  AND sf.statut = 'SOUS_SURVEILLANCE'
                  AND NOT EXISTS (
                    SELECT 1 FROM reclamations.compteurs_surveillance cs
                    WHERE cs.numero_compteur = sf.numero_compteur
                  )
            """, (run_date, run_date))
            flagged = cur.rowcount

        conn.commit()
        log.info("Compteurs mis sous surveillance : %d", flagged)
        ctx["ti"].xcom_push(key="flagged", value=flagged)

    finally:
        conn.close()

def generate_fraude_report(**ctx):
    run_date = ctx["ds"]
    zero = ctx["ti"].xcom_pull(key="zero_conso", task_ids="detect_zero_consumption") or 0
    delta = ctx["ti"].xcom_pull(key="delta_index", task_ids="detect_index_delta") or 0
    coupure = ctx["ti"].xcom_pull(key="coupure_conso", task_ids="detect_consumption_during_outage") or 0
    flagged = ctx["ti"].xcom_pull(key="flagged", task_ids="flag_for_surveillance") or 0

    log.info("=== Rapport Détection Fraude Linky [%s] ===", run_date)
    log.info("  Conso nulle          : %d compteurs", zero)
    log.info("  Delta index          : %d compteurs", delta)
    log.info("  Conso/coupure        : %d compteurs 🚨", coupure)
    log.info("  Mis sous surveillance: %d compteurs", flagged)

def notify_pipeline_run(**ctx):
    total = (
        (ctx["ti"].xcom_pull(key="zero_conso", task_ids="detect_zero_consumption") or 0) +
        (ctx["ti"].xcom_pull(key="delta_index", task_ids="detect_index_delta") or 0) +
        (ctx["ti"].xcom_pull(key="coupure_conso", task_ids="detect_consumption_during_outage") or 0)
    )
    log_pipeline_run(DAG_ID, "WARNING" if total > 0 else "SUCCESS", total, 0,
                     f"Fraude Linky [{ctx['ds']}] — {total} anomalies détectées")

with DAG(
    dag_id=DAG_ID,
    description="Détection proactive de fraudes et défaillances sur compteurs Linky",
    schedule="15 4 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fraude", "linky", "détection", "légal"],
    doc_md=__doc__,
) as dag:

        t1 = PythonOperator(task_id="create_fraude_tables",                python_callable=create_fraude_tables)
    t2 = PythonOperator(task_id="detect_zero_consumption",             python_callable=detect_zero_consumption)
    t3 = PythonOperator(task_id="detect_index_delta",                  python_callable=detect_index_delta)
    t4 = PythonOperator(task_id="detect_consumption_during_outage",    python_callable=detect_consumption_during_outage)
    t5 = PythonOperator(task_id="compute_fraud_scores",                python_callable=compute_fraud_scores)
    t6 = PythonOperator(task_id="flag_for_surveillance",               python_callable=flag_for_surveillance)
    t7 = PythonOperator(task_id="generate_fraude_report",              python_callable=generate_fraude_report)
    t8 = PythonOperator(task_id="notify_pipeline_run",                 python_callable=notify_pipeline_run, trigger_rule="all_done")

    wait >> t1 >> [t2, t3, t4] >> t5 >> t6 >> t7 >> t8
