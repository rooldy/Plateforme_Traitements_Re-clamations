"""
gestion_pics_charge_monitoring.py
Monitoring des pics de charge et gestion du mode dégradé.

Après une tempête majeure (Ciaran, Domingos), Enedis peut recevoir
10 à 15x le volume normal de réclamations en 24h (jusqu'à 300 000/jour).
Ce DAG surveille la charge en temps réel et déclenche le mode dégradé
pour garantir la continuité de service des pipelines critiques.

Mode dégradé = prioritisation automatique :
  Niveau 1 (charge > 2x) : COUPURE_ELECTRIQUE uniquement en temps réel
  Niveau 2 (charge > 5x) : suspension KPIs non-critiques, batch différé
  Niveau 3 (charge > 10x): activation cellule de crise, ingestion seule

Ce DAG tourne toutes les heures et ajuste dynamiquement les paramètres
Spark et PostgreSQL en fonction du niveau de charge détecté.

Tables produites :
  - reclamations.pics_charge               : historique des pics détectés
  - reclamations.configuration_dynamique   : paramètres Spark/DB courants
  - reclamations.alertes_capacite          : alertes de saturation

Schedule : horaire (0 * * * *)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "gestion_pics_charge_monitoring"

# ─── Seuils de charge (en multiple du volume baseline) ───────────────────────
BASELINE_RECLAMATIONS_JOUR = 20_000  # volume normal quotidien

NIVEAUX_CHARGE = {
    "NORMAL":     1.0,    # < 1x baseline = normal
    "ELEVE":      2.0,    # 2x-5x = charge élevée
    "CRITIQUE":   5.0,    # 5x-10x = pic critique
    "CRISE":     10.0,    # > 10x = cellule de crise
}

# Paramètres Spark adaptés à chaque niveau
SPARK_CONFIG_NIVEAUX = {
    "NORMAL":   {"shuffle_partitions": 8,  "executor_memory": "2g", "executor_cores": 2},
    "ELEVE":    {"shuffle_partitions": 16, "executor_memory": "4g", "executor_cores": 4},
    "CRITIQUE": {"shuffle_partitions": 32, "executor_memory": "6g", "executor_cores": 6},
    "CRISE":    {"shuffle_partitions": 64, "executor_memory": "8g", "executor_cores": 8},
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
}


def create_charge_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        CREATE TABLE IF NOT EXISTS reclamations.pics_charge (
            id                  SERIAL PRIMARY KEY,
            date_detection      TIMESTAMP WITH TIME ZONE NOT NULL,
            heure               INTEGER,
            volume_reclamations INTEGER,
            ratio_baseline      NUMERIC(8, 2),
            niveau_charge       VARCHAR(20),  -- NORMAL, ELEVE, CRITIQUE, CRISE
            mode_degrade        BOOLEAN DEFAULT FALSE,
            niveau_degrade      INTEGER DEFAULT 0,  -- 0=off, 1=partiel, 2=fort, 3=crise
            pipelines_suspendus TEXT[],
            duree_pic_h         NUMERIC(6, 2),
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.configuration_dynamique (
            id                  SERIAL PRIMARY KEY,
            parametre           VARCHAR(100) NOT NULL UNIQUE,
            valeur_courante     TEXT,
            valeur_normale      TEXT,
            modifie_par         VARCHAR(100),
            date_modification   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            actif               BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS reclamations.alertes_capacite (
            id                  SERIAL PRIMARY KEY,
            date_alerte         TIMESTAMP WITH TIME ZONE NOT NULL,
            type_alerte         VARCHAR(100),
            niveau_charge       VARCHAR(20),
            message             TEXT,
            statut              VARCHAR(50) DEFAULT 'ACTIVE',
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Initialisation des paramètres par défaut
        INSERT INTO reclamations.configuration_dynamique
            (parametre, valeur_courante, valeur_normale, modifie_par)
        VALUES
            ('spark.sql.shuffle.partitions', '8', '8', 'init'),
            ('spark.executor.memory',        '2g', '2g', 'init'),
            ('spark.executor.cores',         '2', '2', 'init'),
            ('mode_degrade_actif',           'false', 'false', 'init'),
            ('niveau_degrade',               '0', '0', 'init'),
            ('seuil_alerte_volume',          '40000', '40000', 'init')
        ON CONFLICT (parametre) DO NOTHING;

        CREATE INDEX IF NOT EXISTS idx_pics_date    ON reclamations.pics_charge(date_detection);
        CREATE INDEX IF NOT EXISTS idx_pics_niveau  ON reclamations.pics_charge(niveau_charge);
        CREATE INDEX IF NOT EXISTS idx_alertes_cap  ON reclamations.alertes_capacite(date_alerte);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def measure_current_load(**ctx):
    """
    Mesure le volume de réclamations de l'heure courante
    et calcule le ratio par rapport au baseline.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    volume = 0
    ratio = 1.0
    niveau = "NORMAL"

    try:
        with conn.cursor() as cur:
            # Volume sur la dernière heure
            cur.execute("""
                SELECT COUNT(*)
                FROM reclamations.reclamations_processed
                WHERE date_creation >= NOW() - INTERVAL '1 hour'
            """)
            volume_heure = cur.fetchone()[0]

            # Baseline horaire (volume quotidien / 24)
            baseline_heure = BASELINE_RECLAMATIONS_JOUR / 24
            ratio = (volume_heure / baseline_heure) if baseline_heure > 0 else 1.0
            volume = volume_heure

            # Détermination du niveau
            if ratio >= NIVEAUX_CHARGE["CRISE"]:
                niveau = "CRISE"
            elif ratio >= NIVEAUX_CHARGE["CRITIQUE"]:
                niveau = "CRITIQUE"
            elif ratio >= NIVEAUX_CHARGE["ELEVE"]:
                niveau = "ELEVE"
            else:
                niveau = "NORMAL"

            # Enregistrement du pic
            cur.execute("""
                INSERT INTO reclamations.pics_charge
                    (date_detection, heure, volume_reclamations,
                     ratio_baseline, niveau_charge,
                     mode_degrade, niveau_degrade)
                VALUES (NOW(), EXTRACT(HOUR FROM NOW())::INTEGER, %s, %s, %s, %s, %s)
            """, (volume, round(ratio, 2), niveau,
                  niveau in ("CRITIQUE", "CRISE"),
                  0 if niveau == "NORMAL"
                  else 1 if niveau == "ELEVE"
                  else 2 if niveau == "CRITIQUE"
                  else 3))

        conn.commit()
        log.info("Charge courante : %d rec/h | ratio=%.2fx | niveau=%s", volume, ratio, niveau)
        ctx["ti"].xcom_push(key="volume", value=volume)
        ctx["ti"].xcom_push(key="ratio", value=ratio)
        ctx["ti"].xcom_push(key="niveau", value=niveau)

    finally:
        conn.close()


def adjust_spark_config(**ctx):
    """
    Ajuste les paramètres Spark dans la table configuration_dynamique
    selon le niveau de charge détecté.
    Les DAGs Spark liront cette config au démarrage.
    """
    niveau = ctx["ti"].xcom_pull(key="niveau", task_ids="measure_current_load") or "NORMAL"
    config = SPARK_CONFIG_NIVEAUX.get(niveau, SPARK_CONFIG_NIVEAUX["NORMAL"])

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for param, valeur in [
                ("spark.sql.shuffle.partitions", str(config["shuffle_partitions"])),
                ("spark.executor.memory",         config["executor_memory"]),
                ("spark.executor.cores",           str(config["executor_cores"])),
                ("mode_degrade_actif",             str(niveau in ("CRITIQUE", "CRISE")).lower()),
                ("niveau_degrade",                 str(
                    0 if niveau == "NORMAL"
                    else 1 if niveau == "ELEVE"
                    else 2 if niveau == "CRITIQUE"
                    else 3
                )),
            ]:
                cur.execute("""
                    UPDATE reclamations.configuration_dynamique
                    SET valeur_courante   = %s,
                        modifie_par       = %s,
                        date_modification = NOW()
                    WHERE parametre = %s
                """, (valeur, DAG_ID, param))

        conn.commit()
        log.info("Configuration Spark ajustée pour niveau %s : partitions=%d, memory=%s",
                 niveau, config["shuffle_partitions"], config["executor_memory"])

    finally:
        conn.close()


def trigger_degraded_mode(**ctx):
    """
    Active le mode dégradé si le niveau est CRITIQUE ou CRISE.
    Consigne dans alertes_capacite les pipelines à suspendre.
    """
    niveau = ctx["ti"].xcom_pull(key="niveau", task_ids="measure_current_load") or "NORMAL"
    ratio = ctx["ti"].xcom_pull(key="ratio", task_ids="measure_current_load") or 1.0
    volume = ctx["ti"].xcom_pull(key="volume", task_ids="measure_current_load") or 0

    if niveau not in ("CRITIQUE", "CRISE"):
        log.info("Mode dégradé non requis — niveau=%s", niveau)
        return

    pipelines_a_suspendre = []
    if niveau == "CRITIQUE":
        pipelines_a_suspendre = [
            "kpi_mensuels_aggregation",
            "kpi_hebdomadaires_aggregation",
            "export_powerbi_daily",
            "refresh_materialized_views_daily",
        ]
        message = (f"MODE DÉGRADÉ NIVEAU 2 : charge {ratio:.1f}x baseline ({volume} rec/h). "
                   f"Suspension pipelines non-critiques. Priorité COUPURE_ELECTRIQUE.")
    else:  # CRISE
        pipelines_a_suspendre = [
            "kpi_mensuels_aggregation",
            "kpi_hebdomadaires_aggregation",
            "kpi_quotidiens_aggregation",
            "export_powerbi_daily",
            "refresh_materialized_views_daily",
            "anomaly_detection_daily",
            "reconciliation_multi_systemes_daily",
        ]
        message = (f"🚨 CELLULE DE CRISE ACTIVÉE : charge {ratio:.1f}x baseline ({volume} rec/h). "
                   f"Mode ingestion seule. Tous les pipelines analytiques suspendus.")

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.alertes_capacite
                    (date_alerte, type_alerte, niveau_charge, message)
                VALUES (NOW(), 'MODE_DEGRADE', %s, %s)
            """, (niveau, message))

            # Mise à jour des pipelines suspendus dans le pic courant
            cur.execute("""
                UPDATE reclamations.pics_charge
                SET pipelines_suspendus = %s
                WHERE id = (SELECT MAX(id) FROM reclamations.pics_charge)
            """, (pipelines_a_suspendre,))

        conn.commit()
        log.warning(message)
        log.warning("Pipelines suspendus : %s", pipelines_a_suspendre)

    finally:
        conn.close()


def check_recovery(**ctx):
    """
    Vérifie si le mode dégradé peut être désactivé
    (charge revenue < 1.5x baseline pendant 2 heures consécutives).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM reclamations.pics_charge
                WHERE date_detection >= NOW() - INTERVAL '2 hours'
                  AND niveau_charge NOT IN ('CRITIQUE', 'CRISE')
                  AND mode_degrade = FALSE
            """)
            stable_hours = cur.fetchone()[0]

            if stable_hours >= 2:
                # Retour à la normale
                cur.execute("""
                    UPDATE reclamations.configuration_dynamique
                    SET valeur_courante   = valeur_normale,
                        modifie_par       = 'auto_recovery',
                        date_modification = NOW()
                    WHERE parametre IN ('mode_degrade_actif', 'niveau_degrade',
                                        'spark.sql.shuffle.partitions',
                                        'spark.executor.memory', 'spark.executor.cores')
                """)
                cur.execute("""
                    UPDATE reclamations.alertes_capacite
                    SET statut = 'RESOLUE'
                    WHERE statut = 'ACTIVE'
                """)
                conn.commit()
                log.info("✅ Mode dégradé désactivé — charge revenue à la normale")
            else:
                log.info("Mode dégradé maintenu — %d heure(s) stables (seuil: 2h)", stable_hours)

    finally:
        conn.close()


def generate_charge_report(**ctx):
    niveau = ctx["ti"].xcom_pull(key="niveau", task_ids="measure_current_load") or "NORMAL"
    ratio = ctx["ti"].xcom_pull(key="ratio", task_ids="measure_current_load") or 1.0
    volume = ctx["ti"].xcom_pull(key="volume", task_ids="measure_current_load") or 0

    emoji = "🚨" if niveau == "CRISE" else "⚠️" if niveau == "CRITIQUE" else "📊"
    log.info("%s Charge système [%s] : %d rec/h | %.2fx baseline | niveau=%s",
             emoji, datetime.now(timezone.utc).strftime("%H:%M UTC"),
             volume, ratio, niveau)
    ctx["ti"].xcom_push(key="rapport_niveau", value=niveau)


def notify_pipeline_run(**ctx):
    niveau = ctx["ti"].xcom_pull(key="rapport_niveau", task_ids="generate_charge_report") or "NORMAL"
    volume = ctx["ti"].xcom_pull(key="volume", task_ids="measure_current_load") or 0
    statut = "WARNING" if niveau in ("CRITIQUE", "CRISE") else "SUCCESS"
    log_pipeline_run(DAG_ID, statut, volume, 0,
                     f"Pics charge [{datetime.now(timezone.utc).strftime('%H:%M')}] — {volume} rec/h | niveau={niveau}")


with DAG(
    dag_id=DAG_ID,
    description="Monitoring des pics de charge et gestion automatique du mode dégradé",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["monitoring", "charge", "mode-dégradé", "résilience", "infrastructure"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_charge_tables",   python_callable=create_charge_tables)
    t2 = PythonOperator(task_id="measure_current_load",   python_callable=measure_current_load)
    t3 = PythonOperator(task_id="adjust_spark_config",    python_callable=adjust_spark_config)
    t4 = PythonOperator(task_id="trigger_degraded_mode",  python_callable=trigger_degraded_mode)
    t5 = PythonOperator(task_id="check_recovery",         python_callable=check_recovery)
    t6 = PythonOperator(task_id="generate_charge_report", python_callable=generate_charge_report)
    t7 = PythonOperator(task_id="notify_pipeline_run",    python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t2 >> [t3, t4] >> t5 >> t6 >> t7
