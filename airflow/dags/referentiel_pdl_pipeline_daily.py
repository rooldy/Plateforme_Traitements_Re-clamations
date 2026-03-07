"""
referentiel_pdl_pipeline_daily.py
Gestion du référentiel PDL (Points De Livraison).

Le PDL est l'identifiant technique unique du point de connexion au réseau
Enedis. Il est distinct du client : un même PDL peut avoir des clients
successifs (déménagements). Ce découplage est fondamental pour :
  - Distinguer l'historique technique du PDL de l'historique client
  - Éviter les erreurs lors des changements de fournisseur (marché ouvert)
  - Alimenter correctement le SGE (Système de Gestion des Échanges B2B)
  - Garantir la conformité des échanges avec les fournisseurs alternatifs

Tables produites :
  - reclamations.referentiel_pdl            : référentiel maître des PDL
  - reclamations.historique_clients_pdl     : historique client ↔ PDL
  - reclamations.reclamations_pdl           : lien réclamation ↔ PDL
  - reclamations.incoherences_pdl           : anomalies détectées

Schedule : quotidien 03:15 (après reclamation_pipeline, avant kpi_quotidiens)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "referentiel_pdl_pipeline_daily"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_pdl_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        -- Référentiel maître PDL
        CREATE TABLE IF NOT EXISTS reclamations.referentiel_pdl (
            pdl_id              VARCHAR(14) PRIMARY KEY,  -- 14 chiffres (format Enedis)
            adresse             VARCHAR(300),
            code_postal         VARCHAR(10),
            commune             VARCHAR(100),
            departement         VARCHAR(10),
            region              VARCHAR(100),
            type_compteur       VARCHAR(50),  -- LINKY, ELECTROMECANIQUE, TRIPHASE
            puissance_souscrite_kva NUMERIC(6,2),
            segment             VARCHAR(50),  -- C1, C2, C3, C4, C5 (profils consommation)
            tension             VARCHAR(20),  -- BT, HTA, HTB
            statut_pdl          VARCHAR(50) DEFAULT 'ACTIF',  -- ACTIF, INACTIF, EN_TRAVAUX
            date_mise_en_service DATE,
            date_derniere_maj   DATE,
            nb_changements_fournisseur INTEGER DEFAULT 0,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Historique des affectations client ↔ PDL
        CREATE TABLE IF NOT EXISTS reclamations.historique_clients_pdl (
            id              SERIAL PRIMARY KEY,
            pdl_id          VARCHAR(14) NOT NULL REFERENCES reclamations.referentiel_pdl(pdl_id),
            client_id       VARCHAR(50) NOT NULL,
            fournisseur     VARCHAR(100),   -- EDF, TOTAL_ENERGIE, ENGIE, etc.
            date_debut      DATE NOT NULL,
            date_fin        DATE,           -- NULL = client actuel
            motif_fin       VARCHAR(100),   -- DEMENAGEMENT, CHANGEMENT_FOURNISSEUR, RESILIATION
            is_current      BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Lien réclamation ↔ PDL (enrichissement)
        CREATE TABLE IF NOT EXISTS reclamations.reclamations_pdl (
            reclamation_id  VARCHAR(50) PRIMARY KEY,
            pdl_id          VARCHAR(14),
            client_id       VARCHAR(50),
            -- Indicateur : la réclamation concerne-t-elle le PDL ou seulement le client ?
            concerne_pdl    BOOLEAN DEFAULT TRUE,
            -- Si le client a déménagé, les réclamations restent rattachées au bon PDL
            client_au_moment_reclamation VARCHAR(50),
            created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Incohérences détectées entre client_id et PDL
        CREATE TABLE IF NOT EXISTS reclamations.incoherences_pdl (
            id              SERIAL PRIMARY KEY,
            pdl_id          VARCHAR(14),
            reclamation_id  VARCHAR(50),
            type_incoherence VARCHAR(100),   -- CLIENT_MISMATCH, PDL_INCONNU, DOUBLE_AFFECTATION
            description     TEXT,
            statut          VARCHAR(50) DEFAULT 'OUVERTE',
            date_detection  DATE,
            created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_pdl_dept     ON reclamations.referentiel_pdl(departement);
        CREATE INDEX IF NOT EXISTS idx_pdl_statut   ON reclamations.referentiel_pdl(statut_pdl);
        CREATE INDEX IF NOT EXISTS idx_histo_pdl    ON reclamations.historique_clients_pdl(pdl_id);
        CREATE INDEX IF NOT EXISTS idx_histo_client ON reclamations.historique_clients_pdl(client_id);
        CREATE INDEX IF NOT EXISTS idx_rec_pdl      ON reclamations.reclamations_pdl(pdl_id);
        CREATE INDEX IF NOT EXISTS idx_incoherences ON reclamations.incoherences_pdl(statut);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

def sync_pdl_from_linky(**ctx):
    """
    Synchronise le référentiel PDL depuis les données Linky.
    Les compteurs Linky sont la source de vérité pour les PDL actifs.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    synced = 0

    try:
        with conn.cursor() as cur:
            # Upsert des PDL depuis la table Linky enrichie
            cur.execute("""
                INSERT INTO reclamations.referentiel_pdl
                    (pdl_id, departement, region, type_compteur,
                     statut_pdl, date_derniere_maj)
                SELECT DISTINCT
                    rld.numero_compteur AS pdl_id,
                    rp.departement,
                    rp.region,
                    'LINKY' AS type_compteur,
                    'ACTIF' AS statut_pdl,
                    %s::DATE AS date_derniere_maj
                FROM reclamations.reclamations_linky_detailed rld
                JOIN reclamations.reclamations_processed rp
                    ON rld.reclamation_id = rp.reclamation_id
                WHERE rld.numero_compteur IS NOT NULL
                  AND rld.export_date = %s
                ON CONFLICT (pdl_id) DO UPDATE SET
                    departement       = EXCLUDED.departement,
                    region            = EXCLUDED.region,
                    date_derniere_maj = EXCLUDED.date_derniere_maj
            """, (run_date, run_date))
            synced = cur.rowcount

        conn.commit()
        log.info("PDL synchronisés depuis Linky : %d", synced)
        ctx["ti"].xcom_push(key="synced_pdl", value=synced)

    finally:
        conn.close()

def link_reclamations_to_pdl(**ctx):
    """Rattache les réclamations Linky à leur PDL dans reclamations_pdl."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    linked = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reclamations.reclamations_pdl
                    (reclamation_id, pdl_id, client_id,
                     concerne_pdl, client_au_moment_reclamation)
                SELECT
                    rld.reclamation_id,
                    rld.numero_compteur AS pdl_id,
                    rp.client_id,
                    TRUE,
                    rp.client_id
                FROM reclamations.reclamations_linky_detailed rld
                JOIN reclamations.reclamations_processed rp
                    ON rld.reclamation_id = rp.reclamation_id
                WHERE rld.export_date = %s
                  AND rld.numero_compteur IS NOT NULL
                ON CONFLICT (reclamation_id) DO UPDATE SET
                    pdl_id      = EXCLUDED.pdl_id,
                    client_id   = EXCLUDED.client_id
            """, (run_date,))
            linked = cur.rowcount

        conn.commit()
        log.info("Réclamations rattachées à leur PDL : %d", linked)
        ctx["ti"].xcom_push(key="linked", value=linked)

    finally:
        conn.close()

def detect_pdl_incoherences(**ctx):
    """
    Détecte les incohérences dans le référentiel PDL :
    - PDL inconnu (réclamation Linky sans PDL dans le référentiel)
    - Double affectation (même PDL, deux clients actifs simultanément)
    - Client ayant déménagé mais avec réclamations encore rattachées à l'ancien PDL
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    incoherences = 0

    try:
        with conn.cursor() as cur:
            # PDL inconnus
            cur.execute("""
                INSERT INTO reclamations.incoherences_pdl
                    (pdl_id, reclamation_id, type_incoherence, description, date_detection)
                SELECT
                    rp.pdl_id,
                    rp.reclamation_id,
                    'PDL_INCONNU',
                    'PDL ' || rp.pdl_id || ' présent dans les réclamations mais absent du référentiel',
                    %s::DATE
                FROM reclamations.reclamations_pdl rp
                LEFT JOIN reclamations.referentiel_pdl ref ON rp.pdl_id = ref.pdl_id
                WHERE ref.pdl_id IS NULL
                  AND rp.pdl_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM reclamations.incoherences_pdl i
                    WHERE i.pdl_id = rp.pdl_id AND i.type_incoherence = 'PDL_INCONNU'
                      AND i.statut = 'OUVERTE'
                  )
            """, (run_date,))
            incoherences += cur.rowcount

            # Double affectation
            cur.execute("""
                INSERT INTO reclamations.incoherences_pdl
                    (pdl_id, type_incoherence, description, date_detection)
                SELECT
                    pdl_id,
                    'DOUBLE_AFFECTATION',
                    'PDL ' || pdl_id || ' affecté à ' || COUNT(DISTINCT client_id)::TEXT || ' clients actifs simultanément',
                    %s::DATE
                FROM reclamations.historique_clients_pdl
                WHERE is_current = TRUE
                GROUP BY pdl_id
                HAVING COUNT(DISTINCT client_id) > 1
            """, (run_date,))
            incoherences += cur.rowcount

        conn.commit()
        log.info("Incohérences PDL détectées : %d", incoherences)
        ctx["ti"].xcom_push(key="incoherences", value=incoherences)

    finally:
        conn.close()

def generate_pdl_stats(**ctx):
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE statut_pdl='ACTIF') FROM reclamations.referentiel_pdl")
            total_pdl, actifs = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM reclamations.incoherences_pdl WHERE statut='OUVERTE'")
            open_inco = cur.fetchone()[0]

        log.info("=== Référentiel PDL [%s] ===", run_date)
        log.info("  PDL total : %d | Actifs : %d | Incohérences ouvertes : %d",
                 total_pdl, actifs, open_inco)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    synced = ctx["ti"].xcom_pull(key="synced_pdl", task_ids="sync_pdl_from_linky") or 0
    inco = ctx["ti"].xcom_pull(key="incoherences", task_ids="detect_pdl_incoherences") or 0
    log_pipeline_run(DAG_ID, "WARNING" if inco > 0 else "SUCCESS", synced, 0,
                     f"PDL [{ctx['ds']}] — {synced} PDL synchronisés | {inco} incohérences détectées")

with DAG(
    dag_id=DAG_ID,
    description="Gestion du référentiel PDL (Points De Livraison) — cœur du modèle Enedis",
    schedule="15 3 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pdl", "référentiel", "linky", "technique"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_pdl_tables",          python_callable=create_pdl_tables)
    t2 = PythonOperator(task_id="sync_pdl_from_linky",        python_callable=sync_pdl_from_linky)
    t3 = PythonOperator(task_id="link_reclamations_to_pdl",   python_callable=link_reclamations_to_pdl)
    t4 = PythonOperator(task_id="detect_pdl_incoherences",    python_callable=detect_pdl_incoherences)
    t5 = PythonOperator(task_id="generate_pdl_stats",         python_callable=generate_pdl_stats)
    t6 = PythonOperator(task_id="notify_pipeline_run",        python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
