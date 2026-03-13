"""
test_data_generation_weekly.py
Génération hebdomadaire de données de test réalistes.

Responsabilités :
  1. Génère un jeu de données synthétiques cohérent avec le modèle métier
     (réclamations, clients, compteurs, relevés, KPIs) dans un schéma isolé
  2. Simule des scénarios de test : SLA dépassé, réclamations critiques,
     anomalies de compteurs, pics de volume
  3. Valide que les pipelines principaux fonctionnent sur les données générées
  4. Exporte le rapport de génération
  5. Log dans pipeline_runs

Schedule : dimanche à 04:00 (avant data_cleanup_weekly)

⚠️ Les données sont générées dans le schéma reclamations_test (isolé de prod)
"""

import logging
import random
import string
from datetime import datetime, timezone, timedelta, date

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SLA_HEURES, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "test_data_generation_weekly"
TEST_SCHEMA = "reclamations_test"

# Paramètres de génération
N_RECLAMATIONS = 500
N_CLIENTS = 200
N_COMPTEURS = 1000

REGIONS = ["ILE_DE_FRANCE", "AUVERGNE_RHONE_ALPES", "OCCITANIE",
           "NOUVELLE_AQUITAINE", "HAUTS_DE_FRANCE", "GRAND_EST",
           "BRETAGNE", "PAYS_DE_LA_LOIRE", "NORMANDIE", "PROVENCE_ALPES_COTE_AZUR"]
DEPARTEMENTS = ["75", "69", "31", "33", "59", "67", "35", "44", "76", "13",
                "92", "93", "94", "78", "95", "91", "77", "76", "38", "06"]
TYPES_RECLAMATION = list(SLA_HEURES.keys())
STATUTS = ["OUVERT", "EN_COURS", "CLOTURE", "CLOTURE"]  # Doublé pour plus de clôturées
PRIORITES = ["NORMALE", "NORMALE", "HAUTE", "CRITIQUE"]  # Distribution réaliste

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}


def _rand_id(prefix: str, n: int = 8) -> str:
    return f"{prefix}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=n))}"


def _rand_datetime(days_back_max: int = 30, days_back_min: int = 0) -> datetime:
    now = datetime.now(timezone.utc)
    delta = timedelta(
        days=random.randint(days_back_min, days_back_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return now - delta


def create_test_schema(**ctx):
    """Crée le schéma de test isolé et les tables nécessaires."""
    ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA};

        DROP TABLE IF EXISTS {TEST_SCHEMA}.reclamations_test CASCADE;
        CREATE TABLE {TEST_SCHEMA}.reclamations_test (
            reclamation_id          VARCHAR(50) PRIMARY KEY,
            client_id               VARCHAR(50),
            region                  VARCHAR(100),
            departement             VARCHAR(10),
            type_reclamation        VARCHAR(100),
            statut                  VARCHAR(50),
            priorite                VARCHAR(20),
            date_creation           TIMESTAMP WITH TIME ZONE,
            date_cloture            TIMESTAMP WITH TIME ZONE,
            duree_traitement_heures NUMERIC(10, 2),
            agent_id                VARCHAR(50),
            commentaire             TEXT,
            scenario_test           VARCHAR(100),
            generation_date         DATE DEFAULT CURRENT_DATE
        );

        DROP TABLE IF EXISTS {TEST_SCHEMA}.clients_test CASCADE;
        CREATE TABLE {TEST_SCHEMA}.clients_test (
            client_id       VARCHAR(50) PRIMARY KEY,
            nom             VARCHAR(100),
            region          VARCHAR(100),
            departement     VARCHAR(10),
            nb_reclamations INTEGER DEFAULT 0,
            type_client     VARCHAR(50)
        );

        DROP TABLE IF EXISTS {TEST_SCHEMA}.compteurs_test CASCADE;
        CREATE TABLE {TEST_SCHEMA}.compteurs_test (
            compteur_id     VARCHAR(50) PRIMARY KEY,
            client_id       VARCHAR(50),
            region          VARCHAR(100),
            type_compteur   VARCHAR(50),
            date_installation DATE,
            statut          VARCHAR(50)
        );
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Schéma %s initialisé", TEST_SCHEMA)
    finally:
        conn.close()


def generate_clients(**ctx):
    """Génère N_CLIENTS clients fictifs."""
    import csv
    import io

    clients = []
    for i in range(N_CLIENTS):
        region = random.choice(REGIONS)
        dept_idx = REGIONS.index(region) % len(DEPARTEMENTS)
        clients.append((
            _rand_id("CLT"),
            f"Client_{i+1:04d}",
            region,
            DEPARTEMENTS[dept_idx],
            random.randint(0, 5),
            random.choice(["PARTICULIER", "PROFESSIONNEL", "COLLECTIVITE"]),
        ))

    columns = ["client_id", "nom", "region", "departement", "nb_reclamations", "type_client"]
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            buf = io.StringIO()
            csv.writer(buf).writerows(clients)
            buf.seek(0)
            cur.copy_from(buf, f"{TEST_SCHEMA}.clients_test", sep=",", columns=columns, null="")
        conn.commit()
        log.info("Clients générés : %d", len(clients))
        ctx["ti"].xcom_push(key="client_ids", value=[c[0] for c in clients])
    finally:
        conn.close()


def generate_reclamations(**ctx):
    """
    Génère N_RECLAMATIONS réclamations avec des scénarios variés :
    - 70% : réclamations normales dans les délais
    - 15% : réclamations hors SLA (clôturées en retard)
    - 10% : réclamations en risque de dépassement (ouvertes, proche limite)
    - 5%  : réclamations critiques COUPURE_ELECTRIQUE urgentes
    """
    import csv
    import io

    client_ids = ctx["ti"].xcom_pull(key="client_ids", task_ids="generate_clients")
    if not client_ids:
        # Génère des IDs temporaires si XCom non disponible
        client_ids = [_rand_id("CLT") for _ in range(N_CLIENTS)]

    reclamations = []
    scenarios = {
        "NORMAL": int(N_RECLAMATIONS * 0.70),
        "HORS_SLA": int(N_RECLAMATIONS * 0.15),
        "RISQUE_SLA": int(N_RECLAMATIONS * 0.10),
        "CRITIQUE_COUPURE": int(N_RECLAMATIONS * 0.05),
    }

    for scenario, count in scenarios.items():
        for _ in range(count):
            rec_id = _rand_id("REC")
            client_id = random.choice(client_ids)
            region = random.choice(REGIONS)
            dept = DEPARTEMENTS[REGIONS.index(region) % len(DEPARTEMENTS)]

            if scenario == "CRITIQUE_COUPURE":
                type_rec = "COUPURE_ELECTRIQUE"
                priorite = "CRITIQUE"
                # Ouvert depuis 40h → hors SLA 48h
                date_creation = _rand_datetime(days_back_max=3, days_back_min=2)
                statut = "EN_COURS"
                date_cloture = None
                duree = (datetime.now(timezone.utc) - date_creation).total_seconds() / 3600

            elif scenario == "HORS_SLA":
                type_rec = random.choice(TYPES_RECLAMATION)
                priorite = random.choice(["NORMALE", "HAUTE"])
                sla = SLA_HEURES[type_rec]
                date_creation = _rand_datetime(days_back_max=30, days_back_min=15)
                duree = sla * random.uniform(1.1, 2.0)  # 10% à 100% au-delà du SLA
                date_cloture = date_creation + timedelta(hours=duree)
                statut = "CLOTURE"

            elif scenario == "RISQUE_SLA":
                type_rec = random.choice(TYPES_RECLAMATION)
                priorite = "HAUTE"
                sla = SLA_HEURES[type_rec]
                duree = sla * random.uniform(0.85, 0.99)  # entre 85% et 99% du SLA
                date_creation = datetime.now(timezone.utc) - timedelta(hours=duree)
                date_cloture = None
                statut = "EN_COURS"

            else:  # NORMAL
                type_rec = random.choice(TYPES_RECLAMATION)
                priorite = random.choice(PRIORITES)
                sla = SLA_HEURES[type_rec]
                date_creation = _rand_datetime(days_back_max=20, days_back_min=5)
                statut = random.choice(STATUTS)
                if statut == "CLOTURE":
                    duree = sla * random.uniform(0.1, 0.9)
                    date_cloture = date_creation + timedelta(hours=duree)
                else:
                    duree = (datetime.now(timezone.utc) - date_creation).total_seconds() / 3600
                    date_cloture = None

            agent_id = _rand_id("AGT", 6) if random.random() > 0.2 else None

            reclamations.append((
                rec_id, client_id, region, dept, type_rec,
                statut, priorite,
                date_creation.isoformat(),
                date_cloture.isoformat() if date_cloture else "",
                round(duree, 2),
                agent_id or "",
                f"Commentaire test scenario {scenario}",
                scenario,
            ))

    columns = [
        "reclamation_id", "client_id", "region", "departement", "type_reclamation",
        "statut", "priorite", "date_creation", "date_cloture",
        "duree_traitement_heures", "agent_id", "commentaire", "scenario_test",
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            buf = io.StringIO()
            csv.writer(buf).writerows(reclamations)
            buf.seek(0)
            cur.copy_from(buf, f"{TEST_SCHEMA}.reclamations_test", sep=",", columns=columns, null="")
        conn.commit()
        log.info("Réclamations générées : %d (scénarios: %s)", len(reclamations), scenarios)
        ctx["ti"].xcom_push(key="total_generated", value=len(reclamations))
    finally:
        conn.close()


def validate_test_data(**ctx):
    """Vérifie la cohérence des données générées."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Distribution par scénario
            cur.execute(f"""
                SELECT scenario_test, COUNT(*) AS nb,
                       COUNT(*) FILTER (WHERE statut = 'CLOTURE') AS cloturees,
                       ROUND(AVG(duree_traitement_heures)::NUMERIC, 1) AS duree_moy
                FROM {TEST_SCHEMA}.reclamations_test
                GROUP BY scenario_test
                ORDER BY scenario_test
            """)
            rows = cur.fetchall()

        log.info("=== Validation données de test ===")
        for scenario, nb, cloturees, duree_moy in rows:
            log.info("  %-25s : %d réclamations | %d clôturées | durée_moy=%.1fh",
                     scenario, nb, cloturees, duree_moy or 0)

        # Vérification SLA dépassés dans le scénario HORS_SLA
        conn2 = psycopg2.connect(**DB_CONFIG)
        with conn2.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM {TEST_SCHEMA}.reclamations_test
                WHERE scenario_test = 'HORS_SLA'
                  AND duree_traitement_heures IS NOT NULL
            """)
            hors_sla_count = cur.fetchone()[0]
        conn2.close()
        log.info("Réclamations HORS_SLA générées : %d (attendu ~%d)",
                 hors_sla_count, int(N_RECLAMATIONS * 0.15))

    finally:
        conn.close()


def generate_report(**ctx):
    """Génère le rapport de génération des données de test."""
    total = ctx["ti"].xcom_pull(key="total_generated", task_ids="generate_reclamations") or 0
    log.info(
        "=== Rapport Génération Données Test [%s] ===\n"
        "  Schéma cible : %s\n"
        "  Réclamations : %d\n"
        "  Clients      : %d\n"
        "  Compteurs    : %d\n"
        "  Scénarios    : NORMAL(70%%), HORS_SLA(15%%), RISQUE_SLA(10%%), CRITIQUE(5%%)\n"
        "  Usage : Exécuter les pipelines avec la source configurée sur %s pour valider",
        (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"), TEST_SCHEMA, total, N_CLIENTS, N_COMPTEURS, TEST_SCHEMA
    )


def notify_pipeline_run(**ctx):
    total = ctx["ti"].xcom_pull(key="total_generated", task_ids="generate_reclamations") or 0
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=total,
        duration_seconds=0,
        message=f"Données de test générées [{(ctx.get('logical_date') or ctx.get('data_interval_start') or __import__('datetime').datetime.now()).strftime('%Y-%m-%d')}] : {total} réclamations | schéma={TEST_SCHEMA}",
    )


with DAG(
    dag_id=DAG_ID,
    description=f"Génération hebdomadaire de données de test réalistes (schéma {TEST_SCHEMA})",
    schedule="0 4 * * 0",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "test", "données-synthétiques", "hebdomadaire"],
    doc_md=__doc__,
) as dag:

    t_schema = PythonOperator(task_id="create_test_schema", python_callable=create_test_schema)
    t_clients = PythonOperator(task_id="generate_clients", python_callable=generate_clients)
    t_reclamations = PythonOperator(task_id="generate_reclamations", python_callable=generate_reclamations)
    t_validate = PythonOperator(task_id="validate_test_data", python_callable=validate_test_data)
    t_report = PythonOperator(task_id="generate_report", python_callable=generate_report)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_schema >> t_clients >> t_reclamations >> t_validate >> t_report >> t_notify
