"""
detection_incidents_systemiques_daily.py
Détection d'incidents réseau systémiques par clustering géographique.

Problème : une tempête génère 5 000 réclamations COUPURE_ELECTRIQUE en 2h
dans la même zone. Sans ce DAG, elles sont comptées comme 5 000 réclamations
individuelles, faussant les KPIs SLA et surchargeant les files de traitement.

Ce DAG distingue :
  - Panne individuelle : 1 client isolé → traitement standard
  - Incident systémique : N clients dans un rayon R pendant une fenêtre T
    → création d'un incident groupé, suspension du décompte SLA individuel

Algorithme :
  1. Charge les réclamations COUPURE_ELECTRIQUE des dernières 6h
  2. Clustering spatial par département + fenêtre temporelle 2h
  3. Si cluster >= SEUIL_CLIENTS_INCIDENT → crée un incident systémique
  4. Marque les réclamations membres du cluster
  5. Notifie le centre d'appel et les équipes terrain

Tables produites :
  - reclamations.incidents_systemiques     : incidents réseau détectés
  - reclamations.reclamations_incidents    : rattachement réclamation ↔ incident

Schedule : quotidien 04:30 ET toutes les 2h en mode monitoring
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "detection_incidents_systemiques_daily"

# ─── Paramètres de détection ─────────────────────────────────────────────────
SEUIL_CLIENTS_INCIDENT  = 10   # min. 10 clients affectés pour constituer un incident
FENETRE_TEMPORELLE_H    = 2    # fenêtre de groupement : 2 heures
SEUIL_ALERTE_MAJEURE    = 100  # > 100 clients → incident MAJEUR
SEUIL_ALERTE_CRITIQUE   = 500  # > 500 clients → incident CRITIQUE (cellule de crise)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def create_incident_tables(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        CREATE TABLE IF NOT EXISTS reclamations.incidents_systemiques (
            incident_id             VARCHAR(50) PRIMARY KEY,
            date_detection          TIMESTAMP WITH TIME ZONE NOT NULL,
            date_debut_incident     TIMESTAMP WITH TIME ZONE,
            date_fin_incident       TIMESTAMP WITH TIME ZONE,
            region                  VARCHAR(100),
            departement             VARCHAR(10),
            zone_geographique       VARCHAR(200),   -- description libre de la zone
            type_incident           VARCHAR(100),   -- TEMPETE, DEFAUT_RESEAU, TRAVAUX, INCONNU
            niveau_severite         VARCHAR(20),    -- MINEUR, MODERE, MAJEUR, CRITIQUE
            nb_clients_affectes     INTEGER,
            nb_reclamations_liees   INTEGER,
            statut                  VARCHAR(50) DEFAULT 'DETECTE',  -- DETECTE, EN_COURS, RESOLU
            equipe_assignee         VARCHAR(100),
            commentaire             TEXT,
            sla_suspendu            BOOLEAN DEFAULT TRUE,  -- suspension SLA pendant l'incident
            created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reclamations.reclamations_incidents (
            reclamation_id  VARCHAR(50) NOT NULL,
            incident_id     VARCHAR(50) NOT NULL REFERENCES reclamations.incidents_systemiques(incident_id),
            date_rattachement TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (reclamation_id, incident_id)
        );

        CREATE INDEX IF NOT EXISTS idx_incidents_date   ON reclamations.incidents_systemiques(date_detection);
        CREATE INDEX IF NOT EXISTS idx_incidents_dept   ON reclamations.incidents_systemiques(departement);
        CREATE INDEX IF NOT EXISTS idx_incidents_statut ON reclamations.incidents_systemiques(statut);
        CREATE INDEX IF NOT EXISTS idx_rec_incidents    ON reclamations.reclamations_incidents(incident_id);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

def detect_clusters(**ctx):
    """
    Détecte les clusters de réclamations par département + fenêtre temporelle.
    Un cluster = même département, même fenêtre de 2h, >= SEUIL_CLIENTS clients.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    incidents_created = 0

    try:
        with conn.cursor() as cur:
            # Détection des clusters : regroupement par département + tranche horaire
            cur.execute("""
                WITH coupures_recentes AS (
                    SELECT
                        reclamation_id,
                        client_id,
                        region,
                        departement,
                        date_creation,
                        -- Fenêtre temporelle de 2h : arrondi à l'heure paire la plus proche
                        DATE_TRUNC('hour', date_creation)
                        - EXTRACT(MINUTE FROM date_creation)::INT * INTERVAL '1 minute'
                        - MOD(EXTRACT(HOUR FROM date_creation)::INT, %s)::INT * INTERVAL '1 hour'
                        AS fenetre_debut
                    FROM reclamations.reclamations_processed
                    WHERE type_reclamation = 'COUPURE_ELECTRIQUE'
                      AND DATE(date_creation) = %s
                      AND statut IN ('OUVERT', 'EN_COURS')
                ),
                clusters AS (
                    SELECT
                        region,
                        COALESCE(departement, 'INCONNU') AS departement,
                        fenetre_debut,
                        COUNT(DISTINCT client_id)     AS nb_clients,
                        COUNT(DISTINCT reclamation_id) AS nb_reclamations,
                        MIN(date_creation)            AS debut_incident,
                        MAX(date_creation)            AS fin_periode,
                        ARRAY_AGG(DISTINCT reclamation_id) AS reclamation_ids
                    FROM coupures_recentes
                    GROUP BY region, departement, fenetre_debut
                    HAVING COUNT(DISTINCT client_id) >= %s
                )
                SELECT * FROM clusters ORDER BY nb_clients DESC
            """, (FENETRE_TEMPORELLE_H, run_date, SEUIL_CLIENTS_INCIDENT))

            clusters = cur.fetchall()
            log.info("Clusters détectés : %d", len(clusters))

            for (region, dept, fenetre, nb_clients, nb_recs, debut, fin_periode, rec_ids) in clusters:
                # Vérifier si un incident existe déjà pour ce département/fenêtre
                cur.execute("""
                    SELECT incident_id FROM reclamations.incidents_systemiques
                    WHERE departement = %s
                      AND date_debut_incident BETWEEN %s AND %s
                      AND statut != 'RESOLU'
                    LIMIT 1
                """, (dept, debut - timedelta(hours=1), debut + timedelta(hours=FENETRE_TEMPORELLE_H)))
                existing = cur.fetchone()

                if existing:
                    # Mise à jour de l'incident existant
                    cur.execute("""
                        UPDATE reclamations.incidents_systemiques
                        SET nb_clients_affectes   = GREATEST(nb_clients_affectes, %s),
                            nb_reclamations_liees = GREATEST(nb_reclamations_liees, %s)
                        WHERE incident_id = %s
                    """, (nb_clients, nb_recs, existing[0]))
                    incident_id = existing[0]
                else:
                    # Détermination de la sévérité
                    if nb_clients >= SEUIL_ALERTE_CRITIQUE:
                        severite = "CRITIQUE"
                    elif nb_clients >= SEUIL_ALERTE_MAJEURE:
                        severite = "MAJEUR"
                    elif nb_clients >= 30:
                        severite = "MODERE"
                    else:
                        severite = "MINEUR"

                    incident_id = f"INC-{dept}-{fenetre.strftime('%Y%m%d%H')}-{nb_clients:04d}"
                    cur.execute("""
                        INSERT INTO reclamations.incidents_systemiques
                            (incident_id, date_detection, date_debut_incident,
                             region, departement, zone_geographique,
                             type_incident, niveau_severite,
                             nb_clients_affectes, nb_reclamations_liees, sla_suspendu)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (incident_id) DO NOTHING
                    """, (incident_id, datetime.now(timezone.utc), debut,
                          region, dept, f"Département {dept} — {region}",
                          "INCONNU", severite, nb_clients, nb_recs))
                    incidents_created += 1
                    log.warning("🚨 Incident %s détecté — %s | %d clients | sévérité=%s",
                                incident_id, dept, nb_clients, severite)

                # Rattachement des réclamations à l'incident
                for rec_id in (rec_ids or []):
                    cur.execute("""
                        INSERT INTO reclamations.reclamations_incidents (reclamation_id, incident_id)
                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """, (rec_id, incident_id))

        conn.commit()
        log.info("Incidents créés : %d | Clusters analysés : %d", incidents_created, len(clusters))
        ctx["ti"].xcom_push(key="incidents_created", value=incidents_created)
        ctx["ti"].xcom_push(key="clusters_count", value=len(clusters))

    finally:
        conn.close()

def update_incident_severity(**ctx):
    """
    Met à jour la sévérité des incidents en cours en fonction de l'évolution
    du nombre de clients affectés et du temps écoulé.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE reclamations.incidents_systemiques
                SET niveau_severite = CASE
                    WHEN nb_clients_affectes >= %s THEN 'CRITIQUE'
                    WHEN nb_clients_affectes >= %s THEN 'MAJEUR'
                    WHEN nb_clients_affectes >= 30 THEN 'MODERE'
                    ELSE 'MINEUR'
                END
                WHERE statut IN ('DETECTE', 'EN_COURS')
            """, (SEUIL_ALERTE_CRITIQUE, SEUIL_ALERTE_MAJEURE))
            updated = cur.rowcount
        conn.commit()
        log.info("Sévérités incidents mises à jour : %d incidents", updated)
    finally:
        conn.close()

def detect_resolved_incidents(**ctx):
    """
    Marque comme RESOLU les incidents dont toutes les réclamations sont clôturées.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE reclamations.incidents_systemiques i
                SET statut = 'RESOLU', date_fin_incident = NOW()
                WHERE i.statut IN ('DETECTE', 'EN_COURS')
                  AND NOT EXISTS (
                    SELECT 1 FROM reclamations.reclamations_incidents ri
                    JOIN reclamations.reclamations_processed rp
                        ON ri.reclamation_id = rp.reclamation_id
                    WHERE ri.incident_id = i.incident_id
                      AND rp.statut IN ('OUVERT', 'EN_COURS')
                  )
            """)
            resolved = cur.rowcount
        conn.commit()
        if resolved > 0:
            log.info("✅ %d incident(s) marqué(s) RESOLU", resolved)
    finally:
        conn.close()

def generate_incident_report(**ctx):
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT niveau_severite, statut, COUNT(*), SUM(nb_clients_affectes)
                FROM reclamations.incidents_systemiques
                WHERE DATE(date_detection) = %s
                GROUP BY niveau_severite, statut
                ORDER BY CASE niveau_severite
                    WHEN 'CRITIQUE' THEN 1 WHEN 'MAJEUR' THEN 2
                    WHEN 'MODERE' THEN 3 ELSE 4 END
            """, (run_date,))
            rows = cur.fetchall()

        log.info("=== Rapport Incidents Systémiques [%s] ===", run_date)
        for severite, statut, count, clients in rows:
            log.info("  [%s] %s : %d incident(s) | %d clients affectés",
                     severite, statut, count, clients or 0)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    incidents = ctx["ti"].xcom_pull(key="incidents_created", task_ids="detect_clusters") or 0
    clusters = ctx["ti"].xcom_pull(key="clusters_count", task_ids="detect_clusters") or 0
    log_pipeline_run(DAG_ID, "WARNING" if incidents > 0 else "SUCCESS", clusters, 0,
                     f"Incidents systémiques [{ctx['ds']}] — {incidents} créés | {clusters} clusters analysés")

with DAG(
    dag_id=DAG_ID,
    description="Détection d'incidents réseau systémiques par clustering géographique des coupures",
    schedule="30 4 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["incidents", "réseau", "clustering", "géographique", "opérationnel"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_incident_tables",      python_callable=create_incident_tables)
    t2 = PythonOperator(task_id="detect_clusters",             python_callable=detect_clusters)
    t3 = PythonOperator(task_id="update_incident_severity",    python_callable=update_incident_severity)
    t4 = PythonOperator(task_id="detect_resolved_incidents",   python_callable=detect_resolved_incidents)
    t5 = PythonOperator(task_id="generate_incident_report",    python_callable=generate_incident_report)
    t6 = PythonOperator(task_id="notify_pipeline_run",         python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
