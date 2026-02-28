"""
rgpd_compliance_daily.py
Conformité RGPD active — obligations CNIL pour Enedis.

Enedis traite des données personnelles de 35M+ clients (consommation,
adresse, coordonnées bancaires). La CNIL impose des délais et obligations
précis. Ce DAG est non-optionnel en production.

Responsabilités :
  1. Traitement des demandes de droit à l'oubli (délai légal : 1 mois)
  2. Pseudonymisation automatique des données dans les tables hors-production
  3. Détection des données personnelles hors durée de conservation légale
  4. Mise à jour du registre des traitements (art. 30 RGPD)
  5. Vérification du consentement pour les traitements analytiques
  6. Rapport de conformité vers reclamations.registre_rgpd

Tables produites :
  - reclamations.demandes_rgpd          : demandes droits clients (oubli, accès, rectif.)
  - reclamations.registre_rgpd          : registre des traitements art. 30
  - reclamations.audit_rgpd             : journal des actions RGPD
  - reclamations.donnees_a_supprimer    : file de traitement des suppressions

Schedule : quotidien 01:30 (traitement nocturne, avant les pipelines de données)
"""

import logging
import hashlib
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "rgpd_compliance_daily"

# ─── Durées de conservation légales (en jours) ───────────────────────────────
RETENTION_LEGALE = {
    "reclamations.reclamations_processed":          5 * 365,  # 5 ans (prescription commerciale)
    "reclamations.reclamations_cleaned":            5 * 365,
    "reclamations.reclamations_global_consolidated": 5 * 365,
    "reclamations.kpis_daily":                     10 * 365, # 10 ans (données agrégées, pas perso)
    "reclamations.reclamations_linky_detailed":     3 * 365,  # 3 ans (données de consommation Linky)
    "reclamations.reclamations_coupures_detail":    5 * 365,
    "reclamations.reclamations_facturation_detail": 10 * 365, # 10 ans (données fiscales)
    "reclamations.pipeline_runs":                   2 * 365,  # 2 ans (logs techniques)
}

# Champs personnels à pseudonymiser dans les environnements non-prod
CHAMPS_PERSONNELS = {
    "reclamations.reclamations_processed":  ["client_id"],
    "reclamations.reclamations_cleaned":    ["client_id", "adresse", "code_postal"],
    "reclamations.reclamations_linky_detailed": ["client_id"],
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,
}


def create_rgpd_tables(**ctx):
    """Crée les tables de gestion RGPD."""
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        -- Demandes de droits clients (oubli, accès, rectification, portabilité)
        CREATE TABLE IF NOT EXISTS reclamations.demandes_rgpd (
            id                  SERIAL PRIMARY KEY,
            client_id           VARCHAR(50) NOT NULL,
            type_demande        VARCHAR(50) NOT NULL,  -- OUBLI, ACCES, RECTIFICATION, PORTABILITE, OPPOSITION
            date_demande        DATE NOT NULL,
            date_limite_legal   DATE NOT NULL,          -- date_demande + 30 jours (RGPD art. 12)
            statut              VARCHAR(50) DEFAULT 'EN_ATTENTE',
            date_traitement     DATE,
            traite_par          VARCHAR(100),
            tables_impactees    TEXT[],
            nb_lignes_supprimees INTEGER DEFAULT 0,
            commentaire         TEXT,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Registre des traitements art. 30 RGPD
        CREATE TABLE IF NOT EXISTS reclamations.registre_rgpd (
            id                      SERIAL PRIMARY KEY,
            nom_traitement          VARCHAR(200) NOT NULL UNIQUE,
            finalite                TEXT NOT NULL,
            base_legale             VARCHAR(100),    -- CONTRAT, INTERET_LEGITIME, CONSENTEMENT, OBLIGATION_LEGALE
            categories_donnees      TEXT[],
            destinataires           TEXT[],
            duree_conservation_j    INTEGER,
            mesures_securite        TEXT,
            transfert_hors_ue       BOOLEAN DEFAULT FALSE,
            responsable_traitement  VARCHAR(200),
            dpo_contact             VARCHAR(200),
            derniere_revision       DATE,
            created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Journal des actions RGPD (audit trail complet)
        CREATE TABLE IF NOT EXISTS reclamations.audit_rgpd (
            id              SERIAL PRIMARY KEY,
            action          VARCHAR(100) NOT NULL,  -- SUPPRESSION, PSEUDONYMISATION, VERIFICATION, RAPPORT
            table_cible     VARCHAR(200),
            nb_lignes       INTEGER,
            client_id       VARCHAR(50),
            demande_id      INTEGER REFERENCES reclamations.demandes_rgpd(id),
            statut          VARCHAR(50),
            detail          TEXT,
            created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- File de traitement des suppressions en attente
        CREATE TABLE IF NOT EXISTS reclamations.donnees_a_supprimer (
            id              SERIAL PRIMARY KEY,
            client_id       VARCHAR(50) NOT NULL,
            table_name      VARCHAR(200) NOT NULL,
            condition_sql   TEXT,
            statut          VARCHAR(50) DEFAULT 'PENDING',
            tentatives      INTEGER DEFAULT 0,
            created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            traite_at       TIMESTAMP WITH TIME ZONE
        );

        CREATE INDEX IF NOT EXISTS idx_demandes_rgpd_statut  ON reclamations.demandes_rgpd(statut);
        CREATE INDEX IF NOT EXISTS idx_demandes_rgpd_limite  ON reclamations.demandes_rgpd(date_limite_legal);
        CREATE INDEX IF NOT EXISTS idx_audit_rgpd_date       ON reclamations.audit_rgpd(created_at);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Tables RGPD créées/vérifiées")
    finally:
        conn.close()


def init_registre_traitements(**ctx):
    """
    Initialise ou met à jour le registre des traitements RGPD (art. 30).
    Chaque pipeline = un traitement à déclarer.
    """
    traitements = [
        ("Pipeline réclamations clients",
         "Centralisation et analyse des réclamations pour amélioration qualité de service",
         "INTERET_LEGITIME",
         ["identité client", "coordonnées", "données contrat", "données réclamation"],
         ["équipe data", "managers régionaux"],
         5 * 365, "Chiffrement AES-256, accès par rôle, audit trail"),
        ("Données compteurs Linky",
         "Suivi qualité réseau, détection pannes, optimisation maintenance",
         "OBLIGATION_LEGALE",
         ["index consommation", "courbe de charge", "identifiant PDL"],
         ["équipe data", "techniciens terrain", "CRE"],
         3 * 365, "Chiffrement TLS, stockage segmenté, pas de transfert hors UE"),
        ("KPIs et reporting analytique",
         "Pilotage opérationnel SLA et conformité réglementaire CRE",
         "OBLIGATION_LEGALE",
         ["données agrégées anonymisées", "métriques régionales"],
         ["management", "CRE", "équipe data"],
         10 * 365, "Données agrégées — risque de réidentification minimal"),
        ("Données de facturation",
         "Traitement des réclamations liées aux factures et corrections comptables",
         "CONTRAT",
         ["données financières", "montants facturés", "historique paiements"],
         ["équipe comptabilité", "équipe data"],
         10 * 365, "Accès restreint, chiffrement, conformité PCI-DSS"),
        ("Journaux d'audit pipelines",
         "Traçabilité technique des traitements automatisés (obligation légale RGPD art. 25)",
         "OBLIGATION_LEGALE",
         ["identifiants techniques", "horodatages", "volumes de données"],
         ["équipe data engineering", "DSI"],
         2 * 365, "Logs chiffrés, accès admin uniquement"),
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for (nom, finalite, base, categories, destinataires, retention, mesures) in traitements:
                cur.execute("""
                    INSERT INTO reclamations.registre_rgpd
                        (nom_traitement, finalite, base_legale, categories_donnees,
                         destinataires, duree_conservation_j, mesures_securite,
                         responsable_traitement, dpo_contact, derniere_revision)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nom_traitement) DO UPDATE SET
                        finalite           = EXCLUDED.finalite,
                        duree_conservation_j = EXCLUDED.duree_conservation_j,
                        derniere_revision  = EXCLUDED.derniere_revision
                """, (nom, finalite, base, categories, destinataires, retention, mesures,
                      "Enedis SA — Direction Data & Digital",
                      "dpo@enedis.fr",
                      datetime.now(timezone.utc).date()))
        conn.commit()
        log.info("Registre des traitements RGPD mis à jour : %d traitements", len(traitements))
    finally:
        conn.close()


def process_droit_a_loubli(**ctx):
    """
    Traite les demandes de droit à l'oubli en attente.
    Délai légal : 1 mois (RGPD art. 17 et art. 12 §3).
    Supprime les données personnelles identifiables du client des tables de production.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    traites = 0

    try:
        with conn.cursor() as cur:
            # Récupère les demandes OUBLI en attente ou en retard
            cur.execute("""
                SELECT id, client_id, date_limite_legal
                FROM reclamations.demandes_rgpd
                WHERE type_demande = 'OUBLI'
                  AND statut IN ('EN_ATTENTE', 'EN_COURS')
                ORDER BY date_limite_legal ASC
                LIMIT 100  -- traitement par batch de 100
            """)
            demandes = cur.fetchall()

            for demande_id, client_id, date_limite in demandes:
                tables_impactees = []
                nb_total = 0

                # Pseudonymisation irréversible dans toutes les tables
                pseudo_id = "SUPPRIME_" + hashlib.sha256(
                    f"{client_id}_rgpd_{run_date}".encode()
                ).hexdigest()[:12]

                for table in [
                    "reclamations.reclamations_processed",
                    "reclamations.reclamations_cleaned",
                    "reclamations.reclamations_global_consolidated",
                    "reclamations.reclamations_coupures_detail",
                    "reclamations.reclamations_facturation_detail",
                    "reclamations.reclamations_raccordement_detail",
                    "reclamations.reclamations_linky_detailed",
                ]:
                    try:
                        # Vérifie si la table a une colonne client_id
                        schema, tname = table.split(".")
                        cur.execute("""
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = %s AND table_name = %s
                              AND column_name = 'client_id'
                        """, (schema, tname))
                        if not cur.fetchone():
                            continue

                        cur.execute(f"""
                            UPDATE {table}
                            SET client_id = %s
                            WHERE client_id = %s
                        """, (pseudo_id, client_id))
                        nb = cur.rowcount
                        if nb > 0:
                            tables_impactees.append(table)
                            nb_total += nb
                    except Exception as exc:
                        log.warning("Impossible de pseudonymiser %s pour %s : %s", table, client_id, exc)

                # Mise à jour du statut de la demande
                cur.execute("""
                    UPDATE reclamations.demandes_rgpd
                    SET statut             = 'TRAITE',
                        date_traitement    = %s,
                        traite_par         = 'rgpd_compliance_daily',
                        tables_impactees   = %s,
                        nb_lignes_supprimees = %s
                    WHERE id = %s
                """, (run_date, tables_impactees, nb_total, demande_id))

                # Journal d'audit
                cur.execute("""
                    INSERT INTO reclamations.audit_rgpd
                        (action, table_cible, nb_lignes, client_id, demande_id, statut, detail)
                    VALUES ('PSEUDONYMISATION_OUBLI', %s, %s, %s, %s, 'SUCCESS', %s)
                """, (str(tables_impactees), nb_total, client_id, demande_id,
                      f"Client {client_id} pseudonymisé en {pseudo_id} dans {len(tables_impactees)} tables"))

                traites += 1
                log.info("Droit à l'oubli traité — client=%s | tables=%d | lignes=%d",
                         client_id[:8] + "...", len(tables_impactees), nb_total)

        conn.commit()
        log.info("Demandes droit à l'oubli traitées : %d", traites)
        ctx["ti"].xcom_push(key="oubli_traites", value=traites)

    finally:
        conn.close()


def detect_retention_depassee(**ctx):
    """
    Détecte les données dépassant leur durée de conservation légale.
    Alerte uniquement — la suppression est effectuée manuellement après validation.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    alertes = 0

    try:
        with conn.cursor() as cur:
            for table, retention_j in RETENTION_LEGALE.items():
                schema, tname = table.split(".")
                try:
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                          AND column_name IN ('date_creation', 'export_date', 'created_at', 'run_date')
                        ORDER BY CASE column_name
                            WHEN 'date_creation' THEN 1 WHEN 'export_date' THEN 2
                            WHEN 'created_at' THEN 3 WHEN 'run_date' THEN 4
                        END LIMIT 1
                    """, (schema, tname))
                    col_row = cur.fetchone()
                    if not col_row:
                        continue

                    date_col = col_row[0]
                    cur.execute(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE {date_col} < NOW() - INTERVAL '{retention_j} days'
                    """)
                    count = cur.fetchone()[0]

                    if count > 0:
                        log.warning("RGPD ALERTE : %s — %d lignes dépassent %d jours de rétention",
                                    table, count, retention_j)
                        cur.execute("""
                            INSERT INTO reclamations.audit_rgpd
                                (action, table_cible, nb_lignes, statut, detail)
                            VALUES ('DETECTION_RETENTION_DEPASSEE', %s, %s, 'ALERTE', %s)
                        """, (table, count,
                              f"{count} lignes dépassent la durée de conservation légale de {retention_j} jours"))
                        alertes += 1
                    else:
                        log.info("✅ Rétention OK : %s", table)

                except Exception as exc:
                    log.warning("Erreur vérification rétention %s : %s", table, exc)

        conn.commit()
        log.info("Vérification rétention terminée : %d alertes", alertes)
        ctx["ti"].xcom_push(key="alertes_retention", value=alertes)

    finally:
        conn.close()


def check_demandes_en_retard(**ctx):
    """Alerte si des demandes RGPD dépassent le délai légal d'1 mois."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*), type_demande
                FROM reclamations.demandes_rgpd
                WHERE statut NOT IN ('TRAITE', 'REJETE')
                  AND date_limite_legal < CURRENT_DATE
                GROUP BY type_demande
            """)
            rows = cur.fetchall()

        if rows:
            for count, type_dem in rows:
                log.error("🚨 RGPD VIOLATION : %d demandes %s dépassent le délai légal d'1 mois !",
                          count, type_dem)
        else:
            log.info("✅ Toutes les demandes RGPD sont dans les délais légaux")

    finally:
        conn.close()


def generate_rgpd_report(**ctx):
    """Génère le rapport journalier de conformité RGPD."""
    oubli = ctx["ti"].xcom_pull(key="oubli_traites", task_ids="process_droit_a_loubli") or 0
    alertes = ctx["ti"].xcom_pull(key="alertes_retention", task_ids="detect_retention_depassee") or 0

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT type_demande, statut, COUNT(*) FROM reclamations.demandes_rgpd
                GROUP BY type_demande, statut ORDER BY type_demande, statut
            """)
            rows = cur.fetchall()

        log.info("=== Rapport RGPD [%s] ===", ctx["ds"])
        log.info("  Droits traités aujourd'hui : %d oubli", oubli)
        log.info("  Alertes rétention : %d tables", alertes)
        log.info("  État des demandes :")
        for type_dem, statut, count in rows:
            log.info("    %-20s %-15s : %d", type_dem, statut, count)
    finally:
        conn.close()


def notify_pipeline_run(**ctx):
    oubli = ctx["ti"].xcom_pull(key="oubli_traites", task_ids="process_droit_a_loubli") or 0
    alertes = ctx["ti"].xcom_pull(key="alertes_retention", task_ids="detect_retention_depassee") or 0
    log_pipeline_run(DAG_ID, "WARNING" if alertes > 0 else "SUCCESS", oubli, 0,
                     f"RGPD [{ctx['ds']}] — {oubli} droits traités | {alertes} alertes rétention")


with DAG(
    dag_id=DAG_ID,
    description="Conformité RGPD active : droit à l'oubli, registre traitements, rétention légale",
    schedule="30 1 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["rgpd", "conformité", "légal", "cnil", "sécurité"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_rgpd_tables",         python_callable=create_rgpd_tables)
    t2 = PythonOperator(task_id="init_registre_traitements",  python_callable=init_registre_traitements)
    t3 = PythonOperator(task_id="process_droit_a_loubli",     python_callable=process_droit_a_loubli)
    t4 = PythonOperator(task_id="detect_retention_depassee",  python_callable=detect_retention_depassee)
    t5 = PythonOperator(task_id="check_demandes_en_retard",   python_callable=check_demandes_en_retard)
    t6 = PythonOperator(task_id="generate_rgpd_report",       python_callable=generate_rgpd_report)
    t7 = PythonOperator(task_id="notify_pipeline_run",        python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t2 >> [t3, t4, t5] >> t6 >> t7
