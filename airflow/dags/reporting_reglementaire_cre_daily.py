"""
reporting_reglementaire_cre_daily.py
Reporting réglementaire CRE (Commission de Régulation de l'Énergie).

La CRE impose à Enedis des indicateurs légaux précis, distincts des KPIs
internes. Une erreur de calcul ou un retard de transmission expose Enedis
à des sanctions financières.

Indicateurs produits :
  - SAIDI  : System Average Interruption Duration Index (durée moy. coupure/client)
  - SAIFI  : System Average Interruption Frequency Index (fréq. moy. coupure/client)
  - CAIDI  : Customer Average Interruption Duration Index (SAIDI / SAIFI)
  - Délais légaux de raccordement (art. L.342-1 Code Énergie)
  - Délais légaux de mise en service (art. D.121-18)
  - Taux de réclamations fondées (obligation art. R.125-2)

Tables produites :
  - reclamations.kpis_reglementaires_cre      : indicateurs CRE quotidiens
  - reclamations.rapport_cre_mensuel          : rapport consolidé mensuel
  - reclamations.alertes_cre                  : dépassements seuils légaux

Schedule : quotidien 08:30 (après consolidation globale)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, log_pipeline_run

log = logging.getLogger(__name__)
DAG_ID = "reporting_reglementaire_cre_daily"

# ─── Seuils légaux CRE (valeurs de référence réglementaires) ─────────────────
SEUILS_CRE = {
    "SAIDI_MAX_ANNUEL_MINUTES":     60,   # 60 min/an max (réseau BT urbain)
    "SAIFI_MAX_ANNUEL":             1.5,  # 1.5 interruption/an/client max
    "DELAI_RACCORDEMENT_MAX_JOURS": 30,   # art. L.342-1 : 30 jours calendaires
    "DELAI_MISE_EN_SERVICE_JOURS":  5,    # art. D.121-18 : 5 jours ouvrés
    "TAUX_RECLAMATIONS_FONDEES_MIN": 0.0, # toute réclamation fondée doit être traitée
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

def create_cre_tables(**ctx):
    """Crée les tables de reporting réglementaire CRE."""
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;

        -- Indicateurs CRE quotidiens
        CREATE TABLE IF NOT EXISTS reclamations.kpis_reglementaires_cre (
            id                          SERIAL PRIMARY KEY,
            date_calcul                 DATE NOT NULL,
            region                      VARCHAR(100),
            departement                 VARCHAR(10),
            -- SAIDI/SAIFI
            saidi_minutes               NUMERIC(10, 4),
            saifi_index                 NUMERIC(10, 6),
            caidi_minutes               NUMERIC(10, 4),
            nb_clients_affectes         INTEGER,
            nb_interruptions            INTEGER,
            duree_totale_interruption_h NUMERIC(12, 2),
            -- Délais légaux raccordement
            nb_raccordements_periode    INTEGER,
            nb_raccordements_hors_delai INTEGER,
            taux_respect_delai_raccordement NUMERIC(5, 2),
            delai_moyen_raccordement_j  NUMERIC(8, 2),
            -- Délais mise en service
            nb_mises_en_service         INTEGER,
            nb_mises_en_service_hors_delai INTEGER,
            taux_respect_delai_mes      NUMERIC(5, 2),
            -- Réclamations fondées
            nb_reclamations_total       INTEGER,
            nb_reclamations_fondees     INTEGER,
            taux_reclamations_fondees   NUMERIC(5, 2),
            -- Conformité globale
            conforme_cre                BOOLEAN,
            motif_non_conformite        TEXT,
            created_at                  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (date_calcul, region, departement)
        );

        -- Rapport mensuel CRE (agrégation pour transmission officielle)
        CREATE TABLE IF NOT EXISTS reclamations.rapport_cre_mensuel (
            id                          SERIAL PRIMARY KEY,
            annee                       INTEGER NOT NULL,
            mois                        INTEGER NOT NULL,
            region                      VARCHAR(100),
            saidi_cumule_minutes        NUMERIC(10, 4),
            saifi_cumule                NUMERIC(10, 6),
            caidi_minutes               NUMERIC(10, 4),
            taux_respect_raccordement   NUMERIC(5, 2),
            taux_respect_mes            NUMERIC(5, 2),
            taux_reclamations_fondees   NUMERIC(5, 2),
            conforme_cre                BOOLEAN,
            date_transmission_cre       DATE,
            valide_par                  VARCHAR(100),
            created_at                  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (annee, mois, region)
        );

        -- Alertes dépassement seuils CRE
        CREATE TABLE IF NOT EXISTS reclamations.alertes_cre (
            id                  SERIAL PRIMARY KEY,
            date_alerte         DATE NOT NULL,
            region              VARCHAR(100),
            type_alerte         VARCHAR(100) NOT NULL,
            valeur_mesuree      NUMERIC(12, 4),
            seuil_legal         NUMERIC(12, 4),
            depassement_pct     NUMERIC(8, 2),
            description         TEXT,
            statut              VARCHAR(50) DEFAULT 'OUVERTE',
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_kpi_cre_date    ON reclamations.kpis_reglementaires_cre(date_calcul);
        CREATE INDEX IF NOT EXISTS idx_kpi_cre_region  ON reclamations.kpis_reglementaires_cre(region);
        CREATE INDEX IF NOT EXISTS idx_alertes_cre     ON reclamations.alertes_cre(date_alerte, type_alerte);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Tables CRE créées/vérifiées")
    finally:
        conn.close()

def compute_saidi_saifi(**ctx):
    """
    Calcule SAIDI et SAIFI depuis les réclamations COUPURE_ELECTRIQUE.
    
    SAIDI = Σ(durée interruption × nb clients affectés) / nb clients total
    SAIFI = Σ(nb clients affectés par interruption) / nb clients total
    CAIDI = SAIDI / SAIFI
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    rows_inserted = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH clients_totaux AS (
                    -- Nombre total de clients actifs par région (approximé depuis reclamations)
                    SELECT region, COUNT(DISTINCT client_id) AS nb_clients_region
                    FROM reclamations.reclamations_processed
                    WHERE date_creation >= NOW() - INTERVAL '365 days'
                    GROUP BY region
                ),
                coupures_jour AS (
                    SELECT
                        rc.region,
                        rc.departement,
                        COUNT(DISTINCT rc.reclamation_id)  AS nb_interruptions,
                        SUM(COALESCE(rc.duree_heures, 0))  AS duree_totale_h,
                        COUNT(DISTINCT rc.client_id)       AS nb_clients_affectes
                    FROM reclamations.reclamations_global_consolidated rc
                    WHERE rc.type_reclamation = 'COUPURE_ELECTRIQUE'
                      AND rc.export_date = %s
                    GROUP BY rc.region, rc.departement
                )
                INSERT INTO reclamations.kpis_reglementaires_cre
                    (date_calcul, region, departement,
                     saidi_minutes, saifi_index, caidi_minutes,
                     nb_clients_affectes, nb_interruptions, duree_totale_interruption_h,
                     nb_reclamations_total, conforme_cre)
                SELECT
                    %s::DATE,
                    c.region,
                    c.departement,
                    -- SAIDI en minutes = (durée totale en heures × 60 × nb_clients) / nb_clients_total
                    ROUND(
                        CASE WHEN ct.nb_clients_region > 0
                        THEN (c.duree_totale_h * 60.0 * c.nb_clients_affectes) / ct.nb_clients_region
                        ELSE 0 END::NUMERIC, 4
                    ) AS saidi_minutes,
                    -- SAIFI = nb_clients_affectés / nb_clients_total
                    ROUND(
                        CASE WHEN ct.nb_clients_region > 0
                        THEN c.nb_clients_affectes::NUMERIC / ct.nb_clients_region
                        ELSE 0 END::NUMERIC, 6
                    ) AS saifi_index,
                    -- CAIDI = SAIDI / SAIFI (si SAIFI > 0)
                    ROUND(
                        CASE WHEN c.nb_clients_affectes > 0
                        THEN (c.duree_totale_h * 60.0)
                        ELSE 0 END::NUMERIC, 4
                    ) AS caidi_minutes,
                    c.nb_clients_affectes,
                    c.nb_interruptions,
                    c.duree_totale_h,
                    c.nb_interruptions,
                    TRUE  -- conformité évaluée dans la tâche suivante
                FROM coupures_jour c
                LEFT JOIN clients_totaux ct ON c.region = ct.region
                ON CONFLICT (date_calcul, region, departement) DO UPDATE SET
                    saidi_minutes              = EXCLUDED.saidi_minutes,
                    saifi_index                = EXCLUDED.saifi_index,
                    caidi_minutes              = EXCLUDED.caidi_minutes,
                    nb_clients_affectes        = EXCLUDED.nb_clients_affectes,
                    nb_interruptions           = EXCLUDED.nb_interruptions,
                    duree_totale_interruption_h = EXCLUDED.duree_totale_interruption_h
            """, (run_date, run_date))

            rows_inserted = cur.rowcount
        conn.commit()
        log.info("SAIDI/SAIFI calculés : %d régions × départements", rows_inserted)
        ctx["ti"].xcom_push(key="saidi_rows", value=rows_inserted)

    finally:
        conn.close()

def compute_delais_legaux(**ctx):
    """Calcule les indicateurs de délais légaux raccordement et mise en service."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            # Raccordements : SLA légal 30 jours calendaires
            cur.execute("""
                UPDATE reclamations.kpis_reglementaires_cre kpi
                SET
                    nb_raccordements_periode    = stats.total_racc,
                    nb_raccordements_hors_delai = stats.hors_delai,
                    taux_respect_delai_raccordement = ROUND(
                        CASE WHEN stats.total_racc > 0
                        THEN 100.0 * (stats.total_racc - stats.hors_delai) / stats.total_racc
                        ELSE 100 END::NUMERIC, 2),
                    delai_moyen_raccordement_j  = stats.delai_moyen
                FROM (
                    SELECT
                        region,
                        departement,
                        COUNT(*)                                          AS total_racc,
                        COUNT(*) FILTER (WHERE duree_heures > 30 * 24)   AS hors_delai,
                        ROUND(AVG(duree_heures / 24.0)::NUMERIC, 2)      AS delai_moyen
                    FROM reclamations.reclamations_raccordement_detail
                    WHERE export_date = %s
                    GROUP BY region, departement
                ) stats
                WHERE kpi.date_calcul = %s
                  AND kpi.region      = stats.region
                  AND kpi.departement = stats.departement
            """, (run_date, run_date))

            # Mise en service : SLA légal 5 jours ouvrés (~7 jours calendaires)
            cur.execute("""
                UPDATE reclamations.kpis_reglementaires_cre kpi
                SET
                    nb_mises_en_service            = stats.total_mes,
                    nb_mises_en_service_hors_delai = stats.hors_delai,
                    taux_respect_delai_mes         = ROUND(
                        CASE WHEN stats.total_mes > 0
                        THEN 100.0 * (stats.total_mes - stats.hors_delai) / stats.total_mes
                        ELSE 100 END::NUMERIC, 2)
                FROM (
                    SELECT
                        region,
                        departement,
                        COUNT(*)                                         AS total_mes,
                        COUNT(*) FILTER (WHERE duree_heures > 7 * 24)   AS hors_delai
                    FROM reclamations.reclamations_processed
                    WHERE DATE(date_creation) = %s
                      AND type_reclamation    = 'RACCORDEMENT_RESEAU'
                      AND statut              = 'CLOTURE'
                    GROUP BY region, departement
                ) stats
                WHERE kpi.date_calcul = %s
                  AND kpi.region      = stats.region
                  AND kpi.departement = stats.departement
            """, (run_date, run_date, run_date))

        conn.commit()
        log.info("Délais légaux raccordement et MES mis à jour")

    finally:
        conn.close()

def compute_reclamations_fondees(**ctx):
    """
    Calcule le taux de réclamations fondées.
    Une réclamation est 'fondée' si elle a été clôturée en faveur du client
    (présence d'un geste commercial, correction de facture, etc.).
    Ici approximé par : réclamation résolue dans les délais SLA.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE reclamations.kpis_reglementaires_cre kpi
                SET
                    nb_reclamations_fondees   = stats.fondees,
                    taux_reclamations_fondees = ROUND(
                        CASE WHEN stats.total > 0
                        THEN 100.0 * stats.fondees / stats.total
                        ELSE 0 END::NUMERIC, 2)
                FROM (
                    SELECT
                        region,
                        COALESCE(departement, 'INCONNU') AS departement,
                        COUNT(*)                                               AS total,
                        COUNT(*) FILTER (WHERE sla_respect = TRUE)            AS fondees
                    FROM reclamations.reclamations_global_consolidated
                    WHERE export_date = %s
                      AND statut      = 'CLOTURE'
                    GROUP BY region, departement
                ) stats
                WHERE kpi.date_calcul = %s
                  AND kpi.region      = stats.region
                  AND kpi.departement = stats.departement
            """, (run_date, run_date))
        conn.commit()
    finally:
        conn.close()

def evaluate_cre_compliance(**ctx):
    """
    Évalue la conformité réglementaire et génère les alertes CRE.
    Toute non-conformité est enregistrée dans alertes_cre.
    """
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)
    alertes = 0

    try:
        with conn.cursor() as cur:
            # Annualisation du SAIDI (SAIDI jour × 365 pour projection annuelle)
            cur.execute("""
                SELECT region, departement,
                       saidi_minutes, saifi_index,
                       taux_respect_delai_raccordement,
                       taux_respect_delai_mes
                FROM reclamations.kpis_reglementaires_cre
                WHERE date_calcul = %s
            """, (run_date,))
            rows = cur.fetchall()

            for region, dept, saidi, saifi, taux_racc, taux_mes in rows:
                motifs = []
                saidi_annualise = (saidi or 0) * 365

                if saidi_annualise > SEUILS_CRE["SAIDI_MAX_ANNUEL_MINUTES"]:
                    motifs.append(f"SAIDI annualisé={saidi_annualise:.1f}min > seuil={SEUILS_CRE['SAIDI_MAX_ANNUEL_MINUTES']}min")
                    cur.execute("""
                        INSERT INTO reclamations.alertes_cre
                            (date_alerte, region, type_alerte, valeur_mesuree, seuil_legal, depassement_pct, description)
                        VALUES (%s, %s, 'SAIDI_DEPASSE', %s, %s, %s, %s)
                    """, (run_date, region, saidi_annualise,
                          SEUILS_CRE["SAIDI_MAX_ANNUEL_MINUTES"],
                          round(100 * (saidi_annualise - SEUILS_CRE["SAIDI_MAX_ANNUEL_MINUTES"]) / SEUILS_CRE["SAIDI_MAX_ANNUEL_MINUTES"], 2),
                          f"SAIDI annualisé {saidi_annualise:.1f} min/an > seuil légal {SEUILS_CRE['SAIDI_MAX_ANNUEL_MINUTES']} min/an"))
                    alertes += 1

                if (taux_racc or 100) < 90:  # moins de 90% de raccordements dans les délais
                    motifs.append(f"Taux raccordement={taux_racc:.1f}% < 90%")
                    cur.execute("""
                        INSERT INTO reclamations.alertes_cre
                            (date_alerte, region, type_alerte, valeur_mesuree, seuil_legal, depassement_pct, description)
                        VALUES (%s, %s, 'DELAI_RACCORDEMENT_DEPASSE', %s, %s, %s, %s)
                    """, (run_date, region, taux_racc, 90.0,
                          round(100 * (90.0 - (taux_racc or 0)) / 90.0, 2),
                          f"Taux respect délai raccordement {taux_racc:.1f}% < seuil 90%"))
                    alertes += 1

                # Mise à jour conformité
                cur.execute("""
                    UPDATE reclamations.kpis_reglementaires_cre
                    SET conforme_cre = %s, motif_non_conformite = %s
                    WHERE date_calcul = %s AND region = %s AND departement = %s
                """, (len(motifs) == 0, "; ".join(motifs) if motifs else None,
                      run_date, region, dept or "INCONNU"))

        conn.commit()
        log.info("Évaluation CRE terminée : %d alertes générées", alertes)
        ctx["ti"].xcom_push(key="alertes_count", value=alertes)

    finally:
        conn.close()

def generate_cre_report(**ctx):
    """Génère le rapport CRE quotidien dans les logs (base pour export mensuel)."""
    run_date = ctx["ds"]
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    region,
                    ROUND(AVG(saidi_minutes)::NUMERIC, 3)              AS saidi_moy,
                    ROUND(AVG(saifi_index)::NUMERIC, 6)                AS saifi_moy,
                    ROUND(AVG(taux_respect_delai_raccordement)::NUMERIC, 1) AS taux_racc,
                    ROUND(AVG(taux_respect_delai_mes)::NUMERIC, 1)     AS taux_mes,
                    COUNT(*) FILTER (WHERE NOT conforme_cre)           AS non_conforme
                FROM reclamations.kpis_reglementaires_cre
                WHERE date_calcul = %s
                GROUP BY region ORDER BY non_conforme DESC, region
            """, (run_date,))
            rows = cur.fetchall()

        log.info("=== Rapport CRE Réglementaire [%s] ===", run_date)
        log.info("  %-25s %10s %12s %12s %10s %12s",
                 "Région", "SAIDI(min)", "SAIFI", "Taux Racc%", "Taux MES%", "Non-conforme")
        for region, saidi, saifi, taux_racc, taux_mes, nc in rows:
            status = "🚨" if nc > 0 else "✅"
            log.info("  %s %-23s %9.3f %11.6f %11.1f%% %9.1f%% %s",
                     status, region or "N/A", saidi or 0, saifi or 0,
                     taux_racc or 100, taux_mes or 100, nc)
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    alertes = ctx["ti"].xcom_pull(key="alertes_count", task_ids="evaluate_cre_compliance") or 0
    rows = ctx["ti"].xcom_pull(key="saidi_rows", task_ids="compute_saidi_saifi") or 0
    log_pipeline_run(DAG_ID, "WARNING" if alertes > 0 else "SUCCESS", rows, 0,
                     f"Reporting CRE [{ctx['ds']}] — {rows} lignes | {alertes} alerte(s) réglementaire(s)")

with DAG(
    dag_id=DAG_ID,
    description="Indicateurs réglementaires CRE : SAIDI, SAIFI, délais légaux raccordement et MES",
    schedule="30 8 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["réglementaire", "cre", "saidi", "saifi", "légal"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="create_cre_tables",          python_callable=create_cre_tables)
    t2 = PythonOperator(task_id="compute_saidi_saifi",         python_callable=compute_saidi_saifi)
    t3 = PythonOperator(task_id="compute_delais_legaux",       python_callable=compute_delais_legaux)
    t4 = PythonOperator(task_id="compute_reclamations_fondees",python_callable=compute_reclamations_fondees)
    t5 = PythonOperator(task_id="evaluate_cre_compliance",     python_callable=evaluate_cre_compliance)
    t6 = PythonOperator(task_id="generate_cre_report",         python_callable=generate_cre_report)
    t7 = PythonOperator(task_id="notify_pipeline_run",         python_callable=notify_pipeline_run, trigger_rule="all_done")

    t1 >> t2 >> t5 >> t6 >> t7
