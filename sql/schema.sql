-- ============================================================================
-- SCHÉMA DE BASE DE DONNÉES : Plateforme de Traitement de Réclamations
-- ============================================================================
-- Base de données : reclamations_db
-- Version : 1.0
-- Date : Février 2025
-- ============================================================================

-- Création du schéma principal
CREATE SCHEMA IF NOT EXISTS reclamations;

-- ============================================================================
-- TABLE : reclamations_cleaned
-- Description : Stockage des réclamations après nettoyage et enrichissement
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.reclamations_cleaned (
    -- Identifiants
    id_reclamation VARCHAR(20) PRIMARY KEY,
    client_id VARCHAR(20) NOT NULL,
    
    -- Informations de base
    type_reclamation VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    canal VARCHAR(20) NOT NULL,
    priorite VARCHAR(20) NOT NULL,
    statut VARCHAR(20) NOT NULL,
    
    -- Localisation
    region VARCHAR(50) NOT NULL,
    departement VARCHAR(5) NOT NULL,
    adresse TEXT,
    code_postal VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    
    -- Références techniques
    reference_linky VARCHAR(50),
    
    -- Dates
    date_creation TIMESTAMP NOT NULL,
    date_premiere_reponse TIMESTAMP,
    date_cloture TIMESTAMP,
    
    -- Métriques calculées
    duree_traitement_heures INTEGER,
    delai_premiere_reponse_heures INTEGER,
    score_priorite INTEGER,
    
    -- Corrélations
    incident_lie VARCHAR(20),
    distance_incident_km DECIMAL(10, 2),
    est_recurrent BOOLEAN DEFAULT FALSE,
    nombre_reclamations_client INTEGER DEFAULT 1,
    
    -- Métadonnées
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_mise_a_jour TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT chk_priorite CHECK (priorite IN ('BASSE', 'NORMALE', 'HAUTE', 'CRITIQUE')),
    CONSTRAINT chk_statut CHECK (statut IN ('OUVERT', 'EN_COURS', 'RESOLU', 'CLOTURE', 'ESCALADE')),
    CONSTRAINT chk_canal CHECK (canal IN ('TELEPHONE', 'EMAIL', 'WEB', 'AGENCE')),
    CONSTRAINT chk_dates CHECK (date_creation <= COALESCE(date_premiere_reponse, date_creation)),
    CONSTRAINT chk_dates_cloture CHECK (date_creation <= COALESCE(date_cloture, date_creation))
);

-- ============================================================================
-- TABLE : incidents_reseau
-- Description : Incidents réseau géolocalisés pour corrélation
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.incidents_reseau (
    id_incident VARCHAR(20) PRIMARY KEY,
    type_incident VARCHAR(50) NOT NULL,
    date_incident TIMESTAMP NOT NULL,
    date_resolution TIMESTAMP,
    region VARCHAR(50) NOT NULL,
    departement VARCHAR(5) NOT NULL,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    clients_impactes INTEGER,
    description TEXT,
    
    -- Métadonnées
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_dates_incident CHECK (date_incident <= COALESCE(date_resolution, date_incident))
);

-- ============================================================================
-- TABLE : clients
-- Description : Informations clients et historique
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.clients (
    client_id VARCHAR(20) PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    telephone VARCHAR(20),
    date_premier_contact TIMESTAMP NOT NULL,
    region VARCHAR(50),
    
    -- Métriques clients
    nombre_total_reclamations INTEGER DEFAULT 0,
    nombre_reclamations_ouvertes INTEGER DEFAULT 0,
    score_satisfaction DECIMAL(3, 2),
    
    -- Métadonnées
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_mise_a_jour TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TABLE : kpis_daily
-- Description : KPIs calculés quotidiennement par région et type
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.kpis_daily (
    id SERIAL PRIMARY KEY,
    date_calcul DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    type_reclamation VARCHAR(50),
    
    -- Volumétrie
    nombre_reclamations_ouvertes INTEGER,
    nombre_reclamations_cloturees INTEGER,
    nombre_reclamations_en_cours INTEGER,
    
    -- Temps de traitement
    duree_moyenne_traitement_heures DECIMAL(10, 2),
    duree_mediane_traitement_heures DECIMAL(10, 2),
    delai_moyen_premiere_reponse_heures DECIMAL(10, 2),
    
    -- Qualité de service
    taux_respect_sla DECIMAL(5, 2),
    taux_reclamations_critiques DECIMAL(5, 2),
    taux_escalade DECIMAL(5, 2),
    
    -- Satisfaction
    score_satisfaction_moyen DECIMAL(3, 2),
    
    -- Métadonnées
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_kpi_date_region_type UNIQUE (date_calcul, region, type_reclamation)
);

-- ============================================================================
-- TABLE : anomalies_detected
-- Description : Détection automatique d'anomalies (pics, patterns inhabituels)
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.anomalies_detected (
    id SERIAL PRIMARY KEY,
    date_detection TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    type_anomalie VARCHAR(50) NOT NULL,
    
    -- Localisation de l'anomalie
    region VARCHAR(50),
    departement VARCHAR(5),
    type_reclamation VARCHAR(50),
    
    -- Métrique concernée
    metrique VARCHAR(100) NOT NULL,
    valeur_observee DECIMAL(15, 2),
    valeur_moyenne_historique DECIMAL(15, 2),
    ecart_type_historique DECIMAL(15, 2),
    z_score DECIMAL(10, 2),
    
    -- Sévérité
    severite VARCHAR(20) NOT NULL,
    description TEXT,
    
    -- Statut de traitement
    statut VARCHAR(20) DEFAULT 'NOUVEAU',
    date_resolution TIMESTAMP,
    
    CONSTRAINT chk_severite CHECK (severite IN ('FAIBLE', 'MOYENNE', 'HAUTE', 'CRITIQUE')),
    CONSTRAINT chk_statut_anomalie CHECK (statut IN ('NOUVEAU', 'EN_COURS', 'RESOLU', 'IGNORE'))
);

-- ============================================================================
-- TABLE : pipeline_logs
-- Description : Logs d'exécution des pipelines Spark
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.pipeline_logs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    
    -- Status
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    
    -- Métriques
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    
    -- Erreurs
    error_message TEXT,
    error_stacktrace TEXT,
    
    -- Métadonnées
    airflow_dag_id VARCHAR(100),
    airflow_task_id VARCHAR(100),
    airflow_run_id VARCHAR(255),
    
    CONSTRAINT chk_status_log CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED'))
);

-- ============================================================================
-- TABLE : data_quality_metrics
-- Description : Métriques de qualité des données
-- ============================================================================

CREATE TABLE IF NOT EXISTS reclamations.data_quality_metrics (
    id SERIAL PRIMARY KEY,
    date_controle TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    job_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    
    -- Complétude
    total_records INTEGER,
    records_with_nulls INTEGER,
    completeness_score DECIMAL(5, 2),
    
    -- Unicité
    duplicate_records INTEGER,
    uniqueness_score DECIMAL(5, 2),
    
    -- Validité
    invalid_records INTEGER,
    validity_score DECIMAL(5, 2),
    
    -- Cohérence
    inconsistent_records INTEGER,
    consistency_score DECIMAL(5, 2),
    
    -- Score global
    overall_quality_score DECIMAL(5, 2),
    
    -- Détails des problèmes
    quality_issues JSONB,
    
    -- Statut
    passed BOOLEAN,
    
    CONSTRAINT chk_quality_scores CHECK (
        completeness_score BETWEEN 0 AND 100 AND
        uniqueness_score BETWEEN 0 AND 100 AND
        validity_score BETWEEN 0 AND 100 AND
        consistency_score BETWEEN 0 AND 100 AND
        overall_quality_score BETWEEN 0 AND 100
    )
);

-- ============================================================================
-- VUES MATÉRIALISÉES
-- ============================================================================

-- Vue : Statistiques par région
CREATE MATERIALIZED VIEW IF NOT EXISTS reclamations.mv_stats_region AS
SELECT 
    region,
    COUNT(*) as total_reclamations,
    COUNT(CASE WHEN statut IN ('OUVERT', 'EN_COURS') THEN 1 END) as reclamations_en_cours,
    COUNT(CASE WHEN statut = 'CLOTURE' THEN 1 END) as reclamations_cloturees,
    AVG(duree_traitement_heures) as duree_moyenne_traitement,
    AVG(score_priorite) as score_priorite_moyen,
    COUNT(CASE WHEN est_recurrent = TRUE THEN 1 END) as reclamations_recurrentes
FROM reclamations.reclamations_cleaned
GROUP BY region;

-- Vue : Top 10 clients avec le plus de réclamations
CREATE MATERIALIZED VIEW IF NOT EXISTS reclamations.mv_top_clients_reclamants AS
SELECT 
    c.client_id,
    c.nom,
    c.prenom,
    c.region,
    COUNT(r.id_reclamation) as nombre_reclamations,
    AVG(r.duree_traitement_heures) as duree_moyenne_traitement,
    COUNT(CASE WHEN r.statut = 'ESCALADE' THEN 1 END) as reclamations_escaladees
FROM reclamations.clients c
LEFT JOIN reclamations.reclamations_cleaned r ON c.client_id = r.client_id
GROUP BY c.client_id, c.nom, c.prenom, c.region
ORDER BY nombre_reclamations DESC
LIMIT 100;

-- ============================================================================
-- COMMENTAIRES SUR LES TABLES
-- ============================================================================

COMMENT ON TABLE reclamations.reclamations_cleaned IS 'Table principale des réclamations nettoyées et enrichies';
COMMENT ON TABLE reclamations.incidents_reseau IS 'Incidents réseau pour corrélation avec les réclamations';
COMMENT ON TABLE reclamations.clients IS 'Informations clients et historique des interactions';
COMMENT ON TABLE reclamations.kpis_daily IS 'KPIs calculés quotidiennement pour le monitoring';
COMMENT ON TABLE reclamations.anomalies_detected IS 'Anomalies détectées automatiquement dans les données';
COMMENT ON TABLE reclamations.pipeline_logs IS 'Logs d''exécution des jobs Spark';
COMMENT ON TABLE reclamations.data_quality_metrics IS 'Métriques de qualité des données';

-- ============================================================================
-- FONCTIONS UTILITAIRES
-- ============================================================================

-- Fonction pour rafraîchir les vues matérialisées
CREATE OR REPLACE FUNCTION reclamations.refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW reclamations.mv_stats_region;
    REFRESH MATERIALIZED VIEW reclamations.mv_top_clients_reclamants;
END;
$$ LANGUAGE plpgsql;

-- Fonction pour nettoyer les vieux logs (>90 jours)
CREATE OR REPLACE FUNCTION reclamations.cleanup_old_logs()
RETURNS INTEGER AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    DELETE FROM reclamations.pipeline_logs 
    WHERE execution_date < CURRENT_DATE - INTERVAL '90 days';
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    RETURN rows_deleted;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- GRANTS (Permissions)
-- ============================================================================

GRANT USAGE ON SCHEMA reclamations TO airflow;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA reclamations TO airflow;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA reclamations TO airflow;
GRANT SELECT ON ALL TABLES IN SCHEMA reclamations TO PUBLIC;

-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================
