-- ============================================================================
-- INDEX ET OPTIMISATIONS POSTGRESQL
-- ============================================================================
-- Base de données : reclamations_db
-- Description : Index stratégiques pour optimiser les performances des requêtes
-- ============================================================================

-- ============================================================================
-- INDEX SUR reclamations_cleaned
-- ============================================================================

-- Index sur les colonnes les plus requêtées
CREATE INDEX IF NOT EXISTS idx_reclamations_date_creation 
ON reclamations.reclamations_cleaned(date_creation DESC);

CREATE INDEX IF NOT EXISTS idx_reclamations_statut 
ON reclamations.reclamations_cleaned(statut);

CREATE INDEX IF NOT EXISTS idx_reclamations_priorite 
ON reclamations.reclamations_cleaned(priorite);

CREATE INDEX IF NOT EXISTS idx_reclamations_type 
ON reclamations.reclamations_cleaned(type_reclamation);

CREATE INDEX IF NOT EXISTS idx_reclamations_region 
ON reclamations.reclamations_cleaned(region);

CREATE INDEX IF NOT EXISTS idx_reclamations_client 
ON reclamations.reclamations_cleaned(client_id);

-- Index composite pour les requêtes analytiques courantes
CREATE INDEX IF NOT EXISTS idx_reclamations_region_type_date 
ON reclamations.reclamations_cleaned(region, type_reclamation, date_creation DESC);

CREATE INDEX IF NOT EXISTS idx_reclamations_statut_priorite 
ON reclamations.reclamations_cleaned(statut, priorite);

-- Index géospatial pour les recherches de proximité
CREATE INDEX IF NOT EXISTS idx_reclamations_geoloc 
ON reclamations.reclamations_cleaned(latitude, longitude);

-- Index sur les dates pour les requêtes temporelles
CREATE INDEX IF NOT EXISTS idx_reclamations_date_cloture 
ON reclamations.reclamations_cleaned(date_cloture)
WHERE date_cloture IS NOT NULL;

-- Index partiel pour les réclamations en cours
CREATE INDEX IF NOT EXISTS idx_reclamations_en_cours 
ON reclamations.reclamations_cleaned(date_creation, priorite)
WHERE statut IN ('OUVERT', 'EN_COURS');

-- Index pour les réclamations récurrentes
CREATE INDEX IF NOT EXISTS idx_reclamations_recurrentes 
ON reclamations.reclamations_cleaned(client_id, date_creation)
WHERE est_recurrent = TRUE;

-- ============================================================================
-- INDEX SUR incidents_reseau
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_incidents_date 
ON reclamations.incidents_reseau(date_incident DESC);

CREATE INDEX IF NOT EXISTS idx_incidents_region 
ON reclamations.incidents_reseau(region);

CREATE INDEX IF NOT EXISTS idx_incidents_type 
ON reclamations.incidents_reseau(type_incident);

CREATE INDEX IF NOT EXISTS idx_incidents_geoloc 
ON reclamations.incidents_reseau(latitude, longitude);

-- Index composite pour corrélation temporelle et géographique
CREATE INDEX IF NOT EXISTS idx_incidents_region_date 
ON reclamations.incidents_reseau(region, date_incident DESC);

-- ============================================================================
-- INDEX SUR clients
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_clients_region 
ON reclamations.clients(region);

CREATE INDEX IF NOT EXISTS idx_clients_nb_reclamations 
ON reclamations.clients(nombre_total_reclamations DESC);

CREATE INDEX IF NOT EXISTS idx_clients_email 
ON reclamations.clients(email);

-- ============================================================================
-- INDEX SUR kpis_daily
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_kpis_date_region 
ON reclamations.kpis_daily(date_calcul DESC, region);

CREATE INDEX IF NOT EXISTS idx_kpis_region_type 
ON reclamations.kpis_daily(region, type_reclamation);

-- ============================================================================
-- INDEX SUR anomalies_detected
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_anomalies_date_detection 
ON reclamations.anomalies_detected(date_detection DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_statut 
ON reclamations.anomalies_detected(statut)
WHERE statut = 'NOUVEAU';

CREATE INDEX IF NOT EXISTS idx_anomalies_severite 
ON reclamations.anomalies_detected(severite, date_detection DESC);

-- ============================================================================
-- INDEX SUR pipeline_logs
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_logs_execution_date 
ON reclamations.pipeline_logs(execution_date DESC);

CREATE INDEX IF NOT EXISTS idx_logs_job_status 
ON reclamations.pipeline_logs(job_name, status, execution_date DESC);

-- Index partiel pour les échecs seulement
CREATE INDEX IF NOT EXISTS idx_logs_failed 
ON reclamations.pipeline_logs(execution_date DESC, job_name)
WHERE status = 'FAILED';

-- ============================================================================
-- INDEX SUR data_quality_metrics
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_quality_date_table 
ON reclamations.data_quality_metrics(date_controle DESC, table_name);

CREATE INDEX IF NOT EXISTS idx_quality_score 
ON reclamations.data_quality_metrics(overall_quality_score, date_controle DESC);

-- Index partiel pour les échecs de qualité
CREATE INDEX IF NOT EXISTS idx_quality_failed 
ON reclamations.data_quality_metrics(date_controle DESC, table_name, overall_quality_score)
WHERE passed = FALSE;

-- ============================================================================
-- STATISTIQUES
-- ============================================================================

-- Mise à jour des statistiques pour améliorer le query planner
ANALYZE reclamations.reclamations_cleaned;
ANALYZE reclamations.incidents_reseau;
ANALYZE reclamations.clients;
ANALYZE reclamations.kpis_daily;
ANALYZE reclamations.anomalies_detected;
ANALYZE reclamations.pipeline_logs;
ANALYZE reclamations.data_quality_metrics;

-- ============================================================================
-- MAINTENANCE AUTOMATIQUE
-- ============================================================================

-- Configuration de l'autovacuum pour les tables à fort taux de modification
ALTER TABLE reclamations.reclamations_cleaned 
SET (autovacuum_vacuum_scale_factor = 0.1);

ALTER TABLE reclamations.pipeline_logs 
SET (autovacuum_vacuum_scale_factor = 0.05);

-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================
