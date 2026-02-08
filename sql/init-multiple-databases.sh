#!/bin/bash
# Script pour créer automatiquement plusieurs bases de données dans PostgreSQL
# Utilisé par docker-entrypoint-initdb.d

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Création de la base de données métier
    CREATE DATABASE reclamations_db;
    
    -- Accorder les permissions
    GRANT ALL PRIVILEGES ON DATABASE reclamations_db TO $POSTGRES_USER;
EOSQL

echo "✅ Base de données reclamations_db créée avec succès"
