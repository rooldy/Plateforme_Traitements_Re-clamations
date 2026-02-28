#!/bin/bash
# Migration vers SparkSubmitOperator - Installation Automatique

set -e

echo "🚀 MIGRATION VERS SPARKSUBMITOPERATOR"
echo "======================================"
echo ""

# 1. Créer table pipeline_runs
echo "1/5 📊 Création table pipeline_runs..."
docker exec reclamations-postgres psql -U airflow -d reclamations_db << 'SQL'
CREATE TABLE IF NOT EXISTS reclamations.pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    run_id VARCHAR(255),
    status VARCHAR(50),
    execution_time_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SQL
echo "   ✅ Table créée"
echo ""

# 2. Créer connection Spark
echo "2/5 🔗 Création connection Spark..."
docker exec reclamations-airflow-api airflow connections delete spark_default 2>/dev/null || true
docker exec reclamations-airflow-api airflow connections add spark_default \
    --conn-type spark \
    --conn-host reclamations-spark-master \
    --conn-port 7077

echo "   ✅ Connection spark_default créée"
echo ""

# 3. Créer connection PostgreSQL
echo "3/5 🔗 Création connection PostgreSQL..."
docker exec reclamations-airflow-api airflow connections delete reclamations_postgres 2>/dev/null || true
docker exec reclamations-airflow-api airflow connections add reclamations_postgres \
    --conn-type postgres \
    --conn-host reclamations-postgres \
    --conn-port 5432 \
    --conn-login airflow \
    --conn-password "${POSTGRES_PASSWORD}" \
    --conn-schema reclamations_db
echo "   ✅ Connection reclamations_postgres créée"
echo ""

# 4. Copier export_postgres.py
echo "4/5 📁 Copie export_postgres.py..."
# Assume le fichier est dans le répertoire courant
if [ -f "export_postgres.py" ]; then
    docker cp export_postgres.py reclamations-airflow-api:/opt/airflow/spark/jobs/
    echo "   ✅ Fichier copié"
else
    echo "   ⚠️  export_postgres.py non trouvé dans le répertoire courant"
    echo "   → Téléchargez-le et relancez le script"
    exit 1
fi
echo ""

# 5. Copier nouveau DAG
echo "5/5 📁 Copie nouveau DAG..."
if [ -f "medallion_pipeline_production.py" ]; then
    docker cp medallion_pipeline_production.py reclamations-airflow-api:/opt/airflow/dags/medallion_pipeline_daily.py
    echo "   ✅ DAG copié"
else
    echo "   ⚠️  medallion_pipeline_production.py non trouvé"
    echo "   → Téléchargez-le et relancez le script"
    exit 1
fi
echo ""

echo "======================================"
echo "✅ MIGRATION TERMINÉE"
echo "======================================"
echo ""
echo "📋 Prochaines étapes :"
echo "   1. Attendre 30s qu'Airflow détecte le nouveau DAG"
echo "   2. Trigger le DAG : docker exec reclamations-airflow-api airflow dags trigger medallion_pipeline_daily"
echo "   3. Ouvrir UI : http://localhost:8080"
echo ""
echo "🔍 Vérifier connections :"
echo "   docker exec reclamations-airflow-api airflow connections list | grep -E 'spark_default|reclamations_postgres'"
echo ""
