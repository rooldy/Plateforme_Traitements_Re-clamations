#!/bin/bash
# Script de Backup Complet - Plateforme Réclamations
# Sauvegarde PostgreSQL + Volumes + Fichiers

set -e  # Arrêt si erreur

# ============================================================================
# CONFIGURATION
# ============================================================================

BACKUP_DIR="$HOME/Desktop/DataEngineering/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="reclamations_backup_${TIMESTAMP}"
BACKUP_PATH="${BACKUP_DIR}/${BACKUP_NAME}"

echo "🔧 Configuration du backup..."
echo "   Destination : ${BACKUP_PATH}"
echo ""

# Créer répertoire de backup
mkdir -p "${BACKUP_PATH}"

# ============================================================================
# 1. BACKUP POSTGRESQL
# ============================================================================

echo "📊 1/5 Backup PostgreSQL..."

docker exec reclamations-postgres pg_dump \
    -U airflow \
    -d reclamations_db \
    --format=custom \
    --file=/tmp/reclamations_db.dump

docker cp reclamations-postgres:/tmp/reclamations_db.dump \
    "${BACKUP_PATH}/reclamations_db.dump"

echo "   ✅ PostgreSQL sauvegardé ($(du -h ${BACKUP_PATH}/reclamations_db.dump | cut -f1))"

# ============================================================================
# 2. BACKUP VOLUMES DOCKER
# ============================================================================

echo ""
echo "📦 2/5 Backup volumes Docker..."

# Volume PostgreSQL data
docker run --rm \
    -v airflow_postgres-data:/data \
    -v "${BACKUP_PATH}:/backup" \
    alpine \
    tar czf /backup/postgres_volume.tar.gz -C /data .

echo "   ✅ Volume PostgreSQL sauvegardé ($(du -h ${BACKUP_PATH}/postgres_volume.tar.gz | cut -f1))"

# Volume Redis (si important)
docker run --rm \
    -v airflow_redis-data:/data \
    -v "${BACKUP_PATH}:/backup" \
    alpine \
    tar czf /backup/redis_volume.tar.gz -C /data . 2>/dev/null || echo "   ⚠️  Volume Redis non trouvé (normal si pas de volume nommé)"

# Volume logs Airflow (optionnel)
if docker volume ls | grep -q airflow_logs; then
    docker run --rm \
        -v airflow_logs:/data \
        -v "${BACKUP_PATH}:/backup" \
        alpine \
        tar czf /backup/airflow_logs.tar.gz -C /data .
    echo "   ✅ Logs Airflow sauvegardés ($(du -h ${BACKUP_PATH}/airflow_logs.tar.gz | cut -f1))"
fi

# ============================================================================
# 3. BACKUP DONNÉES (Bronze/Silver/Gold)
# ============================================================================

echo ""
echo "🗂️  3/5 Backup données Bronze/Silver/Gold..."

PROJECT_DIR="$HOME/Desktop/DataEngineering/Projects_Data_Engineer/Plateforme_Traitements_Réclamations"

# Copier données depuis container si elles y sont
docker exec reclamations-airflow-api tar czf /tmp/data_backup.tar.gz \
    -C /opt/airflow data/raw data/bronze data/silver data/gold 2>/dev/null || echo "   ⚠️  Données dans container non trouvées"

if docker exec reclamations-airflow-api test -f /tmp/data_backup.tar.gz 2>/dev/null; then
    docker cp reclamations-airflow-api:/tmp/data_backup.tar.gz "${BACKUP_PATH}/data_all.tar.gz"
    echo "   ✅ Données sauvegardées depuis container ($(du -h ${BACKUP_PATH}/data_all.tar.gz | cut -f1))"
fi

# Backup données locales (si bind mount)
if [ -d "${PROJECT_DIR}/airflow/data" ]; then
    tar czf "${BACKUP_PATH}/data_local.tar.gz" -C "${PROJECT_DIR}/airflow" data/
    echo "   ✅ Données locales sauvegardées ($(du -h ${BACKUP_PATH}/data_local.tar.gz | cut -f1))"
fi

# ============================================================================
# 4. BACKUP CONFIGURATION & CODE
# ============================================================================

echo ""
echo "⚙️  4/5 Backup configuration & code..."

cd "${PROJECT_DIR}/airflow"

# DAGs
if [ -d "dags" ]; then
    tar czf "${BACKUP_PATH}/dags.tar.gz" dags/
    echo "   ✅ DAGs sauvegardés ($(du -h ${BACKUP_PATH}/dags.tar.gz | cut -f1))"
fi

# Jobs Spark
if [ -d "spark" ]; then
    tar czf "${BACKUP_PATH}/spark_jobs.tar.gz" spark/
    echo "   ✅ Jobs Spark sauvegardés ($(du -h ${BACKUP_PATH}/spark_jobs.tar.gz | cut -f1))"
fi

# Scripts
if [ -d "scripts" ]; then
    tar czf "${BACKUP_PATH}/scripts.tar.gz" scripts/
    echo "   ✅ Scripts sauvegardés ($(du -h ${BACKUP_PATH}/scripts.tar.gz | cut -f1))"
fi

# Fichiers de config
cp docker-compose.yml "${BACKUP_PATH}/" 2>/dev/null || true
cp Dockerfile "${BACKUP_PATH}/" 2>/dev/null || true
cp requirements.txt "${BACKUP_PATH}/" 2>/dev/null || true
cp .env "${BACKUP_PATH}/" 2>/dev/null || true

echo "   ✅ Configuration sauvegardée"

# ============================================================================
# 5. BACKUP ÉTAT DOCKER
# ============================================================================

echo ""
echo "🐳 5/5 Backup état Docker..."

# Liste des containers
docker ps -a > "${BACKUP_PATH}/docker_containers.txt"

# Liste des images
docker images > "${BACKUP_PATH}/docker_images.txt"

# Liste des volumes
docker volume ls > "${BACKUP_PATH}/docker_volumes.txt"

# Docker compose config
docker compose config > "${BACKUP_PATH}/docker-compose-resolved.yml" 2>/dev/null || true

echo "   ✅ État Docker sauvegardé"

# ============================================================================
# RÉSUMÉ
# ============================================================================

echo ""
echo "="*70
echo "✅ BACKUP TERMINÉ"
echo "="*70
echo ""
echo "📁 Emplacement : ${BACKUP_PATH}"
echo ""
echo "📊 Contenu :"
ls -lh "${BACKUP_PATH}" | tail -n +2 | awk '{printf "   %s  %s\n", $5, $9}'
echo ""
echo "💾 Taille totale : $(du -sh ${BACKUP_PATH} | cut -f1)"
echo ""
echo "="*70

# ============================================================================
# INSTRUCTIONS RESTAURATION
# ============================================================================

cat > "${BACKUP_PATH}/RESTORE_INSTRUCTIONS.txt" << 'RESTORE_EOF'
# 📖 INSTRUCTIONS DE RESTAURATION

## 1. Restaurer PostgreSQL

```bash
# Copier le dump dans le container
docker cp reclamations_db.dump reclamations-postgres:/tmp/

# Restaurer
docker exec reclamations-postgres pg_restore \
    -U airflow \
    -d reclamations_db \
    --clean \
    /tmp/reclamations_db.dump
```

## 2. Restaurer Volumes Docker

```bash
# Volume PostgreSQL
docker run --rm \
    -v airflow_postgres-data:/data \
    -v $(pwd):/backup \
    alpine \
    sh -c "cd /data && tar xzf /backup/postgres_volume.tar.gz"

# Volume Redis
docker run --rm \
    -v airflow_redis-data:/data \
    -v $(pwd):/backup \
    alpine \
    sh -c "cd /data && tar xzf /backup/redis_volume.tar.gz"
```

## 3. Restaurer Données (Bronze/Silver/Gold)

```bash
# Dans le container
docker cp data_all.tar.gz reclamations-airflow-api:/tmp/
docker exec reclamations-airflow-api tar xzf /tmp/data_all.tar.gz -C /opt/airflow

# Ou localement
tar xzf data_local.tar.gz -C /path/to/project/airflow/
```

## 4. Restaurer Code

```bash
# DAGs
tar xzf dags.tar.gz -C /path/to/project/airflow/

# Jobs Spark
tar xzf spark_jobs.tar.gz -C /path/to/project/airflow/

# Scripts
tar xzf scripts.tar.gz -C /path/to/project/airflow/

# Config
cp docker-compose.yml /path/to/project/airflow/
cp Dockerfile /path/to/project/airflow/
cp requirements.txt /path/to/project/airflow/
```

## 5. Vérifier

```bash
# Vérifier PostgreSQL
docker exec -it reclamations-postgres psql -U airflow -d reclamations_db -c "SELECT COUNT(*) FROM reclamations.clients;"

# Vérifier données
docker exec reclamations-airflow-api ls -la /opt/airflow/data/
```
RESTORE_EOF

echo "📝 Instructions de restauration créées : ${BACKUP_PATH}/RESTORE_INSTRUCTIONS.txt"
echo ""
echo "🎉 Backup complet réussi !"
echo ""
