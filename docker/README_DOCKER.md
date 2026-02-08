# 🚀 Guide d'utilisation - Docker Compose Projet Réclamations

Ce document explique comment utiliser la configuration Docker adaptée pour le projet de traitement de réclamations.

---

## ✅ Ce qui a été adapté

Votre configuration Docker d'origine a été **adaptée** pour le projet réclamations :

### Changements appliqués :

1. ✅ **Tous les containers renommés** avec le préfixe `reclamations-`
   - `reclamations-postgres`
   - `reclamations-redis`
   - `reclamations-spark-master`
   - `reclamations-spark-worker`
   - `reclamations-airflow-api`
   - `reclamations-airflow-scheduler`
   - etc.

2. ✅ **Scripts SQL d'initialisation ajoutés**
   - Création automatique de `reclamations_db`
   - Schéma complet des tables
   - Index optimisés

3. ✅ **Variables PostgreSQL métier ajoutées**
   - `POSTGRES_HOST`, `POSTGRES_DB`, etc.
   - Accessibles dans les jobs Spark

4. ✅ **Scheduling @daily résolu**
   - `DAGS_ARE_PAUSED_AT_CREATION: 'false'`
   - `CATCHUP_BY_DEFAULT: 'false'`

5. ✅ **Réseau et volumes isolés**
   - Network: `reclamations_network`
   - Volume: `reclamations_postgres_data`

---

## 📁 Structure attendue

```
Plateforme_Traitement_Reclamations/
│
├── airflow/                        # ⬅️ Vous êtes ICI
│   ├── docker-compose.yml         # ✅ Fichier adapté
│   ├── Dockerfile                 # Votre Dockerfile (à adapter si besoin)
│   ├── .env                       # ✅ Configuration
│   ├── dags/                      # DAGs Airflow (créé auto)
│   ├── logs/                      # Logs (créé auto)
│   ├── config/                    # Config (créé auto)
│   └── plugins/                   # Plugins (créé auto)
│
├── spark/                         # Code PySpark
│   ├── jobs/
│   │   └── ingestion.py
│   └── utils/
│       ├── spark_config.py
│       └── data_generator.py
│
├── data/                          # Données
│   ├── raw/
│   ├── raw_parquet/
│   └── processed/
│
└── sql/                           # Scripts PostgreSQL
    ├── init-multiple-databases.sh
    ├── schema.sql
    └── indexes.sql
```

---

## 🔧 Vérifier le Dockerfile

Votre `Dockerfile` doit avoir **Java installé** pour PySpark.

### Si Java n'est pas présent, ajoutez :

```dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root

# Installation de Java pour PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configuration de JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Installation des dépendances Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Configuration PySpark
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/airflow
```

### Créer requirements.txt :

```bash
# airflow/requirements.txt
pyspark==3.5.0
pandas==2.1.4
numpy==1.26.3
psycopg2-binary==2.9.9
sqlalchemy==2.0.25
great-expectations==0.18.8
pytest==7.4.3
pytest-cov==4.1.0
python-dotenv==1.0.0
pytz==2023.3
loguru==0.7.2
```

---

## 🚀 Démarrage

### 1. Vérifier la structure

```bash
cd Plateforme_Traitement_Reclamations

# Vérifier que les dossiers existent
ls -la airflow/
ls -la spark/
ls -la data/
ls -la sql/
```

### 2. Adapter le .env (si Linux)

```bash
cd airflow

# Sur Linux, définir votre UID
echo "AIRFLOW_UID=$(id -u)" >> .env

# Vérifier
cat .env
```

### 3. Créer les répertoires manquants

```bash
# Depuis airflow/
mkdir -p dags logs config plugins
```

### 4. Générer les données (si pas déjà fait)

```bash
cd ../spark/utils
python3 data_generator.py
cd ../../airflow
```

### 5. Démarrer les services

```bash
# Depuis airflow/
docker-compose up -d
```

### 6. Suivre les logs

```bash
# Tous les services
docker-compose logs -f

# Un service spécifique
docker-compose logs -f reclamations-airflow-api
docker-compose logs -f reclamations-postgres
```

---

## 🌐 Accès aux services

Une fois démarrés, les services sont accessibles :

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | airflow / airflow |
| **Spark Master UI** | http://localhost:8081 | N/A |
| **Flower** (optionnel) | http://localhost:5555 | N/A |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

---

## 🔍 Vérifications

### Vérifier que tous les containers tournent :

```bash
docker ps --filter "name=reclamations-"
```

**Vous devriez voir :**
- reclamations-postgres (healthy)
- reclamations-redis (healthy)
- reclamations-spark-master
- reclamations-spark-worker
- reclamations-airflow-api (healthy)
- reclamations-airflow-scheduler (healthy)
- reclamations-airflow-worker (healthy)
- reclamations-airflow-triggerer (healthy)
- reclamations-airflow-dag-processor (healthy)

### Vérifier PostgreSQL :

```bash
# Se connecter
docker exec -it reclamations-postgres psql -U airflow

# Lister les bases de données
\l

# Vous devriez voir :
# - airflow (base Airflow)
# - reclamations_db (base métier) ← Important !

# Se connecter à reclamations_db
\c reclamations_db

# Lister les tables
\dt reclamations.*

# Quitter
\q
```

### Vérifier Spark :

```bash
# Vérifier que Spark Master fonctionne
curl http://localhost:8081

# Vérifier les workers connectés
docker logs reclamations-spark-worker
```

---

## 🛠️ Commandes utiles

### Arrêter les services

```bash
docker-compose down
```

### Redémarrer un service

```bash
docker-compose restart reclamations-airflow-scheduler
```

### Voir les logs d'un service

```bash
docker-compose logs -f reclamations-postgres
```

### Exécuter une commande dans un container

```bash
# CLI Airflow
docker exec -it reclamations-airflow-api airflow version

# PostgreSQL
docker exec -it reclamations-postgres psql -U airflow -d reclamations_db
```

### Activer Flower (monitoring Celery)

```bash
# Démarrer avec le profil flower
docker-compose --profile flower up -d

# Accéder à Flower
open http://localhost:5555
```

### Nettoyer complètement (⚠️ Supprime les données)

```bash
docker-compose down -v
```

---

## 🐛 Troubleshooting

### Problème : Port 8080 déjà utilisé

**Solution 1 :** Arrêter votre ancien projet
```bash
cd ~/ancien_projet/airflow
docker-compose down
```

**Solution 2 :** Changer le port dans `docker-compose.yml`
```yaml
reclamations-airflow-api:
  ports:
    - "8082:8080"  # Au lieu de 8080:8080
```

### Problème : Database reclamations_db n'existe pas

```bash
# Vérifier que les scripts SQL sont montés
docker exec reclamations-postgres ls -la /docker-entrypoint-initdb.d/

# Si vide, recréer le container
docker-compose down -v
docker-compose up -d
```

### Problème : Spark ne trouve pas les fichiers

```bash
# Vérifier les volumes montés
docker exec reclamations-spark-master ls -la /app/spark/jobs/

# Si vide, vérifier les chemins dans docker-compose.yml
```

### Problème : Permissions denied (Linux)

```bash
# Définir AIRFLOW_UID
echo "AIRFLOW_UID=$(id -u)" >> .env

# Recréer les containers
docker-compose down
docker-compose up -d
```

### Problème : Pas assez de mémoire

**Réduire les limites dans `docker-compose.yml` :**

```yaml
# Exemple pour Airflow Worker
deploy:
  resources:
    limits:
      memory: 4G  # Au lieu de 6G
```

---

## 📊 Comparaison avec votre ancien projet

| Aspect | Ancien projet | Projet réclamations | Conflit ? |
|--------|---------------|---------------------|-----------|
| Containers | `ancien_*` | `reclamations-*` | ❌ Non |
| Network | `ancien_network` | `reclamations_network` | ❌ Non |
| Volume | `ancien_data` | `reclamations_postgres_data` | ❌ Non |
| Port 8080 | Airflow UI | Airflow UI | ⚠️ Possible |
| Port 5432 | PostgreSQL | PostgreSQL | ⚠️ Possible |

**Conclusion :** Les deux projets peuvent coexister **mais pas en même temps** si vous gardez les mêmes ports.

---

## ✅ Checklist avant de démarrer

- [ ] Java installé dans le Dockerfile
- [ ] requirements.txt créé
- [ ] .env configuré (AIRFLOW_UID sur Linux)
- [ ] Données générées dans data/raw/
- [ ] Scripts SQL présents dans sql/
- [ ] Structure de répertoires complète
- [ ] Ancien projet arrêté (si même ports)

---

## 🎯 Prochaines étapes

Une fois les services démarrés :

1. **Accéder à Airflow UI** : http://localhost:8080
2. **Se connecter** avec airflow/airflow
3. **Vérifier que le DAG apparaît** (Phase 2 à venir)
4. **Tester le job d'ingestion** manuellement

---

**🚀 Vous êtes prêt à démarrer le projet réclamations sans conflit avec votre ancien projet !**
