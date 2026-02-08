# Architecture Technique - Plateforme de Traitement Automatisé de Réclamations Clients

## 📋 Table des matières

1. [Vue d'ensemble du projet](#1-vue-densemble-du-projet)
2. [Architecture technique](#2-architecture-technique)
3. [Modèle de données](#3-modèle-de-données)
4. [Organisation du projet](#4-organisation-du-projet)
5. [Roadmap détaillée (8 semaines)](#5-roadmap-détaillée-8-semaines)
6. [Stack technique](#6-stack-technique)
7. [Transformations PySpark](#7-transformations-pyspark)
8. [Stratégie de tests](#8-stratégie-de-tests)
9. [Préparation entretien technique](#9-préparation-entretien-technique)

---

## 1. Vue d'ensemble du projet

### 1.1 Contexte métier

Ce projet simule une plateforme de traitement automatisé de réclamations clients pour un opérateur d'infrastructure énergétique (type Enedis). Il s'agit d'un système qui doit ingérer quotidiennement des réclamations provenant de multiples canaux (téléphone, email, web, agence) et les traiter de manière automatisée.

**Problématiques métier résolues :**
- Déduplication des réclamations multi-canaux (un client peut appeler puis envoyer un email pour le même problème)
- Corrélation avec les incidents réseau pour identifier les causes systémiques
- Priorisation automatique basée sur la criticité et l'impact
- Calcul des SLA (Service Level Agreement) et détection des dépassements
- Détection d'anomalies (pics inhabituels de réclamations dans une zone)

### 1.2 Objectifs techniques

**Objectif principal :** Démontrer la maîtrise end-to-end d'une infrastructure Big Data moderne, du build au run.

**Compétences mises en avant :**
- Data Engineering avec PySpark (transformations complexes, optimisations)
- Orchestration de pipelines avec Apache Airflow
- Gestion de données relationnelles (PostgreSQL)
- Containerisation et déploiement (Docker)
- Qualité des données et monitoring
- Architecture scalable et maintenable

### 1.3 Périmètre et contraintes

**Périmètre fonctionnel :**
- ✅ Ingestion de données multi-sources
- ✅ Transformations et enrichissements
- ✅ Quality checks automatisés
- ✅ Stockage optimisé (Parquet + PostgreSQL)
- ✅ Orchestration et scheduling
- ✅ Monitoring et logs centralisés

**Hors périmètre (volontairement simplifié) :**
- ❌ Streaming temps réel (batch quotidien suffit)
- ❌ Machine Learning (priorisation basée sur des règles métier)
- ❌ Interface utilisateur complexe (focus sur le backend)
- ❌ Authentification/Sécurité (environnement de développement)

**Contraintes techniques :**
- Environnement local : PC 8GB RAM, 4 cores
- Pas de cluster Hadoop/HDFS
- Mode local pour Spark (pas de cluster distribué)
- Volume de données raisonnable (20k réclamations)

---

## 2. Architecture technique

### 2.1 Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DOCKER COMPOSE                             │
│                                                                     │
│  ┌────────────────┐      ┌────────────────┐      ┌──────────────┐ │
│  │   AIRFLOW      │      │   POSTGRESQL   │      │    SPARK     │ │
│  │  (Scheduler +  │─────▶│   (Storage +   │◀─────│   (Local     │ │
│  │   Webserver)   │      │    Metadata)   │      │    Mode)     │ │
│  │                │      │                │      │              │ │
│  │  RAM: 2GB      │      │  RAM: 1GB      │      │  RAM: 3GB    │ │
│  │  Port: 8080    │      │  Port: 5432    │      │  Executors:2 │ │
│  └────────┬───────┘      └────────┬───────┘      └──────┬───────┘ │
│           │                       │                     │         │
│           │         Orchestration │         Exécution   │         │
│           └───────────────────────┴─────────────────────┘         │
│                                   │                               │
└───────────────────────────────────┼───────────────────────────────┘
                                    │
                         ┌──────────▼──────────┐
                         │   VOLUMES DOCKER    │
                         │                     │
                         │  • data/raw/        │ ◀── CSV sources
                         │  • data/processed/  │ ◀── Parquet partitionné
                         │  • logs/            │ ◀── Logs centralisés
                         │  • spark/jobs/      │ ◀── Scripts PySpark
                         │  • airflow/dags/    │ ◀── DAGs Airflow
                         └─────────────────────┘
```

### 2.2 Flux de données détaillé

```
┌──────────────────────┐
│  Données Sources     │
│  (CSV Synthétiques)  │
│                      │
│  • reclamations.csv  │
│  • incidents.csv     │
│  • clients.csv       │
└──────────┬───────────┘
           │
           │ 1. Trigger quotidien (Airflow)
           ▼
┌──────────────────────────────────────────────────┐
│              JOB SPARK : INGESTION               │
│                                                  │
│  • Lecture CSV avec schéma strict               │
│  • Validation des types                         │
│  • Détection des anomalies structurelles        │
│  • Sauvegarde Parquet brut (data/raw_parquet/)  │
└──────────┬───────────────────────────────────────┘
           │
           │ 2. Transmission des données validées
           ▼
┌──────────────────────────────────────────────────┐
│         JOB SPARK : QUALITY CHECKS               │
│                                                  │
│  • Détection doublons (même client + même jour) │
│  • Validation références Linky (format)         │
│  • Vérification cohérence dates                 │
│  • Détection valeurs aberrantes                 │
│  • Génération rapport qualité                   │
└──────────┬───────────────────────────────────────┘
           │
           │ 3. Données nettoyées
           ▼
┌──────────────────────────────────────────────────┐
│       JOB SPARK : TRANSFORMATIONS                │
│                                                  │
│  A. Enrichissement                               │
│     • Normalisation références Linky            │
│     • Géocodage et enrichissement localisation  │
│     • Catégorisation automatique                │
│                                                  │
│  B. Corrélations métier                          │
│     • Jointure avec incidents réseau            │
│     • Rapprochement historique client           │
│     • Détection réclamations récurrentes        │
│                                                  │
│  C. Calculs KPIs                                 │
│     • Durée de traitement (SLA)                 │
│     • Score de priorité                         │
│     • Agrégations géographiques                 │
│     • Statistiques temporelles                  │
└──────────┬───────────────────────────────────────┘
           │
           │ 4. Sauvegarde intermédiaire
           ▼
┌──────────────────────────────────────────────────┐
│         PARQUET PARTITIONNÉ                      │
│  (data/processed/)                               │
│                                                  │
│  Partitionnement : year/month/region/            │
│  Compression : Snappy                            │
│  Format optimisé pour requêtes analytiques       │
└──────────┬───────────────────────────────────────┘
           │
           │ 5. Export final
           ▼
┌──────────────────────────────────────────────────┐
│       JOB SPARK : EXPORT POSTGRESQL              │
│                                                  │
│  • Écriture en mode append                      │
│  • Gestion des erreurs et retry                 │
│  • Mise à jour des tables de logs               │
└──────────┬───────────────────────────────────────┘
           │
           │ 6. Stockage persistant
           ▼
┌──────────────────────────────────────────────────┐
│            POSTGRESQL                            │
│                                                  │
│  Tables :                                        │
│  • reclamations_cleaned                         │
│  • kpis_daily                                   │
│  • anomalies_detected                           │
│  • pipeline_logs                                │
│  • data_quality_metrics                         │
└──────────────────────────────────────────────────┘
```

### 2.3 Justification des choix techniques

| Composant | Choix | Alternative envisagée | Justification |
|-----------|-------|----------------------|---------------|
| **Processing** | PySpark (local) | Pandas / Dask | Scalabilité du code (compatible cluster), API riche pour transformations complexes, standard de l'industrie |
| **Orchestration** | Airflow | Prefect / Dagster | Maturité, communauté large, interface web complète, scheduling robuste |
| **Stockage** | PostgreSQL | MySQL / MongoDB | ACID, performances analytiques (avec index), support JSON, familiarité |
| **Container** | Docker Compose | Kubernetes | Simplicité pour environnement local, suffisant pour le périmètre, démarrage rapide |
| **Format intermédiaire** | Parquet | Avro / ORC | Compression efficace, lecture columnaire rapide, standard de facto, support natif Spark |
| **Partitionnement** | Date + Région | Date seule | Optimisation des requêtes géographiques fréquentes, équilibrage des partitions |

### 2.4 Configuration matérielle optimisée

**Pour PC 8GB RAM :**

```yaml
# Allocation mémoire
Total disponible: 8GB
├── Système (OS): 2GB
└── Docker: 6GB
    ├── PostgreSQL: 1GB
    ├── Airflow: 2GB
    └── Spark: 3GB
        ├── Driver: 1GB
        └── Executors (2x): 2GB
```

**Optimisations Spark :**
```python
spark.driver.memory = 1g
spark.executor.memory = 1g
spark.executor.instances = 2
spark.sql.shuffle.partitions = 8  # Réduit vs défaut 200
spark.default.parallelism = 4
```

**Optimisations PostgreSQL :**
```conf
shared_buffers = 256MB
effective_cache_size = 512MB
max_connections = 20
```

---

## 3. Modèle de données

### 3.1 Données sources (CSV)

#### 3.1.1 reclamations.csv (20 000 lignes)

```
Colonnes :
- id_reclamation        : STRING   (format: REC_YYYYMMDD_XXXXX)
- id_client             : STRING   (format: CLI_XXXXX)
- type_reclamation      : STRING   (RACCORDEMENT, LINKY, COUPURE, FACTURATION, INTERVENTION)
- canal                 : STRING   (TELEPHONE, EMAIL, WEB, AGENCE)
- description           : TEXT     (description naturelle)
- date_creation         : TIMESTAMP
- date_cloture          : TIMESTAMP (nullable)
- statut                : STRING   (OUVERT, EN_COURS, CLOS, ESCALADE)
- priorite              : STRING   (BASSE, MOYENNE, HAUTE, CRITIQUE)
- region                : STRING   (ex: ILE_DE_FRANCE, NORMANDIE...)
- adresse               : STRING
- code_postal           : STRING
- latitude              : DOUBLE
- longitude             : DOUBLE
- ref_compteur_linky    : STRING   (format: LINKY_XXXXX, nullable)
- ref_incident          : STRING   (format: INC_XXXXX, nullable)

Volumétrie : ~20 000 lignes, ~15MB CSV, ~5MB Parquet compressé
```

**Règles métier pour génération :**
- 30% des réclamations ont une ref_incident (corrélation avec panne réseau)
- 60% des réclamations de type LINKY ont une ref_compteur_linky
- Les réclamations groupées dans le temps/espace (simulation pannes)
- Distribution réaliste des canaux : TELEPHONE (40%), WEB (30%), EMAIL (20%), AGENCE (10%)
- 70% des réclamations sont clôturées

#### 3.1.2 incidents_reseau.csv (500 lignes)

```
Colonnes :
- id_incident           : STRING   (format: INC_XXXXX)
- type_incident         : STRING   (PANNE_ELECTRIQUE, MAINTENANCE, SURCHARGE, DEFAUT_CABLE)
- date_debut            : TIMESTAMP
- date_fin              : TIMESTAMP (nullable si incident en cours)
- zone_affectee         : STRING   (nom de la zone)
- nb_clients_impactes   : INTEGER
- latitude              : DOUBLE
- longitude             : DOUBLE
- cause                 : STRING
- statut                : STRING   (EN_COURS, RESOLU)

Volumétrie : ~500 lignes, ~200KB CSV
```

#### 3.1.3 historique_clients.csv (5 000 lignes)

```
Colonnes :
- id_client             : STRING   (format: CLI_XXXXX)
- nb_reclamations_anterieures : INTEGER
- type_client           : STRING   (PARTICULIER, PROFESSIONNEL, ENTREPRISE)
- anciennete_mois       : INTEGER
- score_satisfaction    : INTEGER  (0-100)
- derniere_reclamation  : DATE     (nullable)

Volumétrie : ~5 000 lignes, ~300KB CSV
```

### 3.2 Modèle relationnel PostgreSQL

#### 3.2.1 Table : reclamations_cleaned

```sql
CREATE TABLE reclamations_cleaned (
    id_reclamation VARCHAR(50) PRIMARY KEY,
    id_client VARCHAR(20) NOT NULL,
    type_reclamation VARCHAR(30) NOT NULL,
    canal VARCHAR(20) NOT NULL,
    description TEXT,
    date_creation TIMESTAMP NOT NULL,
    date_cloture TIMESTAMP,
    statut VARCHAR(20) NOT NULL,
    priorite VARCHAR(20) NOT NULL,
    region VARCHAR(50) NOT NULL,
    adresse VARCHAR(200),
    code_postal VARCHAR(10),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    ref_compteur_linky VARCHAR(30),
    ref_incident VARCHAR(30),
    
    -- Champs enrichis
    categorie_auto VARCHAR(50),
    zone_geographique VARCHAR(100),
    duree_traitement_heures INTEGER,
    score_priorite DOUBLE PRECISION,
    est_recurrent BOOLEAN,
    
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Index
    FOREIGN KEY (ref_incident) REFERENCES incidents_reseau(id_incident)
);

CREATE INDEX idx_reclamations_date ON reclamations_cleaned(date_creation);
CREATE INDEX idx_reclamations_region ON reclamations_cleaned(region);
CREATE INDEX idx_reclamations_statut ON reclamations_cleaned(statut);
CREATE INDEX idx_reclamations_client ON reclamations_cleaned(id_client);
CREATE INDEX idx_reclamations_geo ON reclamations_cleaned(latitude, longitude);
```

#### 3.2.2 Table : kpis_daily

```sql
CREATE TABLE kpis_daily (
    id SERIAL PRIMARY KEY,
    date_calcul DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    type_reclamation VARCHAR(30),
    
    -- Métriques volumétriques
    nb_reclamations_total INTEGER,
    nb_reclamations_ouvertes INTEGER,
    nb_reclamations_closes INTEGER,
    
    -- Métriques temporelles
    duree_moyenne_traitement_heures DOUBLE PRECISION,
    taux_respect_sla DOUBLE PRECISION,
    nb_depassements_sla INTEGER,
    
    -- Métriques qualité
    taux_recurrence DOUBLE PRECISION,
    satisfaction_moyenne DOUBLE PRECISION,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(date_calcul, region, type_reclamation)
);

CREATE INDEX idx_kpis_date ON kpis_daily(date_calcul);
CREATE INDEX idx_kpis_region ON kpis_daily(region);
```

#### 3.2.3 Table : anomalies_detected

```sql
CREATE TABLE anomalies_detected (
    id SERIAL PRIMARY KEY,
    date_detection TIMESTAMP NOT NULL,
    type_anomalie VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    description TEXT,
    severite VARCHAR(20),
    valeur_observee DOUBLE PRECISION,
    valeur_attendue DOUBLE PRECISION,
    ecart_percentage DOUBLE PRECISION,
    est_traitee BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 3.2.4 Table : pipeline_logs

```sql
CREATE TABLE pipeline_logs (
    id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP NOT NULL,
    job_name VARCHAR(100) NOT NULL,
    statut VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, RUNNING
    nb_lignes_input INTEGER,
    nb_lignes_output INTEGER,
    nb_lignes_rejetees INTEGER,
    duree_secondes INTEGER,
    message_erreur TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_logs_date ON pipeline_logs(execution_date);
CREATE INDEX idx_logs_job ON pipeline_logs(job_name);
```

### 3.3 Stratégie de partitionnement Parquet

```
data/processed/
├── reclamations/
│   ├── year=2024/
│   │   ├── month=01/
│   │   │   ├── region=ILE_DE_FRANCE/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── region=NORMANDIE/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   └── ...
│   │   └── month=02/
│   │       └── ...
│   └── year=2025/
│       └── ...
```

**Avantages :**
- Partition pruning automatique sur les requêtes filtrées par date/région
- Équilibrage des partitions (évite les skews)
- Maintenance facilitée (suppression par partition)

---

## 4. Organisation du projet

### 4.1 Structure complète des dossiers

```
Plateforme_de_traitement_automatise_de_reclamations_clients/
│
├── .github/
│   └── workflows/
│       └── ci.yml                    # CI/CD (optionnel Phase 4)
│
├── data/
│   ├── raw/                          # CSV sources (gitignored)
│   │   ├── reclamations.csv
│   │   ├── incidents_reseau.csv
│   │   └── historique_clients.csv
│   ├── raw_parquet/                  # Parquet brut post-ingestion
│   └── processed/                    # Parquet transformé et partitionné
│       └── reclamations/
│           └── year=2024/...
│
├── spark/
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── ingestion.py              # Job 1: Lecture CSV → Parquet brut
│   │   ├── quality_check.py          # Job 2: Contrôles qualité
│   │   ├── transformation.py         # Job 3: Transformations métier
│   │   └── export_postgres.py        # Job 4: Export vers PostgreSQL
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── spark_config.py           # Configuration Spark réutilisable
│   │   ├── data_generator.py         # Génération données synthétiques
│   │   ├── logger.py                 # Logging centralisé
│   │   └── quality_rules.py          # Règles métier qualité
│   │
│   └── tests/
│       ├── __init__.py
│       ├── test_transformations.py
│       └── test_quality_checks.py
│
├── airflow/
│   ├── dags/
│   │   ├── __init__.py
│   │   └── reclamations_pipeline_dag.py  # DAG principal
│   │
│   ├── config/
│   │   └── airflow.cfg               # Configuration Airflow
│   │
│   └── logs/                         # Logs Airflow (gitignored)
│
├── sql/
│   ├── schema/
│   │   ├── 01_create_tables.sql
│   │   ├── 02_create_indexes.sql
│   │   └── 03_create_views.sql
│   │
│   ├── queries/
│   │   ├── kpis_calculation.sql
│   │   └── anomaly_detection.sql
│   │
│   └── migrations/
│       └── README.md
│
├── docker/
│   ├── docker-compose.yml            # Composition complète
│   ├── Dockerfile.spark              # Image Spark custom
│   ├── Dockerfile.airflow            # Image Airflow custom
│   └── .env.example                  # Variables d'environnement
│
├── monitoring/
│   ├── log_ingestion.py              # Centralisation logs → PostgreSQL
│   └── alerts.py                     # Détection alertes (optionnel)
│
├── docs/
│   ├── architecture.md               # Ce document
│   ├── setup_guide.md                # Guide d'installation
│   ├── user_guide.md                 # Guide utilisateur
│   ├── technical_decisions.md        # Décisions d'architecture
│   └── images/
│       ├── architecture_diagram.png
│       └── data_flow.png
│
├── notebooks/                        # Exploration (optionnel)
│   └── data_exploration.ipynb
│
├── .gitignore
├── .env.example
├── README.md
├── requirements.txt                  # Dépendances Python
├── setup.py                          # Package Python (optionnel)
└── Makefile                          # Commandes utiles
```

### 4.2 Conventions de nommage

**Fichiers Python :**
- `snake_case` pour tous les fichiers et fonctions
- Préfixe par numéro pour l'ordre d'exécution si pertinent

**Variables :**
- `UPPER_CASE` pour les constantes
- `snake_case` pour les variables

**Tables SQL :**
- `snake_case`
- Suffixe `_cleaned` pour données nettoyées
- Suffixe `_raw` pour données brutes

**Colonnes Spark/SQL :**
- `snake_case`
- Préfixe métier explicite (ex: `nb_`, `taux_`, `date_`)

### 4.3 Gestion des configurations

**Fichier `.env` (non versionné) :**
```bash
# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=reclamations_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=strongpassword

# Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW_UID=50000

# Spark
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=1g

# Chemins
DATA_RAW_PATH=/opt/data/raw
DATA_PROCESSED_PATH=/opt/data/processed
```

**Fichier `.env.example` (versionné) :**
```bash
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=reclamations_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
# ... (sans valeurs sensibles)
```

---

## 5. Roadmap détaillée (8 semaines)

### Semaine 1-2 : Fondations et génération de données

**Objectifs :**
- Comprendre le modèle de données métier
- Générer des données synthétiques réalistes
- Créer le premier job Spark d'ingestion simple

**Livrables :**
- [ ] Script `data_generator.py` fonctionnel
- [ ] 20k réclamations + 500 incidents + 5k clients générés
- [ ] Job `ingestion.py` qui lit CSV et sauvegarde en Parquet
- [ ] Documentation du modèle de données

**Validation :**
- Les données générées respectent les contraintes métier
- Le job Spark s'exécute en local sans erreur
- Les Parquet sont lisibles et bien formatés

**Focus pédagogique :**
- Comprendre les DataFrames Spark
- Maîtriser les schémas et la validation
- Appréhender le format Parquet

---

### Semaine 3-4 : Transformations et quality checks

**Objectifs :**
- Implémenter les transformations métier complexes
- Créer un système de contrôle qualité robuste
- Optimiser les performances Spark

**Livrables :**
- [ ] Job `quality_check.py` avec toutes les règles
- [ ] Job `transformation.py` avec enrichissements
- [ ] Tests unitaires sur les transformations
- [ ] Optimisations (broadcast joins, cache, etc.)

**Validation :**
- Détection des doublons fonctionne (tests avec données dupliquées)
- Les jointures avec incidents sont correctes
- Les KPIs calculés sont cohérents
- Performance acceptable (<5 min pour 20k lignes)

**Focus pédagogique :**
- Transformations Spark avancées (window functions, aggregations)
- Gestion des jointures et broadcast
- Optimisation des performances

---

### Semaine 5 : PostgreSQL et export

**Objectifs :**
- Créer le schéma PostgreSQL complet
- Implémenter l'export depuis Spark
- Optimiser les index et requêtes

**Livrables :**
- [ ] Scripts SQL de création du schéma
- [ ] Job `export_postgres.py` fonctionnel
- [ ] Index et vues SQL optimisés
- [ ] Requêtes analytiques de validation

**Validation :**
- Les données sont correctement insérées dans PostgreSQL
- Les index améliorent significativement les performances
- Les contraintes d'intégrité sont respectées

**Focus pédagogique :**
- Modélisation relationnelle
- Optimisation PostgreSQL (EXPLAIN ANALYZE)
- Stratégies d'indexation

---

### Semaine 6 : Orchestration Airflow

**Objectifs :**
- Installer et configurer Airflow
- Créer le DAG principal
- Gérer les dépendances entre jobs

**Livrables :**
- [ ] DAG `reclamations_pipeline_dag.py` complet
- [ ] Gestion des erreurs et retry
- [ ] Notifications en cas d'échec
- [ ] Interface Airflow opérationnelle

**Validation :**
- Le pipeline s'exécute end-to-end via Airflow
- Les erreurs sont correctement gérées
- Les logs sont consultables dans l'UI

**Focus pédagogique :**
- Concepts Airflow (DAG, Tasks, Operators)
- Gestion des dépendances
- Monitoring et debugging

---

### Semaine 7 : Dockerisation

**Objectifs :**
- Containeriser tous les composants
- Créer un docker-compose fonctionnel
- Documenter le déploiement

**Livrables :**
- [ ] `docker-compose.yml` complet
- [ ] Dockerfiles optimisés
- [ ] Guide de déploiement
- [ ] Scripts de démarrage/arrêt

**Validation :**
- `docker-compose up` démarre tout le stack
- Les services communiquent correctement
- Persistance des données via volumes

**Focus pédagogique :**
- Architecture multi-conteneurs
- Networking Docker
- Gestion des volumes

---

### Semaine 8 : Finalisation et préparation entretien

**Objectifs :**
- Monitoring et logs centralisés
- Tests de bout en bout
- Documentation complète
- Préparation de la démonstration

**Livrables :**
- [ ] Système de monitoring opérationnel
- [ ] README complet et professionnel
- [ ] Scénarios de démonstration préparés
- [ ] Liste des questions/réponses techniques

**Validation :**
- Le projet peut être cloné et lancé en <10 min
- Tous les composants fonctionnent ensemble
- La documentation est claire et exhaustive

**Focus pédagogique :**
- Vision end-to-end du projet
- Capacité à expliquer les choix techniques
- Identification des axes d'amélioration

---

## 6. Stack technique

### 6.1 Versions des outils

| Outil | Version | Justification |
|-------|---------|---------------|
| **Python** | 3.10 | Stabilité, compatibilité large |
| **PySpark** | 3.5.0 | Dernière version stable, performances améliorées |
| **Apache Airflow** | 2.8.0 | Dernière version LTS, interface améliorée |
| **PostgreSQL** | 15 | Performances, support JSON natif |
| **Docker** | 24.0+ | Dernière version stable |
| **Docker Compose** | 2.23+ | Syntaxe v2, fonctionnalités récentes |

### 6.2 Dépendances Python (requirements.txt)

```txt
# Spark
pyspark==3.5.0

# Data manipulation
pandas==2.1.4
numpy==1.26.2

# Airflow
apache-airflow==2.8.0
apache-airflow-providers-postgres==5.10.0

# PostgreSQL
psycopg2-binary==2.9.9
SQLAlchemy==2.0.23

# Data generation
faker==22.0.0
geopy==2.4.1

# Testing
pytest==7.4.3
pytest-spark==0.6.0
great-expectations==0.18.8

# Logging
python-json-logger==2.0.7

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1
```

### 6.3 Configuration Docker recommandée

**Configuration minimale :**
- RAM : 8GB
- CPU : 4 cores
- Disque : 20GB libres

**Configuration recommandée :**
- RAM : 16GB
- CPU : 8 cores
- Disque : 50GB libres

---

## 7. Transformations PySpark

### 7.1 Vue d'ensemble des transformations

```
INGESTION → QUALITY → TRANSFORMATION → EXPORT
   ↓          ↓            ↓             ↓
 Parquet   Rapport     Parquet        PostgreSQL
  brut     qualité     enrichi
```

### 7.2 Job 1 : Ingestion (ingestion.py)

**Objectif :** Lire les CSV sources, valider la structure, sauvegarder en Parquet brut.

**Transformations :**

```python
# Pseudo-code illustratif

1. Définition du schéma strict
   - Typage fort (STRING, TIMESTAMP, DOUBLE, INTEGER)
   - Validation des formats (regex pour ID, codes postaux)
   
2. Lecture CSV avec schéma
   - Mode permissive pour capturer les erreurs
   - Détection des lignes malformées
   
3. Validation structurelle
   - Colonnes obligatoires présentes
   - Pas de lignes entièrement NULL
   - Formats de dates cohérents
   
4. Statistiques basiques
   - Nombre de lignes lues
   - Nombre de lignes rejetées
   - Distribution par type/région
   
5. Sauvegarde Parquet brut
   - Mode overwrite
   - Compression snappy
   - Sans partitionnement (brut)
```

**Métriques collectées :**
- Nb lignes input
- Nb lignes valides
- Nb lignes rejetées
- Durée d'exécution

**Intérêt technique :**
- Maîtrise des schémas Spark
- Gestion des erreurs de parsing
- Première étape de qualité

---

### 7.3 Job 2 : Quality Checks (quality_check.py)

**Objectif :** Détecter et traiter les problèmes de qualité des données.

**Transformations :**

```python
# 1. DÉTECTION DES DOUBLONS
# Règle métier : Même client + même jour + même type = doublon
df_dedup = df.withColumn(
    "dedup_key",
    concat(col("id_client"), lit("_"), 
           date_format("date_creation", "yyyyMMdd"), lit("_"),
           col("type_reclamation"))
)

# Window pour identifier les doublons
window = Window.partitionBy("dedup_key").orderBy(col("date_creation").asc())
df_with_rank = df_dedup.withColumn("rank", row_number().over(window))

# Garder le premier, marquer les autres
df_cleaned = df_with_rank.filter(col("rank") == 1)
df_duplicates = df_with_rank.filter(col("rank") > 1)


# 2. VALIDATION DES RÉFÉRENCES LINKY
# Format attendu : LINKY_XXXXX (5 chiffres)
df_validated = df_cleaned.withColumn(
    "ref_linky_valide",
    when(
        col("ref_compteur_linky").rlike("^LINKY_[0-9]{5}$"),
        True
    ).otherwise(False)
)

# Correction automatique si possible
df_validated = df_validated.withColumn(
    "ref_compteur_linky",
    when(
        col("ref_linky_valide") == False,
        regexp_replace(col("ref_compteur_linky"), "[^0-9]", "")
    ).otherwise(col("ref_compteur_linky"))
)


# 3. VALIDATION COHÉRENCE DES DATES
df_validated = df_validated.withColumn(
    "date_coherence",
    when(
        col("date_cloture").isNotNull(),
        col("date_cloture") >= col("date_creation")
    ).otherwise(True)
)

# Filtrer les incohérences
df_anomalies_dates = df_validated.filter(col("date_coherence") == False)


# 4. DÉTECTION VALEURS ABERRANTES
# Exemple : durée de traitement > 90 jours = suspect
df_with_duree = df_validated.withColumn(
    "duree_jours",
    when(
        col("date_cloture").isNotNull(),
        datediff(col("date_cloture"), col("date_creation"))
    )
)

df_validated = df_with_duree.withColumn(
    "duree_aberrante",
    when(col("duree_jours") > 90, True).otherwise(False)
)


# 5. GESTION DES VALEURS MANQUANTES
# Stratégie par colonne
df_cleaned = df_validated \
    .fillna({"priorite": "MOYENNE"}) \
    .fillna({"canal": "INCONNU"}) \
    .filter(col("id_client").isNotNull())  # Obligatoire


# 6. GÉNÉRATION RAPPORT QUALITÉ
quality_report = {
    "nb_doublons_detectes": df_duplicates.count(),
    "nb_ref_linky_invalides": df_validated.filter(~col("ref_linky_valide")).count(),
    "nb_dates_incoherentes": df_anomalies_dates.count(),
    "nb_durees_aberrantes": df_validated.filter(col("duree_aberrante")).count(),
    "taux_completude": calcul_taux_completude(df_cleaned)
}
```

**Métriques collectées :**
- Nombre de doublons détectés et supprimés
- Taux de complétude par colonne
- Anomalies par type
- Rapport qualité exporté en JSON

**Intérêt technique :**
- Window functions avancées
- Regex et validation de formats
- Gestion intelligente des NULL
- Métriques de qualité

---

### 7.4 Job 3 : Transformations métier (transformation.py)

**Objectif :** Enrichir, corréler et calculer les KPIs métier.

**A. ENRICHISSEMENT**

```python
# 1. NORMALISATION RÉFÉRENCES LINKY
# Uniformisation du format
df_enriched = df.withColumn(
    "ref_compteur_linky_norm",
    when(
        col("ref_compteur_linky").isNotNull(),
        concat(lit("LINKY_"), 
               lpad(regexp_extract(col("ref_compteur_linky"), "[0-9]+", 0), 5, "0"))
    )
)


# 2. GÉOCODAGE ET ENRICHISSEMENT LOCALISATION
# Création de zones géographiques
df_enriched = df_enriched.withColumn(
    "zone_geographique",
    when(col("code_postal").startswith("75"), "PARIS_CENTRE")
    .when(col("code_postal").between("78000", "78999"), "YVELINES")
    .when(col("code_postal").between("92000", "92999"), "HAUTS_DE_SEINE")
    # ... mapping complet des zones
    .otherwise("AUTRE")
)

# Distance au centre régional (pour calcul de criticité)
df_enriched = df_enriched.withColumn(
    "distance_centre_km",
    calcul_distance_haversine(
        col("latitude"), col("longitude"),
        lit(CENTRE_REGION_LAT), lit(CENTRE_REGION_LON)
    )
)


# 3. CATÉGORISATION AUTOMATIQUE
# Classification fine basée sur mots-clés dans description
df_enriched = df_enriched.withColumn(
    "categorie_auto",
    when(
        lower(col("description")).contains("linky") | 
        lower(col("description")).contains("compteur"),
        "COMPTEUR_LINKY"
    )
    .when(
        lower(col("description")).contains("coupure") | 
        lower(col("description")).contains("panne"),
        "COUPURE_ELECTRIQUE"
    )
    .when(
        lower(col("description")).contains("facture") | 
        lower(col("description")).contains("montant"),
        "FACTURATION"
    )
    .when(
        lower(col("description")).contains("raccordement") | 
        lower(col("description")).contains("travaux"),
        "RACCORDEMENT"
    )
    .otherwise(col("type_reclamation"))
)
```

**B. CORRÉLATIONS MÉTIER**

```python
# 1. JOINTURE AVEC INCIDENTS RÉSEAU
# Détection si réclamation liée à un incident connu
df_incidents = spark.read.parquet("data/raw_parquet/incidents_reseau")

# Jointure spatiale et temporelle
df_correlated = df_enriched.alias("r").join(
    df_incidents.alias("i"),
    [
        # Même zone géographique (rayon 10km)
        (col("r.latitude").between(col("i.latitude") - 0.1, col("i.latitude") + 0.1)) &
        (col("r.longitude").between(col("i.longitude") - 0.1, col("i.longitude") + 0.1)) &
        # Réclamation créée pendant l'incident
        (col("r.date_creation").between(col("i.date_debut"), 
                                        coalesce(col("i.date_fin"), current_timestamp())))
    ],
    "left"
)

df_correlated = df_correlated.withColumn(
    "est_lie_incident",
    when(col("i.id_incident").isNotNull(), True).otherwise(False)
)


# 2. RAPPROCHEMENT HISTORIQUE CLIENT
df_clients = spark.read.parquet("data/raw_parquet/historique_clients")

df_with_history = df_correlated.join(
    df_clients,
    on="id_client",
    how="left"
)

# Détection réclamations récurrentes
window_client = Window.partitionBy("id_client").orderBy("date_creation")
df_with_history = df_with_history.withColumn(
    "num_reclamation_client",
    row_number().over(window_client)
)

df_with_history = df_with_history.withColumn(
    "est_recurrent",
    when(col("nb_reclamations_anterieures") > 2, True).otherwise(False)
)


# 3. AGRÉGATIONS PAR ZONE GÉOGRAPHIQUE
# Calcul du nombre de réclamations par zone/jour
window_zone = Window.partitionBy("zone_geographique", 
                                  date_format("date_creation", "yyyy-MM-dd"))

df_with_context = df_with_history.withColumn(
    "nb_reclamations_zone_jour",
    count("*").over(window_zone)
)
```

**C. CALCULS KPIs**

```python
# 1. DURÉE DE TRAITEMENT (SLA MONITORING)
df_with_kpis = df_with_context.withColumn(
    "duree_traitement_heures",
    when(
        col("date_cloture").isNotNull(),
        round((unix_timestamp("date_cloture") - 
               unix_timestamp("date_creation")) / 3600, 2)
    )
)

# SLA par type de réclamation (exemple)
SLA_TARGETS = {
    "COUPURE": 24,      # 24h
    "LINKY": 72,        # 72h
    "RACCORDEMENT": 168, # 7 jours
    "FACTURATION": 120,  # 5 jours
    "INTERVENTION": 48   # 48h
}

df_with_kpis = df_with_kpis.withColumn(
    "sla_target_heures",
    when(col("type_reclamation") == "COUPURE", 24)
    .when(col("type_reclamation") == "LINKY", 72)
    .when(col("type_reclamation") == "RACCORDEMENT", 168)
    .when(col("type_reclamation") == "FACTURATION", 120)
    .when(col("type_reclamation") == "INTERVENTION", 48)
    .otherwise(120)  # Défaut 5 jours
)

df_with_kpis = df_with_kpis.withColumn(
    "sla_respecte",
    when(
        col("date_cloture").isNotNull(),
        col("duree_traitement_heures") <= col("sla_target_heures")
    )
)


# 2. SCORE DE PRIORITÉ
# Algorithme de scoring multi-critères
df_with_kpis = df_with_kpis.withColumn(
    "score_priorite",
    (
        # Priorité déclarée (0-100)
        when(col("priorite") == "CRITIQUE", 100)
        .when(col("priorite") == "HAUTE", 75)
        .when(col("priorite") == "MOYENNE", 50)
        .when(col("priorite") == "BASSE", 25)
        .otherwise(50)
    ) * 0.4 +  # Poids 40%
    
    (
        # Client récurrent (+30 points)
        when(col("est_recurrent"), 30).otherwise(0)
    ) * 0.2 +  # Poids 20%
    
    (
        # Lié à incident réseau (+40 points)
        when(col("est_lie_incident"), 40).otherwise(0)
    ) * 0.2 +  # Poids 20%
    
    (
        # Ancienneté non traitée (max 20 points)
        least(
            datediff(current_date(), col("date_creation")) * 2,
            lit(20)
        )
    ) * 0.2  # Poids 20%
)


# 3. DÉTECTION D'ANOMALIES STATISTIQUES
# Calcul de la moyenne mobile sur 7 jours par zone
window_7j = Window.partitionBy("zone_geographique") \
    .orderBy(col("date_creation").cast("long")) \
    .rangeBetween(-7*86400, 0)  # 7 jours en secondes

df_with_stats = df_with_kpis.withColumn(
    "avg_reclamations_7j",
    avg("nb_reclamations_zone_jour").over(window_7j)
)

df_with_stats = df_with_stats.withColumn(
    "std_reclamations_7j",
    stddev("nb_reclamations_zone_jour").over(window_7j)
)

# Anomalie si > moyenne + 2*écart-type
df_with_stats = df_with_stats.withColumn(
    "est_anomalie_statistique",
    when(
        col("nb_reclamations_zone_jour") > 
        (col("avg_reclamations_7j") + 2 * col("std_reclamations_7j")),
        True
    ).otherwise(False)
)


# 4. MÉTRIQUES TEMPORELLES
# Délai première réponse (simulation : 20% des réclamations)
df_final = df_with_stats.withColumn(
    "delai_premiere_reponse_heures",
    when(
        rand() < 0.2,  # 20% ont une réponse enregistrée
        round(rand() * 48, 2)  # Entre 0 et 48h
    )
)

# Classification temporelle
df_final = df_final.withColumn(
    "periode_journee",
    when(hour(col("date_creation")).between(6, 12), "MATIN")
    .when(hour(col("date_creation")).between(12, 18), "APRES_MIDI")
    .when(hour(col("date_creation")).between(18, 22), "SOIREE")
    .otherwise("NUIT")
)

df_final = df_final.withColumn(
    "est_weekend",
    when(dayofweek(col("date_creation")).isin([1, 7]), True).otherwise(False)
)
```

**D. PARTITIONNEMENT ET OPTIMISATION**

```python
# 1. AJOUT DES COLONNES DE PARTITIONNEMENT
df_final = df_final \
    .withColumn("year", year(col("date_creation"))) \
    .withColumn("month", month(col("date_creation"))) \
    .withColumn("region", col("region"))


# 2. SAUVEGARDE PARQUET PARTITIONNÉ
df_final.write \
    .partitionBy("year", "month", "region") \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save("data/processed/reclamations")


# 3. OPTIMISATIONS APPLIQUÉES
# - Cache des DataFrames réutilisés
df_incidents.cache()
df_clients.cache()

# - Broadcast join pour petites tables (<10MB)
df_correlated = df_enriched.join(
    broadcast(df_incidents),
    on=condition,
    how="left"
)

# - Repartition avant agrégations lourdes
df_with_context = df_with_history.repartition(8, "zone_geographique")

# - Coalesce avant écriture pour éviter trop de petits fichiers
df_final.coalesce(4).write.parquet(...)
```

**Métriques collectées :**
- Taux de corrélation avec incidents (%)
- Distribution des scores de priorité
- Nombre d'anomalies détectées
- Performance des transformations

**Intérêt technique :**
- Jointures complexes (spatiales, temporelles)
- Window functions avancées (moving average)
- Calculs multi-critères
- Optimisations Spark (broadcast, cache, partition)

---

### 7.5 Job 4 : Export PostgreSQL (export_postgres.py)

**Objectif :** Exporter les données transformées vers PostgreSQL de manière optimisée.

**Transformations :**

```python
# 1. LECTURE DES DONNÉES TRANSFORMÉES
df_to_export = spark.read.parquet("data/processed/reclamations")


# 2. SÉLECTION DES COLONNES POUR EXPORT
# Uniquement les colonnes du schéma PostgreSQL
colonnes_postgres = [
    "id_reclamation", "id_client", "type_reclamation", "canal",
    "description", "date_creation", "date_cloture", "statut",
    "priorite", "region", "adresse", "code_postal",
    "latitude", "longitude", "ref_compteur_linky", "ref_incident",
    "categorie_auto", "zone_geographique", "duree_traitement_heures",
    "score_priorite", "est_recurrent"
]

df_export = df_to_export.select(*colonnes_postgres)


# 3. EXPORT VERS POSTGRESQL
# Mode append pour imports incrémentaux
df_export.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "reclamations_cleaned") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()


# 4. CALCUL ET EXPORT DES KPIs QUOTIDIENS
kpis_daily = df_to_export \
    .groupBy("date_creation", "region", "type_reclamation") \
    .agg(
        count("*").alias("nb_reclamations_total"),
        sum(when(col("statut") == "OUVERT", 1).otherwise(0)).alias("nb_reclamations_ouvertes"),
        sum(when(col("statut") == "CLOS", 1).otherwise(0)).alias("nb_reclamations_closes"),
        avg("duree_traitement_heures").alias("duree_moyenne_traitement_heures"),
        (sum(when(col("sla_respecte"), 1).otherwise(0)) / count("*") * 100)
            .alias("taux_respect_sla"),
        sum(when(~col("sla_respecte"), 1).otherwise(0)).alias("nb_depassements_sla"),
        (sum(when(col("est_recurrent"), 1).otherwise(0)) / count("*") * 100)
            .alias("taux_recurrence")
    )

kpis_daily.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "kpis_daily") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .mode("append") \
    .save()


# 5. EXPORT DES ANOMALIES DÉTECTÉES
anomalies = df_to_export \
    .filter(col("est_anomalie_statistique")) \
    .select(
        current_timestamp().alias("date_detection"),
        lit("PIC_RECLAMATIONS").alias("type_anomalie"),
        col("region"),
        concat(
            lit("Pic inhabituel de "), 
            col("nb_reclamations_zone_jour"), 
            lit(" réclamations détecté")
        ).alias("description"),
        when(col("nb_reclamations_zone_jour") > 100, "HAUTE")
            .when(col("nb_reclamations_zone_jour") > 50, "MOYENNE")
            .otherwise("BASSE").alias("severite"),
        col("nb_reclamations_zone_jour").alias("valeur_observee"),
        col("avg_reclamations_7j").alias("valeur_attendue"),
        ((col("nb_reclamations_zone_jour") - col("avg_reclamations_7j")) / 
         col("avg_reclamations_7j") * 100).alias("ecart_percentage")
    )

anomalies.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "anomalies_detected") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .mode("append") \
    .save()


# 6. LOGGING DANS POSTGRESQL
execution_log = spark.createDataFrame([{
    "execution_date": datetime.now(),
    "job_name": "export_postgres",
    "statut": "SUCCESS",
    "nb_lignes_input": df_to_export.count(),
    "nb_lignes_output": df_export.count(),
    "nb_lignes_rejetees": 0,
    "duree_secondes": calcul_duree(),
    "message_erreur": None
}])

execution_log.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "pipeline_logs") \
    .mode("append") \
    .save()
```

**Optimisations appliquées :**
- Batch size optimisé (10000 lignes par batch)
- Connection pooling
- Index PostgreSQL créés en amont
- Gestion des erreurs avec retry

**Métriques collectées :**
- Nombre de lignes exportées
- Durée d'export
- Erreurs rencontrées

**Intérêt technique :**
- Connecteur JDBC Spark
- Écriture optimisée vers RDBMS
- Gestion transactionnelle

---

### 7.6 Récapitulatif des transformations

| Job | Transformations clés | Complexité | Durée estimée (20k lignes) |
|-----|---------------------|------------|---------------------------|
| **Ingestion** | Schéma, validation, Parquet | Faible | 30s |
| **Quality Check** | Dedup, validation, anomalies | Moyenne | 1-2 min |
| **Transformation** | Enrichissement, corrélations, KPIs | Élevée | 3-4 min |
| **Export** | JDBC, agrégations, logging | Moyenne | 1-2 min |
| **TOTAL** | - | - | **~7 min** |

---

## 8. Stratégie de tests

### 8.1 Pyramide de tests

```
           /\
          /  \
         /    \    Tests End-to-End (E2E)
        /      \   • Pipeline complet
       /________\  • 1-2 tests critiques
      /          \
     /            \  Tests d'Intégration
    /              \ • Jobs Spark isolés
   /________________\• 5-10 tests
  /                  \
 /                    \ Tests Unitaires
/______________________\• Fonctions utilitaires
                        • 20-30 tests
```

### 8.2 Tests unitaires

**Fichier : `tests/test_transformations.py`**

```python
import pytest
from pyspark.sql import SparkSession
from spark.jobs.transformation import (
    calculer_score_priorite,
    detecter_doublons,
    normaliser_ref_linky
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("tests") \
        .getOrCreate()


class TestNormalisationRefLinky:
    """Tests de normalisation des références Linky"""
    
    def test_ref_valide(self, spark):
        data = [("LINKY_12345",), ("LINKY_00001",)]
        df = spark.createDataFrame(data, ["ref"])
        
        result = normaliser_ref_linky(df, "ref")
        
        assert result.count() == 2
        assert all(r["ref_norm"].startswith("LINKY_") 
                   for r in result.collect())
    
    def test_ref_invalide_correction(self, spark):
        data = [("12345",), ("LINKY-67890",), (None,)]
        df = spark.createDataFrame(data, ["ref"])
        
        result = normaliser_ref_linky(df, "ref")
        
        # Vérifier que les formats invalides sont corrigés
        results = result.collect()
        assert results[0]["ref_norm"] == "LINKY_12345"
        assert results[1]["ref_norm"] == "LINKY_67890"
        assert results[2]["ref_norm"] is None


class TestDetectionDoublons:
    """Tests de détection des doublons"""
    
    def test_doublons_meme_jour(self, spark):
        data = [
            ("CLI_001", "2024-01-15 10:00:00", "COUPURE"),
            ("CLI_001", "2024-01-15 14:00:00", "COUPURE"),  # Doublon
            ("CLI_002", "2024-01-15 10:00:00", "LINKY"),
        ]
        df = spark.createDataFrame(
            data, 
            ["id_client", "date_creation", "type_reclamation"]
        )
        
        result = detecter_doublons(df)
        
        assert result.count() == 2  # 1 doublon supprimé
    
    def test_pas_doublon_jours_differents(self, spark):
        data = [
            ("CLI_001", "2024-01-15", "COUPURE"),
            ("CLI_001", "2024-01-16", "COUPURE"),  # Pas doublon
        ]
        df = spark.createDataFrame(
            data,
            ["id_client", "date_creation", "type_reclamation"]
        )
        
        result = detecter_doublons(df)
        
        assert result.count() == 2  # Aucune suppression


class TestScorePriorite:
    """Tests de calcul du score de priorité"""
    
    def test_score_priorite_critique(self, spark):
        data = [(
            "CRITIQUE",  # priorite
            True,        # est_recurrent
            True,        # est_lie_incident
            5            # anciennete_jours
        )]
        df = spark.createDataFrame(
            data,
            ["priorite", "est_recurrent", "est_lie_incident", "anciennete_jours"]
        )
        
        result = calculer_score_priorite(df)
        score = result.collect()[0]["score_priorite"]
        
        assert score > 80  # Score élevé attendu
    
    def test_score_priorite_basse(self, spark):
        data = [(
            "BASSE",
            False,
            False,
            1
        )]
        df = spark.createDataFrame(
            data,
            ["priorite", "est_recurrent", "est_lie_incident", "anciennete_jours"]
        )
        
        result = calculer_score_priorite(df)
        score = result.collect()[0]["score_priorite"]
        
        assert score < 40  # Score faible attendu
```

**Fichier : `tests/test_quality_checks.py`**

```python
from spark.jobs.quality_check import (
    valider_format_date,
    valider_coherence_dates,
    calculer_taux_completude
)

class TestValidationDates:
    """Tests de validation des dates"""
    
    def test_dates_coherentes(self, spark):
        data = [
            ("2024-01-15 10:00:00", "2024-01-20 15:00:00"),  # OK
            ("2024-01-15 10:00:00", "2024-01-14 15:00:00"),  # Incohérent
        ]
        df = spark.createDataFrame(data, ["date_creation", "date_cloture"])
        
        result = valider_coherence_dates(df)
        
        assert result.filter("date_coherente = false").count() == 1
    
    def test_format_date_invalide(self, spark):
        data = [
            ("2024-01-15",),      # Format valide
            ("15/01/2024",),      # Format invalide
            ("invalid",),         # Invalide
        ]
        df = spark.createDataFrame(data, ["date_str"])
        
        result = valider_format_date(df, "date_str")
        
        invalides = result.filter("date_valide = false").count()
        assert invalides == 2


class TestCompletudeData:
    """Tests de calcul de complétude"""
    
    def test_taux_completude_100(self, spark):
        data = [
            ("REC_001", "CLI_001", "COUPURE"),
            ("REC_002", "CLI_002", "LINKY"),
        ]
        df = spark.createDataFrame(
            data, 
            ["id_reclamation", "id_client", "type_reclamation"]
        )
        
        taux = calculer_taux_completude(df)
        
        assert taux == 100.0
    
    def test_taux_completude_partiel(self, spark):
        data = [
            ("REC_001", "CLI_001", "COUPURE"),
            ("REC_002", None, "LINKY"),  # Manquant
        ]
        df = spark.createDataFrame(
            data,
            ["id_reclamation", "id_client", "type_reclamation"]
        )
        
        taux = calculer_taux_completude(df, colonne="id_client")
        
        assert taux == 50.0
```

### 8.3 Tests d'intégration

**Fichier : `tests/test_integration_pipeline.py`**

```python
import pytest
from pyspark.sql import SparkSession
from spark.jobs import ingestion, quality_check, transformation

class TestPipelineIntegration:
    """Tests d'intégration du pipeline complet"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        return SparkSession.builder \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    def test_pipeline_ingestion_to_quality(self, spark, tmp_path):
        """Test ingestion → quality check"""
        
        # 1. Créer données de test
        test_csv = tmp_path / "reclamations_test.csv"
        test_csv.write_text(
            "id_reclamation,id_client,type_reclamation,date_creation\n"
            "REC_001,CLI_001,COUPURE,2024-01-15\n"
            "REC_002,CLI_001,COUPURE,2024-01-15\n"  # Doublon
            "REC_003,CLI_002,LINKY,2024-01-16\n"
        )
        
        # 2. Exécuter ingestion
        df_ingested = ingestion.run(spark, str(test_csv))
        assert df_ingested.count() == 3
        
        # 3. Exécuter quality check
        df_cleaned = quality_check.run(spark, df_ingested)
        
        # 4. Vérifications
        assert df_cleaned.count() == 2  # Doublon supprimé
        
    def test_pipeline_quality_to_transformation(self, spark):
        """Test quality check → transformation"""
        
        # Données après quality check
        data_quality = [
            ("REC_001", "CLI_001", "COUPURE", "2024-01-15", 
             48.8566, 2.3522, None),
            ("REC_003", "CLI_002", "LINKY", "2024-01-16",
             48.8700, 2.3400, "INC_001"),
        ]
        df_quality = spark.createDataFrame(
            data_quality,
            ["id_reclamation", "id_client", "type_reclamation", 
             "date_creation", "latitude", "longitude", "ref_incident"]
        )
        
        # Exécuter transformations
        df_transformed = transformation.run(spark, df_quality)
        
        # Vérifications
        assert "score_priorite" in df_transformed.columns
        assert "zone_geographique" in df_transformed.columns
        assert "categorie_auto" in df_transformed.columns
```

### 8.4 Tests End-to-End

**Fichier : `tests/test_e2e.py`**

```python
import pytest
from airflow.models import DagBag
from datetime import datetime

class TestE2EPipeline:
    """Tests end-to-end du pipeline complet"""
    
    def test_dag_chargement_sans_erreur(self):
        """Vérifier que le DAG se charge sans erreur"""
        dagbag = DagBag(dag_folder="airflow/dags/", include_examples=False)
        
        assert len(dagbag.import_errors) == 0
        assert "reclamations_pipeline" in dagbag.dags
    
    def test_dag_structure(self):
        """Vérifier la structure du DAG"""
        dagbag = DagBag(dag_folder="airflow/dags/", include_examples=False)
        dag = dagbag.get_dag("reclamations_pipeline")
        
        # Vérifier les tâches
        task_ids = [task.task_id for task in dag.tasks]
        assert "ingestion" in task_ids
        assert "quality_check" in task_ids
        assert "transformation" in task_ids
        assert "export_postgres" in task_ids
        
        # Vérifier les dépendances
        ingestion_task = dag.get_task("ingestion")
        assert "quality_check" in [t.task_id for t in ingestion_task.downstream_list]
    
    @pytest.mark.integration
    def test_pipeline_complet_donnees_test(self, docker_compose):
        """
        Test du pipeline complet avec données de test
        Nécessite docker-compose en cours d'exécution
        """
        # Ce test nécessite:
        # 1. Docker compose UP
        # 2. Données de test préparées
        # 3. Airflow opérationnel
        
        # Trigger le DAG via API Airflow
        # Vérifier l'exécution complète
        # Valider les données dans PostgreSQL
        pass  # À implémenter si nécessaire
```

### 8.5 Tests de données (Great Expectations)

**Fichier : `tests/data_expectations.py`**

```python
from great_expectations.dataset import SparkDFDataset

def test_reclamations_expectations(spark_df):
    """Expectations sur les données de réclamations"""
    
    ge_df = SparkDFDataset(spark_df)
    
    # Expectations de base
    ge_df.expect_table_row_count_to_be_between(min_value=1000)
    ge_df.expect_column_values_to_not_be_null("id_reclamation")
    ge_df.expect_column_values_to_be_unique("id_reclamation")
    
    # Expectations métier
    ge_df.expect_column_values_to_be_in_set(
        "type_reclamation",
        ["RACCORDEMENT", "LINKY", "COUPURE", "FACTURATION", "INTERVENTION"]
    )
    
    ge_df.expect_column_values_to_be_in_set(
        "statut",
        ["OUVERT", "EN_COURS", "CLOS", "ESCALADE"]
    )
    
    ge_df.expect_column_values_to_match_regex(
        "id_reclamation",
        r"^REC_\d{8}_\d{5}$"
    )
    
    # Expectations sur les valeurs numériques
    ge_df.expect_column_values_to_be_between(
        "latitude",
        min_value=41.0,  # France métropolitaine
        max_value=51.0
    )
    
    ge_df.expect_column_values_to_be_between(
        "score_priorite",
        min_value=0,
        max_value=100
    )
    
    # Expectations conditionnelles
    ge_df.expect_column_pair_values_A_to_be_greater_than_B(
        column_A="date_cloture",
        column_B="date_creation",
        or_equal=True
    )
    
    return ge_df.validate()
```

### 8.6 Stratégie d'exécution des tests

**Commandes de test :**

```bash
# Tests unitaires uniquement (rapide : ~30s)
pytest tests/ -m "not integration" -v

# Tests d'intégration (moyen : ~2-3 min)
pytest tests/ -m integration -v

# Tous les tests
pytest tests/ -v --cov=spark --cov-report=html

# Tests avec rapport de couverture
pytest tests/ --cov=spark --cov-report=term-missing
```

**Critères de succès :**
- ✅ Couverture de code > 70%
- ✅ Tous les tests unitaires passent
- ✅ Tests d'intégration passent (avec Docker)
- ✅ Aucune régression détectée

---

## 9. Préparation entretien technique

### 9.1 Scénarios de démonstration

**Démo 1 : Pipeline complet (5-7 minutes)**

```bash
# 1. Démarrage de l'infrastructure
docker-compose up -d

# 2. Vérification des services
docker-compose ps
# Montrer : Airflow (vert), PostgreSQL (vert), Spark (ready)

# 3. Génération des données
python spark/utils/data_generator.py --nb-reclamations 5000

# 4. Lancement du pipeline via Airflow UI
# Ouvrir http://localhost:8080
# Trigger le DAG reclamations_pipeline
# Montrer les logs en temps réel

# 5. Vérification des résultats
psql -h localhost -U admin -d reclamations_db
SELECT COUNT(*) FROM reclamations_cleaned;
SELECT region, COUNT(*) FROM reclamations_cleaned GROUP BY region;

# 6. Consultation des KPIs
SELECT * FROM kpis_daily ORDER BY date_calcul DESC LIMIT 10;
```

**Démo 2 : Transformation Spark spécifique (3-5 minutes)**

```python
# Montrer une transformation complexe en live
# Exemple : Détection d'anomalies avec window functions

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("demo").getOrCreate()

# Charger les données
df = spark.read.parquet("data/processed/reclamations")

# Calculer la moyenne mobile sur 7 jours
window_7j = Window.partitionBy("zone_geographique") \
    .orderBy(col("date_creation").cast("long")) \
    .rangeBetween(-7*86400, 0)

df_with_avg = df.withColumn(
    "avg_7j",
    avg("nb_reclamations_zone_jour").over(window_7j)
)

# Détecter les anomalies
anomalies = df_with_avg.filter(
    col("nb_reclamations_zone_jour") > col("avg_7j") * 2
)

anomalies.show(10)
```

**Démo 3 : Optimisation de requête (2-3 minutes)**

```python
# Montrer l'impact du broadcast join

# Avant (sans broadcast) - afficher le plan d'exécution
df_join = df_reclamations.join(df_incidents, on="ref_incident")
df_join.explain()

# Après (avec broadcast) - montrer la différence
from pyspark.sql.functions import broadcast
df_join_optimized = df_reclamations.join(
    broadcast(df_incidents), 
    on="ref_incident"
)
df_join_optimized.explain()

# Comparer les temps d'exécution
import time
start = time.time()
df_join.count()
print(f"Sans broadcast: {time.time() - start}s")

start = time.time()
df_join_optimized.count()
print(f"Avec broadcast: {time.time() - start}s")
```

### 9.2 Questions techniques probables et réponses

**Q1 : Pourquoi PySpark plutôt que Pandas pour ce volume de données ?**

**Réponse :**
"Avec 20k lignes, Pandas serait effectivement suffisant. Cependant, j'ai choisi PySpark pour trois raisons :

1. **Scalabilité du code** : Le code que j'ai écrit est 100% compatible avec un cluster Spark. Si demain on passe à 50 millions de réclamations, il suffira de déployer sur un cluster sans réécrire le code.

2. **Démonstration de compétences Big Data** : PySpark est l'outil standard de l'industrie pour le traitement distribué. Cela montre ma maîtrise des concepts de traitement parallèle, lazy evaluation, et optimisation de jobs distribués.

3. **Transformations avancées** : PySpark offre des capacités comme les window functions, broadcast joins, et partition pruning que j'ai exploitées dans le projet pour montrer des transformations métier complexes.

Pour un vrai environnement production avec ce volume, effectivement Pandas + Dask pourrait être une alternative, mais le choix de Spark reste pertinent dans un contexte Big Data."

---

**Q2 : Comment gères-tu les doublons multi-canaux ?**

**Réponse :**
"J'utilise une approche en deux étapes :

1. **Création d'une clé de déduplication composite** :
```python
dedup_key = concat(
    col("id_client"),
    date_format("date_creation", "yyyyMMdd"),
    col("type_reclamation")
)
```
Cela identifie les réclamations du même client, le même jour, pour le même type de problème.

2. **Window function pour sélectionner le 'meilleur' enregistrement** :
```python
window = Window.partitionBy("dedup_key") \
    .orderBy(col("date_creation").asc())
df_dedup = df.withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1)
```

J'ai choisi de garder la première réclamation chronologiquement car elle représente le contact initial du client. Les doublons supprimés sont loggés dans une table d'audit pour traçabilité."

---

**Q3 : Comment optimises-tu les performances Spark sur ton PC ?**

**Réponse :**
"Plusieurs optimisations spécifiques pour un environnement local à ressources limitées :

1. **Configuration mémoire adaptée** :
```python
spark.driver.memory = 1g
spark.executor.memory = 1g
spark.sql.shuffle.partitions = 8  # vs 200 par défaut
```

2. **Utilisation stratégique du cache** :
```python
df_incidents.cache()  # Table petite, réutilisée
```

3. **Broadcast joins pour petites tables** :
```python
df.join(broadcast(df_incidents), on="ref_incident")
```

4. **Coalesce avant écriture** pour éviter trop de petits fichiers :
```python
df.coalesce(4).write.parquet(...)
```

5. **Partitionnement intelligent** : Par date ET région pour équilibrer les partitions et optimiser les requêtes analytiques ultérieures."

---

**Q4 : Comment assures-tu la qualité des données ?**

**Réponse :**
"J'ai implémenté un système de quality checks multi-niveaux :

1. **Niveau structurel** (ingestion) :
- Schéma strict avec typage fort
- Validation des formats (regex pour IDs, codes postaux)
- Détection des lignes malformées

2. **Niveau métier** (quality_check.py) :
- Détection doublons (window functions)
- Validation cohérence dates (date_cloture >= date_creation)
- Contrôle références (format Linky, existence incidents)
- Détection valeurs aberrantes (durée > 90 jours)

3. **Niveau statistique** (transformation.py) :
- Détection d'anomalies avec moyenne mobile sur 7 jours
- Alertes automatiques si écart > 2 σ

4. **Reporting** :
Tous les résultats de qualité sont persistés dans PostgreSQL (table `data_quality_metrics`) pour suivi dans le temps.

J'ai aussi commencé à intégrer Great Expectations pour des expectations déclaratives et reproductibles."

---

**Q5 : Explique ton architecture Airflow. Pourquoi ce choix ?**

**Réponse :**
"J'ai choisi Airflow pour plusieurs raisons :

1. **Standard de l'industrie** : C'est l'orchestrateur le plus utilisé en Big Data
2. **Interface web riche** : Monitoring, logs, retry faciles
3. **Gestion des dépendances** : Les tâches s'exécutent dans le bon ordre avec gestion d'erreurs

Mon DAG suit une structure séquentielle :
```python
ingestion >> quality_check >> transformation >> export_postgres
```

**Points d'attention** :
- Mode Sequential Executor (suffisant pour environnement local)
- Retry automatique (3 tentatives avec délai exponentiel)
- Notifications par email en cas d'échec (configuration)
- Logging centralisé dans PostgreSQL

Pour une vraie production, je passerais sur Celery Executor avec Redis pour du parallélisme, mais ça n'avait pas de sens ici."

---

**Q6 : Comment testerais-tu ce pipeline en production ?**

**Réponse :**
"J'ai mis en place trois niveaux de tests :

1. **Tests unitaires** (30+ tests) :
- Fonctions de transformation isolées
- Validations métier
- Couverture > 70%

2. **Tests d'intégration** :
- Jobs Spark complets avec données de test
- Vérification des outputs

3. **Tests End-to-End** :
- DAG Airflow complet
- Validation données dans PostgreSQL

**En production, j'ajouterais** :
- Tests de régression automatisés (CI/CD)
- Tests de charge (volume 10x-100x)
- Tests de résilience (pannes simulées)
- Monitoring avec alertes (Prometheus + Grafana)
- Data profiling continu (Great Expectations)

Mon approche : **fail fast** en dev avec tests exhaustifs, puis monitoring proactif en prod."

---

**Q7 : Quelles améliorations tu apporterais au projet ?**

**Réponse :**
"Excellente question qui montre que je comprends les limites du projet actuel.

**Court terme (2-4 semaines)** :
1. **Dashboard Streamlit** pour visualiser les KPIs en temps réel
2. **Alerting automatique** (email/Slack) sur anomalies détectées
3. **CI/CD avec GitHub Actions** (tests auto + déploiement)
4. **Data lineage** avec Apache Atlas ou similaire

**Moyen terme (2-3 mois)** :
1. **Streaming avec Kafka** pour traitement temps réel des réclamations urgentes
2. **Machine Learning** : prédiction du délai de traitement, classification automatique améliorée
3. **Data Lake** avec partitionnement Delta Lake pour historisation et time travel
4. **API REST** avec FastAPI pour exposer les données

**Long terme (architecture évolutive)** :
1. **Migration cloud** (AWS EMR / Azure Databricks)
2. **Kubernetes** pour orchestration conteneurs
3. **Feature store** pour le ML
4. **Data mesh** si l'organisation grandit

Je suis conscient que ce projet est un MVP. Ces évolutions montrent ma vision d'une plateforme data mature."

---

### 9.3 Points clés à mentionner durant l'entretien

**Architecture :**
- ✅ "J'ai conçu une architecture modulaire et scalable"
- ✅ "Code 100% compatible avec un déploiement cluster"
- ✅ "Séparation claire des responsabilités (ingestion, quality, transformation, export)"

**Choix techniques :**
- ✅ "PySpark pour la scalabilité future même si le volume actuel ne le nécessite pas"
- ✅ "PostgreSQL pour l'écosystème riche (ACID, index, vues matérialisées)"
- ✅ "Parquet avec partitionnement pour optimiser les requêtes analytiques"

**Qualité :**
- ✅ "Approche quality-first avec contrôles multi-niveaux"
- ✅ "Tests unitaires et intégration avec couverture >70%"
- ✅ "Logging centralisé pour observabilité"

**Métier :**
- ✅ "Compréhension du métier énergétique (incidents réseau, compteurs Linky)"
- ✅ "KPIs pertinents (SLA, taux récurrence, détection anomalies)"
- ✅ "Corrélations métier (réclamations ↔ incidents géolocalisés)"

**Évolutivité :**
- ✅ "Vision claire des améliorations possibles"
- ✅ "Compréhension des limites actuelles"
- ✅ "Capacité à passer à l'échelle (streaming, ML, cloud)"

---

### 9.4 Démonstration visuelle recommandée

**Ordre de présentation idéal (10-15 minutes) :**

1. **Intro (1 min)** : Contexte métier, objectifs du projet
   
2. **Architecture (2 min)** : Schéma, flux de données, justifications
   
3. **Démo live (5 min)** :
   - Lancer le pipeline via Airflow UI
   - Montrer les logs en temps réel
   - Requêter PostgreSQL pour montrer les résultats
   
4. **Code walk-through (3 min)** :
   - Montrer 1-2 transformations complexes
   - Expliquer les optimisations
   
5. **Tests et qualité (2 min)** :
   - Exécuter les tests
   - Montrer la couverture de code
   
6. **Axes d'amélioration (2 min)** :
   - Vision d'évolution
   - Limites identifiées

---

### 9.5 Checklist avant l'entretien

**Préparation technique :**
- [ ] Le projet démarre en <5 min avec `docker-compose up -d`
- [ ] Les données de test sont générées et cohérentes
- [ ] Le pipeline s'exécute sans erreur end-to-end
- [ ] Les tests passent tous
- [ ] Le README est complet et professionnel

**Préparation orale :**
- [ ] Répéter la présentation (10-15 min chrono)
- [ ] Connaître le code par cœur (pouvoir naviguer sans chercher)
- [ ] Anticiper 10-15 questions techniques
- [ ] Préparer des réponses sur les choix d'architecture
- [ ] Être capable d'expliquer chaque transformation Spark

**Documents à avoir sous la main :**
- [ ] Architecture diagram imprimé
- [ ] Liste des transformations avec exemples de code
- [ ] Schéma de la base de données PostgreSQL
- [ ] Liste des KPIs calculés
- [ ] Roadmap d'évolution du projet

---

## 10. Conclusion

Ce document constitue la **feuille de route complète** de ton projet portfolio Big Data. Il couvre :

✅ **L'architecture technique** de bout en bout
✅ **Le modèle de données** détaillé
✅ **L'organisation du code** professionnelle
✅ **La roadmap de développement** sur 8 semaines
✅ **Les transformations PySpark** complexes
✅ **La stratégie de tests** multi-niveaux
✅ **La préparation à l'entretien** technique

**Prochaines étapes recommandées :**

1. **Semaine 1-2** : Générer les données et créer le job d'ingestion
2. **Semaine 3-4** : Implémenter les transformations métier
3. **Semaine 5** : Mettre en place PostgreSQL et l'export
4. **Semaine 6** : Orchestrer avec Airflow
5. **Semaine 7** : Dockeriser l'ensemble
6. **Semaine 8** : Finaliser et documenter

Ce projet démontre une maîtrise **end-to-end** d'une stack Big Data moderne et sera un atout majeur pour tes entretiens techniques. Bonne chance ! 🚀

---

**Version du document** : 1.0
**Dernière mise à jour** : Janvier 2026
**Auteur** : [Ton nom]
**Contact** : [Ton email]