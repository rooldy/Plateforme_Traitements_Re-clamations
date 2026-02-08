# Projet : Plateforme de Traitement Automatisé de Réclamations Clients

## 🎯 Contexte et Objectif

Je souhaite développer un projet portfolio Big Data pour démontrer mes compétences en Data Engineering. Le projet simule une plateforme de traitement automatisé de réclamations clients pour un opérateur d'infrastructure (type Enedis).

**Nom du projet** : `Plateforme_de_traitement_automatisé_de_réclamations_clients`

**Objectif principal** : Construire une infrastructure data complète (du build au run) pour automatiser l'ingestion, la transformation, le stockage et le monitoring de réclamations clients.

---

## 🛠️ Stack Technique Ciblée

### Technologies principales :
- **Processing** : PySpark (mode local, pas de cluster Hadoop)
- **Orchestration** : Apache Airflow
- **Stockage** : PostgreSQL
- **Containerisation** : Docker + Docker Compose
- **Versioning** : Git/GitHub
- **CI/CD** : (Optionnel) Jenkins ou GitHub Actions
- **Monitoring** : Logs centralisés dans PostgreSQL

### Contraintes techniques :
- Environnement local (PC personnel)
- Pas d'HDFS (remplacé par stockage fichier local + Parquet)
- Architecture légère mais professionnelle
- Code 100% compatible avec un déploiement sur cluster réel

---

## 📊 Cas d'Usage : Réclamations Clients (Secteur Énergie)

### Types de réclamations à simuler :
1. **Raccordement au réseau** : retards, travaux non terminés
2. **Compteurs Linky** : dysfonctionnements, erreurs de données
3. **Coupures électriques** : pannes répétées, délais de rétablissement
4. **Facturation** : contestations, erreurs de relevé
5. **Interventions techniques** : dommages, problèmes de qualité

### Données à générer (synthétiques) :
- ID réclamation, client, type, description
- Canal (téléphone, email, web, agence)
- Localisation (région, adresse avec géocodage)
- Dates (création, clôture), statut, priorité
- Références techniques (compteur Linky, etc.)
- Volume : 10k-50k réclamations pour tests

---

## 🏗️ Architecture Cible

```
┌─────────────────────────┐
│   Données Synthétiques  │
│   (CSV - data/raw/)     │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   PySpark (mode local)  │ ◄─── Orchestré par Airflow
│   - Ingestion           │
│   - Transformations     │
│   - Quality checks      │
└───────────┬─────────────┘
            │
            ├──► Parquet partitionné (data/processed/)
            │
            └──► PostgreSQL (stockage final)
                         │
                         ▼
                 ┌───────────────┐
                 │  Monitoring   │
                 │  (Logs + KPIs)│
                 └───────────────┘
```

---

## 🔄 Transformations PySpark Attendues

### 1. Qualité et dédoublonnage :
- Détection et suppression des doublons multi-canaux
- Gestion des valeurs manquantes
- Validation des formats et références

### 2. Enrichissement :
- Normalisation des références compteurs Linky
- Géocodage des adresses clients
- Catégorisation automatique des réclamations

### 3. Corrélations métier :
- Rapprochement avec incidents réseau géolocalisés
- Jointure avec historique client (réclamations récurrentes)
- Agrégations par zone géographique et type

### 4. KPIs et calculs métier :
- Calcul des durées de traitement (SLA monitoring)
- Score de priorité par réclamation
- Détection d'anomalies statistiques (pics inhabituels)
- Métriques temporelles (délai première réponse, etc.)

### 5. Partitionnement et optimisation :
- Partitionnement par date et région
- Filtrage par statut (ouvert/clôturé)
- Optimisation des performances

---

## 📁 Structure de Projet Souhaitée

```
Plateforme_de_traitement_automatisé_de_réclamations_clients/
├── data/
│   ├── raw/                    # CSV bruts
│   └── processed/              # Parquet partitionnés
├── spark/
│   ├── jobs/
│   │   ├── ingestion.py
│   │   ├── transformation.py
│   │   ├── quality_check.py
│   │   └── export_postgres.py
│   └── utils/
│       ├── spark_config.py
│       └── data_generator.py   # Génération données synthétiques
├── airflow/
│   ├── dags/
│   │   └── reclamation_pipeline_dag.py
│   └── config/
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.spark
│   └── Dockerfile.airflow
├── sql/
│   ├── schema.sql              # Schéma PostgreSQL
│   └── indexes.sql             # Index et optimisations
├── monitoring/
│   └── log_ingestion.py        # Centralisation logs
├── tests/
│   ├── test_transformations.py
│   └── test_quality.py
├── docs/
│   ├── architecture.md
│   └── images/
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 🎯 Livrables Attendus

### MVP (Priorité 1) :
✅ Génération de données synthétiques réalistes
✅ Pipeline PySpark complet (ingestion → transformation → export)
✅ Export vers PostgreSQL
✅ DAG Airflow fonctionnel
✅ Docker Compose opérationnel (Spark + Airflow + PostgreSQL)
✅ README avec architecture et instructions

### Version complète (Priorité 2) :
✅ Tests unitaires sur les transformations
✅ Monitoring et logs centralisés
✅ Optimisations PostgreSQL (index, vues, partitionnement)
✅ Dashboard simple (Streamlit ou Grafana)
✅ Documentation technique détaillée
✅ CI/CD basique (GitHub Actions)

---

## ⏱️ Timeline et Contraintes

**Date de l'entretien** : [À préciser]
**Temps disponible** : [À préciser]
**Contraintes matérielles** : PC personnel, [RAM/CPU à préciser si limité]

---

## 🎓 Objectifs Pédagogiques

Ce projet doit me permettre de :
1. **Démontrer ma maîtrise end-to-end** d'une infrastructure Big Data
2. **Avoir du code concret à montrer** en entretien technique
3. **Pouvoir répondre avec précision** aux questions sur les choix d'architecture
4. **Aligner mon portfolio** avec mon pitch professionnel sur Enedis
5. **Créer un repo GitHub** comme preuve tangible de compétences

---

## 📝 Questions pour Démarrer

1. Peux-tu me proposer une **architecture technique détaillée** optimisée pour mon PC ?
2. Peux-tu créer le **script de génération de données synthétiques** (10k-50k réclamations) ?
3. Peux-tu développer les **jobs PySpark** avec les transformations décrites ?
4. Peux-tu créer le **docker-compose.yml** complet et optimisé ?
5. Peux-tu rédiger le **DAG Airflow** pour orchestrer le pipeline ?
6. Peux-tu préparer le **README** et la documentation ?

---

## 💡 Notes Supplémentaires

- Le code doit être **propre, commenté et professionnel**
- Privilégier la **simplicité et la maintenabilité**
- Tout doit être **facilement démontrable** (pas de config complexe)
- Le projet doit pouvoir tourner sur un **PC standard** (8GB RAM minimum idéalement)
- Prêt à faire des **compromis sur la volumétrie** pour garantir les performances locales