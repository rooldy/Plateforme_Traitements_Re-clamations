"""
DAG Airflow : Pipeline de traitement des réclamations clients.
Orchestre l'ingestion, quality check, transformations et export.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Configuration par défaut du DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Définition du DAG
dag = DAG(
    'reclamation_pipeline',
    default_args=default_args,
    description='Pipeline complet de traitement des réclamations clients',
    schedule_interval='@daily',  # Exécution quotidienne
    start_date=datetime(2025, 2, 1),
    catchup=False,  # Ne pas rattraper les exécutions passées
    is_paused_upon_creation=False,  # DAG actif dès la création
    max_active_runs=1,
    tags=['reclamations', 'pyspark', 'production'],
)

# ==============================================================================
# TÂCHE 1 : Ingestion des données brutes
# ==============================================================================
task_ingestion = BashOperator(
    task_id='ingestion_data',
    bash_command='''
        cd /opt/airflow/spark/jobs && \
        python3 ingestion.py
    ''',
    dag=dag,
    doc_md="""
    ### Ingestion des données sources
    
    **Objectif :** Charger les fichiers CSV bruts et les convertir en Parquet.
    
    **Entrées :**
    - `data/raw/reclamations.csv`
    - `data/raw/incidents.csv`
    - `data/raw/clients.csv`
    
    **Sorties :**
    - `data/raw_parquet/reclamations/`
    - `data/raw_parquet/incidents/`
    - `data/raw_parquet/clients/`
    
    **Vérifications :**
    - Schéma strict validé
    - Types de données correctsContrôles :
    - Détection anomalies structurelles
    - Logging dans PostgreSQL
    """,
)

# ==============================================================================
# TÂCHE 2 : Quality Check
# ==============================================================================
task_quality_check = BashOperator(
    task_id='quality_check',
    bash_command='''
        cd /opt/airflow/spark/jobs && \
        python3 quality_check.py
    ''',
    dag=dag,
    doc_md="""
    ### Contrôles qualité des données
    
    **Objectif :** Vérifier la qualité des données ingérées.
    
    **Contrôles effectués :**
    - Complétude (valeurs nulles)
    - Unicité (doublons)
    - Validité (référentiel)
    - Cohérence (dates, formats Linky)
    
    **Seuil de qualité :** 90%
    
    **Actions si échec :**
    - Arrêt du pipeline
    - Alerte équipe data
    - Métriques sauvegardées dans `data_quality_metrics`
    """,
)

# ==============================================================================
# TÂCHE 3 : Transformations métier
# ==============================================================================
task_transformation = BashOperator(
    task_id='transformation',
    bash_command='''
        cd /opt/airflow/spark/jobs && \
        python3 transformation.py
    ''',
    dag=dag,
    doc_md="""
    ### Transformations et enrichissements
    
    **Objectif :** Enrichir les données et calculer les métriques métier.
    
    **Transformations appliquées :**
    1. Calcul durées de traitement
    2. Calcul scores de priorité
    3. Détection réclamations récurrentes
    4. Corrélation avec incidents réseau
    5. Détection d'anomalies statistiques
    
    **Sorties :**
    - `data/processed/reclamations/` (partitionné par région/année/mois)
    - Anomalies dans `anomalies_detected`
    """,
)

# ==============================================================================
# TÂCHE 4 : Export PostgreSQL
# ==============================================================================
task_export_postgres = BashOperator(
    task_id='export_postgres',
    bash_command='''
        cd /opt/airflow/spark/jobs && \
        python3 export_postgres.py
    ''',
    dag=dag,
    doc_md="""
    ### Export vers PostgreSQL
    
    **Objectif :** Exporter les données finales vers la base de données.
    
    **Tables mises à jour :**
    - `reclamations_cleaned` : Réclamations enrichies
    - `kpis_daily` : KPIs quotidiens par région/type
    - `clients` : Statistiques clients
    
    **Post-traitement :**
    - Rafraîchissement des vues matérialisées
    - Logging de l'exécution
    """,
)

# ==============================================================================
# TÂCHE 5 : Notification de succès (optionnel)
# ==============================================================================
def send_success_notification(**context):
    """
    Envoie une notification de succès.
    Peut être étendu avec Slack, email, etc.
    """
    execution_date = context['execution_date']
    print(f"✅ Pipeline exécuté avec succès pour {execution_date}")
    print(f"📊 Réclamations traitées et disponibles dans PostgreSQL")
    # TODO: Ajouter envoi Slack/email si besoin

task_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# ==============================================================================
# DÉFINITION DES DÉPENDANCES
# ==============================================================================

# Pipeline séquentiel :
# Ingestion → Quality Check → Transformation → Export → Notification

task_ingestion >> task_quality_check >> task_transformation >> task_export_postgres >> task_notification

# ==============================================================================
# DOCUMENTATION DU DAG
# ==============================================================================

dag.doc_md = """
# Pipeline de Traitement des Réclamations Clients

## 📋 Vue d'ensemble

Ce DAG orchestre le traitement quotidien des réclamations clients pour un opérateur 
d'infrastructure énergétique.

## 🔄 Flux de données

```
CSV Sources → Ingestion → Quality Check → Transformations → PostgreSQL
                  ↓            ↓              ↓                ↓
              Parquet      Métriques      Parquet         Tables finales
                         qualité         enrichi              + KPIs
```

## ⏱️ Scheduling

- **Fréquence** : Quotidienne (`@daily`)
- **Heure d'exécution** : 02:00 AM (UTC)
- **Durée estimée** : 15-30 minutes
- **Retry** : 3 tentatives avec 5 min d'intervalle

## 📊 Métriques produites

1. **Réclamations enrichies** : Toutes les réclamations avec calculs métier
2. **KPIs quotidiens** : Agrégations par région/type
3. **Anomalies** : Détection automatique de pics inhabituels
4. **Qualité** : Métriques de qualité des données

## 🚨 Alertes

- Échec quality check → Arrêt du pipeline
- Échec transformation → Retry automatique
- Succès → Notification (optionnel)

## 👥 Contacts

- **Owner** : Data Engineering Team
- **Email** : admin@example.com
- **Slack** : #data-engineering

## 📚 Documentation

Pour plus d'informations, consulter :
- [README du projet](../README.md)
- [Architecture technique](../docs/architecture.md)
- [Guide des transformations](../docs/transformations.md)
"""

# ==============================================================================
# NOTES TECHNIQUES
# ==============================================================================

"""
### Optimisations appliquées

1. **Partitionnement Parquet** : Par région/année/mois pour requêtes rapides
2. **Cache Spark** : Utilisation intelligente du cache sur données réutilisées
3. **Broadcast join** : Pour corrélation avec incidents (petite table)
4. **Vues matérialisées** : Rafraîchies automatiquement

### Bonnes pratiques

- ✅ Logs centralisés dans PostgreSQL
- ✅ Métriques de qualité sauvegardées
- ✅ Retry automatique sur erreurs temporaires
- ✅ Idempotence des jobs (re-exécution safe)

### Évolutions futures

- [ ] Ajout alerting Slack/email
- [ ] Dashboard Streamlit des KPIs
- [ ] ML pour prédiction délai traitement
- [ ] Streaming Kafka pour réclamations urgentes
"""
