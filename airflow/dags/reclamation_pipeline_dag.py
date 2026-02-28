"""
DAG Airflow : Pipeline de traitement des réclamations clients.
Orchestre l'ingestion, quality check, transformations et export.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

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

dag = DAG(
    'reclamation_pipeline',
    default_args=default_args,
    description='Pipeline complet de traitement des réclamations clients',
    schedule='@daily',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    is_paused_upon_creation=False,
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
    Charge les fichiers CSV bruts et les convertit en Parquet.
    - Entrées : `data/raw/reclamations.csv`, `incidents.csv`, `clients.csv`
    - Sorties : `data/raw_parquet/reclamations/`, `incidents/`, `clients/`
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
    Vérifie complétude, unicité, validité et cohérence. Seuil : 90%.
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
    1. Calcul durées de traitement
    2. Scores de priorité
    3. Détection réclamations récurrentes
    4. Corrélation incidents réseau
    5. Détection anomalies statistiques
    - Sortie : `data/processed/reclamations/` (partitionné région/année/mois)
    """,
)

# ==============================================================================
# TÂCHE 4 : Export PostgreSQL (Spark CSV+COPY, même pattern que medallion)
# ==============================================================================
task_export_postgres = BashOperator(
    task_id='export_postgres',
    bash_command='''
        python3 /opt/airflow/spark/jobs/export_reclamation_processed_csv.py
    ''',
    dag=dag,
    doc_md="""
    ### Export vers PostgreSQL
    Pattern Spark → CSV → COPY (même que medallion_pipeline_daily).
    Lit depuis `data/processed/reclamations` (sortie de transformation.py).
    - `reclamations.reclamations_cleaned` : réclamations enrichies
    """,
)

# ==============================================================================
# TÂCHE 5 : Notification de succès (même pattern que medallion_pipeline_daily)
# ==============================================================================
def send_success_notification(**context):
    """Log succès dans PostgreSQL — même pattern que medallion_pipeline_daily."""
    from datetime import datetime, timezone
    import psycopg2

    dag_run = context.get('dag_run')

    try:
        conn = psycopg2.connect(
            host="reclamations-postgres",
            database="reclamations_db",
            user="airflow",
            password="airflow_local_dev"
        )
        cur = conn.cursor()

        # Stats réelles depuis PostgreSQL
        cur.execute("SELECT COUNT(*) FROM reclamations.reclamations_processed")
        nb_reclamations = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM reclamations.kpis_daily")
        nb_kpis = cur.fetchone()[0]

        # ✅ Fix timezone : datetime.now(UTC) pour compatibilité Airflow 3.x
        now = datetime.now(timezone.utc)
        start = dag_run.start_date
        duration = (now - start).total_seconds() if start else 0

        cur.execute("""
            INSERT INTO reclamations.pipeline_runs
            (dag_id, run_id, status, start_date, end_date, duration_seconds, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            dag_run.dag_id,
            dag_run.run_id,
            'SUCCESS',
            start,
            now,
            duration,
            f"✅ Pipeline terminé : {nb_reclamations:,} réclamations + {nb_kpis:,} KPIs"
        ))

        conn.commit()

        print(f"""
╔══════════════════════════════════════════╗
║   ✅ PIPELINE TERMINÉ AVEC SUCCÈS       ║
╚══════════════════════════════════════════╝

📊 Résultat :
   • Réclamations : {nb_reclamations:,}
   • KPIs         : {nb_kpis:,}
   • Durée        : {duration:.0f}s

✅ Log sauvegardé dans reclamations.pipeline_runs
        """)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"⚠️ Erreur notification : {e}")
        raise


task_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# ==============================================================================
# DÉPENDANCES
# ==============================================================================
task_ingestion >> task_quality_check >> task_transformation >> task_export_postgres >> task_notification

# ==============================================================================
# DOCUMENTATION DU DAG
# ==============================================================================
dag.doc_md = """
# Pipeline de Traitement des Réclamations Clients

## 📋 Vue d'ensemble
DAG quotidien orchestrant le traitement des réclamations clients pour un opérateur
d'infrastructure énergétique.

## 🔄 Flux de données
```
CSV Sources → Ingestion → Quality Check → Transformations → PostgreSQL
                  ↓            ↓               ↓                ↓
              Parquet      Métriques       Parquet         reclamations_cleaned
                           qualité         enrichi              + pipeline_runs
                                       (processed/)
```

## ⏱️ Scheduling
- **Fréquence** : Quotidienne (`@daily`)
- **Durée estimée** : 15-30 minutes
- **Retry** : 3 tentatives avec 5 min d'intervalle

## 📊 Tables PostgreSQL mises à jour
- `reclamations.reclamations_cleaned` : réclamations enrichies
- `reclamations.pipeline_runs` : logs d'exécution

## 📝 Note architecture
Ce DAG traite les réclamations brutes → enrichies.
Les KPIs sont calculés et exportés par `medallion_pipeline_daily`.
"""
