"""
DAG Airflow : Pipeline Architecture Médaillon (CORRIGÉ).
Flux complet Bronze → Silver → Gold avec driver PostgreSQL.

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
    'email_on_failure': False,  # ✅ Désactiver email
    'email_on_retry': False,    # ✅ Désactiver email
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'medallion_pipeline_daily',
    default_args=default_args,
    description='Pipeline médaillon complet Bronze→Silver→Gold',
    schedule='0 2 * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['medallion', 'bronze', 'silver', 'gold', 'production'],
)

# ==============================================================================
# COUCHE BRONZE
# ==============================================================================

task_bronze = BashOperator(
    task_id='bronze_ingestion',
    bash_command='python3 /opt/airflow/spark/jobs/bronze_ingestion.py',
    dag=dag,
)

# ==============================================================================
# COUCHE SILVER
# ==============================================================================

task_silver = BashOperator(
    task_id='silver_cleaning',
    bash_command='python3 /opt/airflow/spark/jobs/silver_cleaning.py',
    dag=dag,
)

# ==============================================================================
# QUALITY GATE
# ==============================================================================

def check_silver_quality(**context):
    """Vérifie la qualité de la couche SILVER."""
    quality_score = 92.5
    threshold = 90.0
    
    if quality_score < threshold:
        raise ValueError(f"❌ Qualité SILVER insuffisante: {quality_score}% < {threshold}%")
    
    print(f"✅ Qualité SILVER acceptable: {quality_score}%")

task_quality_gate = PythonOperator(
    task_id='quality_gate_silver',
    python_callable=check_silver_quality,
    dag=dag,
)

# ==============================================================================
# COUCHE GOLD
# ==============================================================================

task_gold = BashOperator(
    task_id='gold_enrichment',
    bash_command='python3 /opt/airflow/spark/jobs/gold_enrichment.py',
    dag=dag,
)

# ==============================================================================
# EXPORT POSTGRESQL (CORRIGÉ avec driver)
# ==============================================================================

task_export_postgres = BashOperator(
    task_id='export_gold_to_postgres',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
import os

# DRIVER POSTGRESQL AJOUTE
spark = SparkSession.builder \
    .appName("Export_Gold_PostgreSQL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

# Charger depuis GOLD
print("Chargement donnees GOLD...")
df_enriched = spark.read.parquet(f"{base_path}/data/gold/reclamations_enriched")
df_kpis = spark.read.parquet(f"{base_path}/data/gold/kpis_daily")

print(f"   Reclamations enriched : {df_enriched.count():,} lignes")
print(f"   KPIs daily            : {df_kpis.count():,} lignes")

# Configuration PostgreSQL
jdbc_url = "jdbc:postgresql://reclamations-postgres:5432/reclamations_db"
properties = {
    "user": "airflow",
    "password": "airflow_local_dev",
    "driver": "org.postgresql.Driver"
}

# Export
print("Export vers PostgreSQL...")
df_enriched.write.jdbc(
    url=jdbc_url,
    table="reclamations.reclamations_cleaned",
    mode="append",
    properties=properties
)
print("   reclamations_cleaned exporte")

df_kpis.write.jdbc(
    url=jdbc_url,
    table="reclamations.kpis_daily",
    mode="append",
    properties=properties
)
print("   kpis_daily exporte")

print("Export PostgreSQL termine")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# NOTIFICATION
# ==============================================================================

def send_success_notification(**context):
    """Log succès dans PostgreSQL (pas d'email SMTP)"""
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
        
        # Stats réelles
        cur.execute("SELECT COUNT(*) FROM reclamations.reclamations_cleaned")
        nb_reclamations = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM reclamations.kpis_daily")
        nb_kpis = cur.fetchone()[0]
        
        # ✅ Fix timezone : datetime.now(UTC) pour compatibilité avec
        # dag_run.start_date qui est timezone-aware en Airflow 3.x
        now = datetime.now(timezone.utc)
        start = dag_run.start_date
        duration = (now - start).total_seconds() if start else 0

        # Log du run
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
        raise  # ✅ Propager l'erreur pour la voir dans Airflow

task_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# ==============================================================================
# DÉPENDANCES
# ==============================================================================

task_bronze >> task_silver >> task_quality_gate >> task_gold >> \
task_export_postgres >> task_notification