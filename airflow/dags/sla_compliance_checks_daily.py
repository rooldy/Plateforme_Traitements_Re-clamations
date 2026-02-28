"""
DAG Airflow : Vérification conformité SLA.
Surveille le respect des SLA et génère des alertes.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'sla-monitoring',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email': ['sla-alerts@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'sla_compliance_checks_daily',
    default_args=default_args,
    description='Vérification quotidienne de la conformité SLA',
    schedule='0 6 * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['sla', 'monitoring', 'alerts', 'production'],
)

# ==============================================================================
# TÂCHE 1 : Attendre calcul KPIs
# ==============================================================================
# ==============================================================================
# TÂCHE 2 : Identifier réclamations hors SLA
# ==============================================================================
task_identify_breaches = BashOperator(
    task_id='identify_sla_breaches',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

spark = SparkSession.builder \
    .appName("SLA_Breaches") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

df_sla = df.withColumn("sla_limite_heures",
    when(col("type_reclamation") == "COUPURE_ELECTRIQUE", 48)
    .when(col("type_reclamation") == "COMPTEUR_LINKY", 120)
    .when(col("type_reclamation") == "FACTURATION", 240)
    .when(col("type_reclamation") == "RACCORDEMENT_RESEAU", 360)
    .when(col("type_reclamation") == "INTERVENTION_TECHNIQUE", 168)
    .otherwise(168)
)

df_breaches = df_sla.filter(
    (col("statut") != "CLOTURE") &
    (col("duree_traitement_heures") > col("sla_limite_heures"))
).select(
    "id_reclamation", "client_id", "type_reclamation", "priorite", "statut",
    "region", "date_creation", "duree_traitement_heures", "sla_limite_heures",
    (col("duree_traitement_heures") - col("sla_limite_heures")).alias("depassement_heures")
)

nb_breaches = df_breaches.count()
print(f"🚨 {nb_breaches} réclamations hors SLA détectées")

if nb_breaches > 0:
    df_breaches.orderBy(col("depassement_heures").desc()).show(20, truncate=False)
    df_breaches.write.mode("overwrite").parquet(f"{base_path}/data/sla/breaches_daily")

spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 3 : Calculer taux conformité par région
# ==============================================================================
task_compliance_rate = BashOperator(
    task_id='calculate_compliance_rates',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, current_date
import os

spark = SparkSession.builder \
    .appName("SLA_Compliance_Rate") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

df_sla = df.withColumn("sla_limite_heures",
    when(col("type_reclamation") == "COUPURE_ELECTRIQUE", 48)
    .when(col("type_reclamation") == "COMPTEUR_LINKY", 120)
    .when(col("type_reclamation") == "FACTURATION", 240)
    .when(col("type_reclamation") == "RACCORDEMENT_RESEAU", 360)
    .when(col("type_reclamation") == "INTERVENTION_TECHNIQUE", 168)
    .otherwise(168)
)

compliance = df_sla.filter(col("duree_traitement_heures").isNotNull()) \
    .groupBy("region", "type_reclamation").agg(
        count("*").alias("total_reclamations"),
        count(when(col("duree_traitement_heures") <= col("sla_limite_heures"), 1)).alias("nb_conformes"),
        count(when(col("duree_traitement_heures") > col("sla_limite_heures"), 1)).alias("nb_non_conformes")
    ) \
    .withColumn("taux_conformite",
        (col("nb_conformes") / col("total_reclamations") * 100)) \
    .withColumn("date_calcul", current_date())

print("📊 Taux de conformité SLA par région :")
compliance.orderBy(col("taux_conformite")).show(50, truncate=False)
compliance.write.mode("overwrite").parquet(f"{base_path}/data/sla/compliance_rates_daily")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 4 : Identifier réclamations à risque
# ==============================================================================
task_at_risk = BashOperator(
    task_id='identify_at_risk_reclamations',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

spark = SparkSession.builder \
    .appName("SLA_At_Risk") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

df_sla = df.withColumn("sla_limite_heures",
    when(col("type_reclamation") == "COUPURE_ELECTRIQUE", 48)
    .when(col("type_reclamation") == "COMPTEUR_LINKY", 120)
    .when(col("type_reclamation") == "FACTURATION", 240)
    .when(col("type_reclamation") == "RACCORDEMENT_RESEAU", 360)
    .when(col("type_reclamation") == "INTERVENTION_TECHNIQUE", 168)
    .otherwise(168)
)

df_at_risk = df_sla.filter(col("statut").isin(["OUVERT", "EN_COURS"])) \
    .withColumn("temps_restant_heures",
        col("sla_limite_heures") - col("duree_traitement_heures")) \
    .filter(col("temps_restant_heures") <= 24) \
    .select(
        "id_reclamation", "client_id", "type_reclamation", "priorite", "statut",
        "region", "date_creation", "duree_traitement_heures",
        "sla_limite_heures", "temps_restant_heures"
    )

nb_at_risk = df_at_risk.count()
print(f"⚠️  {nb_at_risk} réclamations à risque (< 24h avant SLA)")

if nb_at_risk > 0:
    df_at_risk.orderBy("temps_restant_heures").show(20, truncate=False)
    df_at_risk.write.mode("overwrite").parquet(f"{base_path}/data/sla/at_risk_daily")

spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 5 : Générer alertes (données réelles depuis Parquet)
# ==============================================================================
def generate_sla_alerts(**context):
    """Génère les alertes SLA à partir des données réelles."""
    import os
    import pandas as pd
    from datetime import datetime, timezone

    dag_run   = context.get('dag_run')
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    # ✅ Compatible Airflow 3.x
    logical_date = dag_run.logical_date if dag_run else None

    # Lire données réelles depuis Parquet (pas de valeurs simulées)
    try:
        df_breaches = pd.read_parquet(f"{base_path}/data/sla/breaches_daily")
        nb_breaches = len(df_breaches)
    except Exception:
        nb_breaches = 0

    try:
        df_at_risk = pd.read_parquet(f"{base_path}/data/sla/at_risk_daily")
        nb_at_risk = len(df_at_risk)
    except Exception:
        nb_at_risk = 0

    date_str = logical_date.strftime('%Y-%m-%d') if logical_date else 'N/A'
    print(f"\n{'='*60}")
    print(f"🚨 RAPPORT SLA - {date_str}")
    print(f"{'='*60}")
    print(f"❌ Dépassements SLA détectés     : {nb_breaches}")
    print(f"⚠️  Réclamations à risque (<24h) : {nb_at_risk}")
    print(f"{'='*60}\n")

    if nb_breaches > 50:
        print("🚨 ALERTE CRITIQUE : Plus de 50 dépassements SLA !")
        # TODO: Envoyer alerte urgente Slack/PagerDuty

    if nb_at_risk > 100:
        print("⚠️  ALERTE : Plus de 100 réclamations à risque !")
        # TODO: Envoyer notification équipe

task_alerts = PythonOperator(
    task_id='generate_alerts',
    python_callable=generate_sla_alerts,
    dag=dag,
)

# ==============================================================================
# TÂCHE 6 : Sauvegarder rapport (psycopg2, pattern medallion)
# ==============================================================================
def save_sla_report(**context):
    """Sauvegarde le rapport SLA dans PostgreSQL — pattern medallion."""
    import os
    import pandas as pd
    import psycopg2
    from datetime import datetime, timezone

    dag_run   = context.get('dag_run')
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

    # Lire données réelles depuis Parquet
    try:
        nb_breaches = len(pd.read_parquet(f"{base_path}/data/sla/breaches_daily"))
    except Exception:
        nb_breaches = 0

    try:
        nb_at_risk = len(pd.read_parquet(f"{base_path}/data/sla/at_risk_daily"))
    except Exception:
        nb_at_risk = 0

    now      = datetime.now(timezone.utc)
    start    = dag_run.start_date if dag_run else now
    duration = (now - start).total_seconds()
    statut   = "ALERTE" if nb_breaches > 50 else "NORMAL"
    message  = f"✅ SLA vérifié : {nb_breaches} dépassements, {nb_at_risk} à risque — {statut}"

    try:
        conn = psycopg2.connect(
            host="reclamations-postgres", port=5432,
            database="reclamations_db",
            user="airflow", password="airflow_local_dev"
        )
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO reclamations.pipeline_runs
            (dag_id, run_id, status, start_date, end_date, duration_seconds, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            dag_run.dag_id, dag_run.run_id, 'SUCCESS',
            start, now, duration, message
        ))

        # Log dans pipeline_logs si la table existe
        try:
            cur.execute("""
                INSERT INTO reclamations.pipeline_logs
                (job_name, execution_date, status, records_processed, error_message)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                'sla_compliance_checks_daily', now, statut,
                nb_breaches + nb_at_risk, None
            ))
        except Exception:
            pass  # Table optionnelle

        conn.commit()
        conn.close()

        print(f"""
╔══════════════════════════════════════════╗
║   ✅ RAPPORT SLA TERMINÉ                ║
╚══════════════════════════════════════════╝
   • Dépassements SLA  : {nb_breaches}
   • À risque (<24h)   : {nb_at_risk}
   • Statut            : {statut}
   • Durée             : {duration:.0f}s
✅ Log sauvegardé dans pipeline_runs
        """)

    except Exception as e:
        print(f"⚠️ Erreur sauvegarde rapport : {e}")
        raise

task_save_report = PythonOperator(
    task_id='save_sla_report',
    python_callable=save_sla_report,
    dag=dag,
)

# ==============================================================================
# DÉPENDANCES
# ==============================================================================
[task_identify_breaches, task_at_risk] >> task_compliance_rate >> \
task_alerts >> task_save_report

dag.doc_md = """
# Vérification Conformité SLA

## 🎯 Objectif
Surveiller quotidiennement le respect des SLA et générer des alertes proactives.

## 📋 SLA par Type
| Type | SLA |
|------|-----|
| Coupure électrique | 48h |
| Compteur Linky | 120h |
| Facturation | 240h |
| Raccordement réseau | 360h |
| Intervention technique | 168h |

## 🚨 Alertes
- **Critique** : > 50 dépassements SLA
- **Warning** : > 100 réclamations à risque

## ⏱️ Fréquence
Quotidien à 6h AM (après KPIs à 5h)
"""
