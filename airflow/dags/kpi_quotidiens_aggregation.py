"""
DAG Airflow : Agrégation des KPIs quotidiens.
Calcule les indicateurs métier quotidiens pour reporting.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'data-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email': ['analytics@example.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kpi_quotidiens_aggregation',
    default_args=default_args,
    description='Calcul des KPIs quotidiens pour dashboards',
    schedule='0 5 * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['kpi', 'reporting', 'analytics', 'production'],
)

# ==============================================================================
# TÂCHE 1 : Attendre l'export global
# ==============================================================================
# ==============================================================================
# TÂCHE 2 : KPIs volumétrie par région/type
# ==============================================================================
task_kpi_volumetrie = BashOperator(
    task_id='calculate_volumetrie_kpis',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, current_date
import os

spark = SparkSession.builder \
    .appName("KPI_Volumetrie") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

kpis_volumetrie = df.groupBy("region", "type_reclamation").agg(
    count("*").alias("nombre_reclamations"),
    count(when(col("statut").isin(["OUVERT", "EN_COURS"]), 1)).alias("nombre_ouvertes"),
    count(when(col("statut") == "CLOTURE", 1)).alias("nombre_cloturees")
).withColumn("date_calcul", current_date())

kpis_volumetrie.show(20, truncate=False)
kpis_volumetrie.write.mode("overwrite").parquet(f"{base_path}/data/kpis/volumetrie_daily")
print(f"✅ KPIs volumétrie calculés : {kpis_volumetrie.count()} lignes")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 3 : KPIs délais de traitement
# ==============================================================================
task_kpi_delais = BashOperator(
    task_id='calculate_delais_kpis',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, percentile_approx, current_date
import os

spark = SparkSession.builder \
    .appName("KPI_Delais") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

kpis_delais = df.filter(col("duree_traitement_heures").isNotNull()) \
    .groupBy("region", "type_reclamation").agg(
        avg("duree_traitement_heures").alias("duree_moyenne_traitement_heures"),
        percentile_approx("duree_traitement_heures", 0.5).alias("duree_mediane_traitement_heures"),
        avg("delai_premiere_reponse_heures").alias("delai_moyen_premiere_reponse_heures")
    ).withColumn("date_calcul", current_date())

kpis_delais.show(20, truncate=False)
kpis_delais.write.mode("overwrite").parquet(f"{base_path}/data/kpis/delais_daily")
print(f"✅ KPIs délais calculés : {kpis_delais.count()} lignes")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 4 : KPIs SLA et priorité
# ==============================================================================
task_kpi_sla = BashOperator(
    task_id='calculate_sla_kpis',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, current_date
import os

spark = SparkSession.builder \
    .appName("KPI_SLA") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

kpis_sla = df.filter(col("duree_traitement_heures").isNotNull()) \
    .groupBy("region", "type_reclamation").agg(
        count("*").alias("total_reclamations_traitees"),
        count(when(
            ((col("type_reclamation") == "COUPURE_ELECTRIQUE") & (col("duree_traitement_heures") <= 48)) |
            ((col("type_reclamation") == "COMPTEUR_LINKY")     & (col("duree_traitement_heures") <= 120)) |
            ((col("type_reclamation") == "FACTURATION")        & (col("duree_traitement_heures") <= 240)) |
            ((col("type_reclamation") == "RACCORDEMENT_RESEAU")& (col("duree_traitement_heures") <= 360)) |
            ((col("type_reclamation") == "INTERVENTION_TECHNIQUE") & (col("duree_traitement_heures") <= 168)),
            1
        )).alias("nb_respect_sla"),
        count(when(col("priorite") == "CRITIQUE", 1)).alias("nb_reclamations_critiques"),
        count(when(col("statut") == "ESCALADE", 1)).alias("nb_escalades")
    ) \
    .withColumn("taux_respect_sla",
        (col("nb_respect_sla") / col("total_reclamations_traitees") * 100)) \
    .withColumn("taux_reclamations_critiques",
        (col("nb_reclamations_critiques") / col("total_reclamations_traitees") * 100)) \
    .withColumn("taux_escalade",
        (col("nb_escalades") / col("total_reclamations_traitees") * 100)) \
    .withColumn("date_calcul", current_date())

kpis_sla.show(20, truncate=False)
kpis_sla.write.mode("overwrite").parquet(f"{base_path}/data/kpis/sla_daily")
print(f"✅ KPIs SLA calculés : {kpis_sla.count()} lignes")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 5 : Consolidation tous KPIs
# ==============================================================================
task_consolidate_kpis = BashOperator(
    task_id='consolidate_all_kpis',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("Consolidate_KPIs") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

df_volumetrie = spark.read.parquet(f"{base_path}/data/kpis/volumetrie_daily")
df_delais     = spark.read.parquet(f"{base_path}/data/kpis/delais_daily")
df_sla        = spark.read.parquet(f"{base_path}/data/kpis/sla_daily")

df_kpis_final = df_volumetrie.alias("v") \
    .join(df_delais.alias("d"), ["region", "type_reclamation", "date_calcul"], "outer") \
    .join(df_sla.alias("s"),    ["region", "type_reclamation", "date_calcul"], "outer") \
    .select(
        "date_calcul", "region", "type_reclamation",
        col("v.nombre_reclamations").alias("nombre_reclamations_ouvertes"),
        col("v.nombre_cloturees").alias("nombre_reclamations_cloturees"),
        col("v.nombre_ouvertes").alias("nombre_reclamations_en_cours"),
        col("d.duree_moyenne_traitement_heures"),
        col("d.duree_mediane_traitement_heures"),
        col("d.delai_moyen_premiere_reponse_heures"),
        col("s.taux_respect_sla"),
        col("s.taux_reclamations_critiques"),
        col("s.taux_escalade")
    )

df_kpis_final.show(20, truncate=False)
df_kpis_final.write.mode("overwrite").parquet(f"{base_path}/data/kpis/consolidated_daily")
print(f"✅ {df_kpis_final.count()} KPIs consolidés")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 6 : Export PostgreSQL (psycopg2, pattern medallion)
# ==============================================================================
def export_kpis_to_postgres(**context):
    """Export KPIs consolidés vers PostgreSQL via psycopg2 — pattern medallion."""
    import os
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values
    from datetime import datetime, timezone

    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")

    conn = psycopg2.connect(
        host="reclamations-postgres", port=5432,
        database="reclamations_db",
        user="airflow", password="airflow_local_dev"
    )
    conn.autocommit = False

    try:
        cur = conn.cursor()

        df_kpis = pd.read_parquet(f"{base_path}/data/kpis/consolidated_daily")
        df_kpis["date_insertion"] = datetime.now(timezone.utc)

        cols = [c for c in [
            "date_calcul", "region", "type_reclamation",
            "nombre_reclamations_ouvertes", "nombre_reclamations_cloturees",
            "nombre_reclamations_en_cours", "duree_moyenne_traitement_heures",
            "duree_mediane_traitement_heures", "delai_moyen_premiere_reponse_heures",
            "taux_respect_sla", "taux_reclamations_critiques", "taux_escalade",
            "date_insertion"
        ] if c in df_kpis.columns]

        execute_values(
            cur,
            f"""INSERT INTO reclamations.kpis_daily ({', '.join(cols)}) VALUES %s
                ON CONFLICT (date_calcul, region, type_reclamation) DO UPDATE SET
                nombre_reclamations_ouvertes  = EXCLUDED.nombre_reclamations_ouvertes,
                nombre_reclamations_cloturees = EXCLUDED.nombre_reclamations_cloturees,
                nombre_reclamations_en_cours  = EXCLUDED.nombre_reclamations_en_cours,
                taux_respect_sla              = EXCLUDED.taux_respect_sla,
                date_insertion                = EXCLUDED.date_insertion""",
            [tuple(row) for row in df_kpis[cols].itertuples(index=False, name=None)],
            page_size=500
        )
        conn.commit()
        print(f"✅ {len(df_kpis):,} KPIs exportés vers PostgreSQL")

    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur export KPIs : {e}")
        raise
    finally:
        conn.close()

task_export_kpis = PythonOperator(
    task_id='export_kpis_to_postgres',
    python_callable=export_kpis_to_postgres,
    dag=dag,
)

# ==============================================================================
# TÂCHE 7 : Notification (pattern medallion)
# ==============================================================================
def send_kpi_report(**context):
    """Log succès dans pipeline_runs — même pattern que medallion_pipeline_daily."""
    from datetime import datetime, timezone
    import psycopg2

    dag_run = context.get('dag_run')

    try:
        conn = psycopg2.connect(
            host="reclamations-postgres", port=5432,
            database="reclamations_db",
            user="airflow", password="airflow_local_dev"
        )
        cur = conn.cursor()

        # Stats réelles depuis PostgreSQL
        cur.execute("SELECT COUNT(*) FROM reclamations.kpis_daily")
        nb_kpis = cur.fetchone()[0]

        now      = datetime.now(timezone.utc)
        start    = dag_run.start_date if dag_run else now
        duration = (now - start).total_seconds()

        cur.execute("""
            INSERT INTO reclamations.pipeline_runs
            (dag_id, run_id, status, start_date, end_date, duration_seconds, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            dag_run.dag_id, dag_run.run_id, 'SUCCESS',
            start, now, duration,
            f"✅ KPIs quotidiens calculés : {nb_kpis:,} entrées dans kpis_daily"
        ))
        conn.commit()
        conn.close()

        print(f"""
╔══════════════════════════════════════════╗
║   ✅ KPIs QUOTIDIENS TERMINÉS           ║
╚══════════════════════════════════════════╝
   • KPIs en base : {nb_kpis:,}
   • Durée        : {duration:.0f}s
✅ Log sauvegardé dans pipeline_runs
        """)

    except Exception as e:
        print(f"⚠️ Erreur notification : {e}")
        raise

task_notification = PythonOperator(
    task_id='send_kpi_report',
    python_callable=send_kpi_report,
    dag=dag,
)

# ==============================================================================
# DÉPENDANCES
# ==============================================================================
[task_kpi_volumetrie, task_kpi_delais, task_kpi_sla] >> \
task_consolidate_kpis >> task_export_kpis >> task_notification

dag.doc_md = """
# Agrégation KPIs Quotidiens

## 🎯 Objectif
Calculer quotidiennement tous les indicateurs métier pour alimenter les dashboards
de pilotage et le reporting management.

## 📊 KPIs calculés
- **Volumétrie** : ouvertes/en cours/clôturées par région et type
- **Délais** : durée moyenne, médiane, délai première réponse
- **SLA** : taux conformité, réclamations critiques, escalades

## 🔄 Dépendances
Attend que `reclamation_pipeline` ait exporté les données transformées.

## ⏱️ Fréquence
Quotidien à 5h AM (après transformations à 2h)
"""
