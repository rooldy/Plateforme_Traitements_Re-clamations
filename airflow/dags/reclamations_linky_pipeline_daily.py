"""
DAG Airflow : Pipeline de traitement des réclamations Linky.
Traite spécifiquement les réclamations liées aux compteurs Linky.

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
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'reclamations_linky_pipeline_daily',
    default_args=default_args,
    description='Pipeline spécialisé pour les réclamations Linky',
    schedule='0 3 * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['reclamations', 'linky', 'metier', 'production'],
)

# ==============================================================================
# TÂCHE 1 : Attendre la consolidation globale
# ==============================================================================
# ==============================================================================
# TÂCHE 2 : Filtrer les réclamations Linky
# ==============================================================================
task_filter_linky = BashOperator(
    task_id='filter_reclamations_linky',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("Filter_Linky") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations")

# Supprimer colonnes de partition Spark si présentes
for c in ["annee", "mois"]:
    if c in df.columns:
        df = df.drop(c)

df_linky = df.filter(col("type_reclamation") == "COMPTEUR_LINKY")
df_linky.write.mode("overwrite").parquet(f"{base_path}/data/processed/reclamations_linky")

print(f"✅ {df_linky.count()} réclamations Linky filtrées")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 3 : Enrichir avec données compteurs (graceful si référentiel manquant)
# ==============================================================================
task_enrich_compteurs = BashOperator(
    task_id='enrich_with_compteur_data',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
import os

spark = SparkSession.builder \
    .appName("Enrich_Linky_Compteurs") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df_reclamations = spark.read.parquet(f"{base_path}/data/processed/reclamations_linky")

try:
    df_compteurs = spark.read.parquet(f"{base_path}/data/raw_parquet/compteurs_linky")
    df_enriched = df_reclamations.alias("r").join(
        df_compteurs.alias("c"),
        col("r.reference_linky") == col("c.reference_linky"),
        "left"
    ).select(
        col("r.*"),
        col("c.type_compteur"),
        col("c.puissance_souscrite_kva"),
        col("c.date_installation"),
        col("c.est_defectueux"),
        col("c.nombre_incidents"),
        datediff(col("r.date_creation"), col("c.date_installation")).alias("anciennete_compteur_jours")
    )
    print(f"✅ {df_enriched.count()} réclamations enrichies avec données compteurs")
except Exception as e:
    print(f"⚠️ Référentiel compteurs indisponible ({e}) — traitement sans enrichissement")
    df_enriched = df_reclamations

df_enriched.write.mode("overwrite").parquet(f"{base_path}/data/processed/reclamations_linky_enriched")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 4 : Corréler avec historique relevés (graceful si données manquantes)
# ==============================================================================
task_correlate_releves = BashOperator(
    task_id='correlate_with_releves',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, datediff
import os

spark = SparkSession.builder \
    .appName("Correlate_Releves") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df_reclamations = spark.read.parquet(f"{base_path}/data/processed/reclamations_linky_enriched")

try:
    df_releves = spark.read.parquet(f"{base_path}/data/raw_parquet/historique_releves")
    df_correlated = df_reclamations.alias("r").join(
        df_releves.filter(col("anomalie_detectee") == True).alias("rel"),
        (col("r.reference_linky") == col("rel.reference_linky")) &
        (col("rel.date_releve") <= col("r.date_creation")) &
        (datediff(col("r.date_creation"), col("rel.date_releve")) <= 180),
        "left"
    ).groupBy(
        "r.id_reclamation", "r.client_id", "r.type_reclamation", "r.priorite",
        "r.statut", "r.reference_linky", "r.date_creation"
    ).agg(
        count("rel.id_releve").alias("nb_anomalies_releves_6mois"),
        avg("rel.consommation_kwh").alias("conso_moyenne_6mois")
    )
    print(f"✅ Corrélation avec relevés terminée : {df_correlated.count()} lignes")
except Exception as e:
    print(f"⚠️ Historique relevés indisponible ({e}) — traitement sans corrélation")
    df_correlated = df_reclamations

df_correlated.write.mode("overwrite").parquet(f"{base_path}/data/processed/reclamations_linky_final")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 5 : Validation format références Linky
# ==============================================================================
task_validate_references = BashOperator(
    task_id='validate_linky_references',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("Validate_Linky_Refs") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations_linky_final")

df_validated = df.withColumn(
    "reference_valide",
    col("reference_linky").rlike("^LKY[0-9]{8}$")
)

total    = df_validated.count()
invalids = df_validated.filter(col("reference_valide") == False).count()
print(f"📊 Validation : {total - invalids}/{total} références valides")
if invalids > 0:
    print(f"⚠️  {invalids} références Linky invalides détectées")

df_validated.write.mode("overwrite").parquet(f"{base_path}/data/processed/reclamations_linky_validated")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 6 : Calcul KPIs Linky
# ==============================================================================
task_calculate_kpis = BashOperator(
    task_id='calculate_linky_kpis',
    bash_command="""
        python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when
import os

spark = SparkSession.builder \
    .appName("KPIs_Linky") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
df = spark.read.parquet(f"{base_path}/data/processed/reclamations_linky_validated")

agg_exprs = [
    count("*").alias("nb_reclamations_linky"),
    count(when(col("reference_valide") == False, 1)).alias("nb_references_invalides")
]

# Colonnes optionnelles issues de l'enrichissement compteurs
if "est_defectueux" in df.columns:
    agg_exprs.append(count(when(col("est_defectueux") == True, 1)).alias("nb_compteurs_defectueux"))
    agg_exprs.append(
        (count(when(col("est_defectueux") == True, 1)) * 100.0 / count("*")).alias("taux_compteurs_defectueux")
    )
if "nb_anomalies_releves_6mois" in df.columns:
    agg_exprs.append(avg("nb_anomalies_releves_6mois").alias("moyenne_anomalies_par_compteur"))

kpis = df.groupBy("region").agg(*agg_exprs)
kpis.show(truncate=False)
kpis.write.mode("overwrite").parquet(f"{base_path}/data/kpis/linky_daily")
print(f"✅ KPIs Linky calculés : {kpis.count()} lignes")
spark.stop()
EOF
    """,
    dag=dag,
)

# ==============================================================================
# TÂCHE 7 : Export PostgreSQL (psycopg2, pattern medallion)
# ==============================================================================
def export_linky_to_postgres(**context):
    """Export réclamations Linky vers PostgreSQL via psycopg2 — pattern medallion."""
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

        # Créer la table si elle n'existe pas
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reclamations.reclamations_linky_detailed
            AS SELECT * FROM reclamations.reclamations_cleaned WHERE 1=0
        """)
        conn.commit()

        df = pd.read_parquet(f"{base_path}/data/processed/reclamations_linky_validated")

        # Supprimer colonnes de partition et colonnes non présentes dans la table cible
        cols_to_drop = [c for c in ["annee", "mois"] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # Garder seulement colonnes communes avec reclamations_cleaned
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'reclamations'
            AND table_name = 'reclamations_linky_detailed'
        """)
        table_cols = [row[0] for row in cur.fetchall()]
        cols = [c for c in df.columns if c in table_cols] if table_cols else df.columns.tolist()

        cur.execute("TRUNCATE TABLE reclamations.reclamations_linky_detailed")
        execute_values(
            cur,
            f"INSERT INTO reclamations.reclamations_linky_detailed ({', '.join(cols)}) VALUES %s",
            [tuple(row) for row in df[cols].itertuples(index=False, name=None)],
            page_size=500
        )
        conn.commit()
        print(f"✅ {len(df):,} réclamations Linky exportées vers PostgreSQL")

    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur export Linky : {e}")
        raise
    finally:
        conn.close()

task_export_postgres = PythonOperator(
    task_id='export_to_postgres',
    python_callable=export_linky_to_postgres,
    dag=dag,
)

# ==============================================================================
# TÂCHE 8 : Notification (pattern medallion)
# ==============================================================================
def send_linky_report(**context):
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

        try:
            cur.execute("SELECT COUNT(*) FROM reclamations.reclamations_linky_detailed")
            nb_linky = cur.fetchone()[0]
        except Exception:
            nb_linky = 0

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
            f"✅ Pipeline Linky terminé : {nb_linky:,} réclamations traitées"
        ))
        conn.commit()
        conn.close()

        print(f"""
╔══════════════════════════════════════════╗
║   ✅ PIPELINE LINKY TERMINÉ             ║
╚══════════════════════════════════════════╝
   • Réclamations Linky : {nb_linky:,}
   • Durée              : {duration:.0f}s
✅ Log sauvegardé dans pipeline_runs
        """)

    except Exception as e:
        print(f"⚠️ Erreur notification : {e}")
        raise

task_notification = PythonOperator(
    task_id='send_report',
    python_callable=send_linky_report,
    dag=dag,
)

# ==============================================================================
# DÉPENDANCES
# ==============================================================================
task_filter_linky >> task_enrich_compteurs >> \
task_correlate_releves >> task_validate_references >> task_calculate_kpis >> \
task_export_postgres >> task_notification

dag.doc_md = """
# Pipeline Réclamations Linky

## 📋 Objectif
Pipeline spécialisé pour le traitement des réclamations liées aux compteurs Linky.
Enrichit les réclamations avec les données du référentiel compteurs et l'historique
des relevés pour identifier les compteurs défectueux.

## 🔄 Dépendances
Dépend de `reclamation_pipeline` (scheduling : 2h global, 3h Linky).

## 📊 Données traitées
- Entrée : réclamations COMPTEUR_LINKY depuis data/processed/
- Référentiels : compteurs_linky, historique_releves (graceful si manquants)
- Sortie : reclamations_linky_detailed + KPIs Linky

## ⏱️ SLA Linky : 5 jours ouvrés (120h)
"""
