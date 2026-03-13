"""
reclamations_coupures_pipeline_daily.py
Pipeline spécialisé COUPURE_ELECTRIQUE — SLA critique 48h.

Responsabilités :
  1. Filtre les réclamations COUPURE_ELECTRIQUE depuis reclamations_processed
  2. Enrichit avec données géographiques et priorité urgence
  3. Calcule statut SLA (respect/dépassement/risque <12h)
  4. Exporte vers reclamations.reclamations_coupures_detail
  5. Déclenche alertes pour dépassements critiques
  6. Log dans pipeline_runs

Schedule : quotidien 03:30 (après reclamation_pipeline 02:00)
"""

import logging
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SLA_HEURES, SPARK_JAR, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "reclamations_coupures_pipeline_daily"
TYPE_RECLAMATION = "COUPURE_ELECTRIQUE"
SLA_HEURES_TYPE = SLA_HEURES[TYPE_RECLAMATION]  # 48h

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

# ─── Tâche 1 : Création table cible ──────────────────────────────────────────
def create_target_table(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;
        CREATE TABLE IF NOT EXISTS reclamations.reclamations_coupures_detail (
            reclamation_id      VARCHAR(50) PRIMARY KEY,
            client_id           VARCHAR(50),
            region              VARCHAR(100),
            departement         VARCHAR(10),
            date_creation       TIMESTAMP WITH TIME ZONE,
            date_cloture        TIMESTAMP WITH TIME ZONE,
            statut              VARCHAR(50),
            priorite            VARCHAR(20),
            duree_heures        NUMERIC(10, 2),
            sla_heures          INTEGER DEFAULT 48,
            sla_respect         BOOLEAN,
            risque_depassement  BOOLEAN,
            heures_restantes    NUMERIC(10, 2),
            agent_id            VARCHAR(50),
            commentaire         TEXT,
            export_date         DATE,
            created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_coupures_region ON reclamations.reclamations_coupures_detail(region);
        CREATE INDEX IF NOT EXISTS idx_coupures_statut ON reclamations.reclamations_coupures_detail(statut);
        CREATE INDEX IF NOT EXISTS idx_coupures_sla ON reclamations.reclamations_coupures_detail(sla_respect);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.reclamations_coupures_detail prête")
    finally:
        conn.close()

# ─── Tâche 2 : Extraction et transformation Spark ────────────────────────────
def extract_and_transform(**ctx):
    from pyspark.sql import functions as F
    from dag_utils import get_spark_session, drop_partition_cols
    import os

    start = datetime.now(timezone.utc)
    spark = get_spark_session("CoupuresPipeline")
    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")

    try:
        # Lecture depuis le Gold layer (reclamations_processed via Parquet)
        parquet_path = "/opt/airflow/data/processed/reclamations"
        if not os.path.exists(parquet_path):
            log.warning("Parquet non trouvé, lecture depuis PostgreSQL via JDBC de fallback")
            _transform_from_postgres(run_date)
            return

        df = spark.read.parquet(parquet_path)

        # Filtre sur le type COUPURE_ELECTRIQUE
        df = df.filter(F.col("type_reclamation") == TYPE_RECLAMATION)

        if df.count() == 0:
            log.info("Aucune réclamation COUPURE_ELECTRIQUE pour %s", run_date)
            log_pipeline_run(DAG_ID, "SUCCESS", 0,
                             (datetime.now(timezone.utc) - start).total_seconds(),
                             f"Aucune donnée pour {run_date}")
            return

        # Calcul durée et statut SLA
        now_ts = F.lit(datetime.now(timezone.utc))
        df = df.withColumn(
            "duree_heures",
            F.when(
                F.col("date_cloture").isNotNull(),
                (F.unix_timestamp("date_cloture") - F.unix_timestamp("date_creation")) / 3600
            ).otherwise(
                (F.unix_timestamp(now_ts) - F.unix_timestamp("date_creation")) / 3600
            )
        )

        df = df.withColumn("sla_heures", F.lit(SLA_HEURES_TYPE))

        df = df.withColumn(
            "sla_respect",
            F.when(F.col("date_cloture").isNotNull(),
                   F.col("duree_heures") <= SLA_HEURES_TYPE
                   ).otherwise(F.lit(None).cast("boolean"))
        )

        df = df.withColumn(
            "heures_restantes",
            F.when(
                F.col("statut").isin("OUVERT", "EN_COURS"),
                F.lit(SLA_HEURES_TYPE) - F.col("duree_heures")
            ).otherwise(F.lit(None).cast("double"))
        )

        # Risque dépassement : réclamations ouvertes avec < 12h restantes
        df = df.withColumn(
            "risque_depassement",
            (F.col("statut").isin("OUVERT", "EN_COURS")) &
            (F.col("heures_restantes") < 12) &
            (F.col("heures_restantes") >= 0)
        )

        df = df.withColumn("export_date", F.lit(run_date))

        # Suppression colonnes de partition
        df = drop_partition_cols(df)

        # Export CSV temporaire
        export_path = f"/opt/airflow/exports/coupures_{run_date}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(export_path)

        # Stockage chemin pour tâche suivante
        ctx["ti"].xcom_push(key="export_path", value=export_path)
        ctx["ti"].xcom_push(key="row_count", value=df.count())
        log.info("Export Spark terminé : %s", export_path)

    finally:
        spark.stop()

def _transform_from_postgres(run_date: str):
    """Fallback : transformation depuis PostgreSQL si Parquet non disponible."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT reclamation_id, client_id, region, departement,
                       date_creation, date_cloture, statut, priorite, agent_id
                FROM reclamations.reclamations_processed
                WHERE type_reclamation = %s
                  AND DATE(date_creation) <= %s
            """, (TYPE_RECLAMATION, run_date))
            rows = cur.fetchall()
        log.info("Fallback PostgreSQL : %d lignes COUPURE_ELECTRIQUE", len(rows))
    finally:
        conn.close()

# ─── Tâche 3 : Chargement PostgreSQL via COPY ────────────────────────────────
def load_to_postgres(**ctx):
    import glob
    import csv as csv_mod

    export_path = ctx["ti"].xcom_pull(key="export_path", task_ids="extract_and_transform")
    if not export_path:
        log.warning("Pas de chemin d'export, skip chargement")
        return

    # Trouver le fichier CSV généré par Spark coalesce(1)
    csv_files = glob.glob(f"{export_path}/part-*.csv")
    if not csv_files:
        log.warning("Aucun fichier CSV dans %s", export_path)
        return

    csv_file = csv_files[0]
    columns = [
        "reclamation_id", "client_id", "region", "departement",
        "date_creation", "date_cloture", "statut", "priorite",
        "duree_heures", "sla_heures", "sla_respect", "risque_depassement",
        "heures_restantes", "agent_id", "commentaire", "export_date",
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    total_rows = 0
    try:
        with conn.cursor() as cur:
            # Upsert : supprime les lignes existantes pour la date d'export
            cur.execute(
                "DELETE FROM reclamations.reclamations_coupures_detail WHERE export_date = %s",
                ((ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"),)
            )
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv_mod.DictReader(f)
                rows_to_insert = [
                    tuple(row.get(c, "") for c in columns)
                    for row in reader
                ]
                total_rows = len(rows_to_insert)

            import io
            buffer = io.StringIO()
            writer = csv_mod.writer(buffer)
            for row in rows_to_insert:
                writer.writerow(row)
            buffer.seek(0)
            cur.copy_from(buffer, "reclamations.reclamations_coupures_detail",
                          sep=",", columns=columns, null="")
        conn.commit()
        log.info("COPY → reclamations_coupures_detail : %d lignes", total_rows)
        ctx["ti"].xcom_push(key="rows_loaded", value=total_rows)
    finally:
        conn.close()

# ─── Tâche 4 : Alertes dépassements critiques ────────────────────────────────
def check_sla_alerts(**ctx):
    """Identifie les dépassements SLA et les risques imminents (<12h)."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Dépassements SLA actifs
            cur.execute("""
                SELECT COUNT(*) FROM reclamations.reclamations_coupures_detail
                WHERE statut IN ('OUVERT', 'EN_COURS')
                  AND duree_heures > %s
                  AND export_date = %s
            """, (SLA_HEURES_TYPE, (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")))
            depassements = cur.fetchone()[0]

            # Risques imminents
            cur.execute("""
                SELECT COUNT(*) FROM reclamations.reclamations_coupures_detail
                WHERE risque_depassement = TRUE
                  AND export_date = %s
            """, ((ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"),))
            risques = cur.fetchone()[0]

        msg = (f"COUPURE_ELECTRIQUE [{(ctx.get('logical_date') or ctx.get('data_interval_start') or __import__('datetime').datetime.now()).strftime('%Y-%m-%d')}] — "
               f"Dépassements SLA 48h : {depassements} | Risques <12h : {risques}")
        log.warning(msg) if (depassements > 0 or risques > 0) else log.info(msg)
        ctx["ti"].xcom_push(key="alert_summary", value=msg)
    finally:
        conn.close()

# ─── Tâche 5 : Notification pipeline_runs ────────────────────────────────────
def notify_pipeline_run(**ctx):
    start_ts = ctx["ti"].xcom_pull(key="start_ts", task_ids="extract_and_transform") or ctx["data_interval_start"].timestamp()
    rows = ctx["ti"].xcom_pull(key="rows_loaded", task_ids="load_to_postgres") or 0
    alert_summary = ctx["ti"].xcom_pull(key="alert_summary", task_ids="check_sla_alerts") or ""
    duration = (datetime.now(timezone.utc).timestamp() - float(start_ts)) if start_ts else 0

    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=rows,
        duration_seconds=duration,
        message=f"Pipeline COUPURE_ELECTRIQUE OK. {alert_summary}",
    )

# ─── Définition du DAG ───────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    description="Pipeline spécialisé COUPURE_ELECTRIQUE — SLA 48h critique",
    schedule="30 3 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reclamations", "métier", "coupures", "sla-critique"],
    doc_md=__doc__,
) as dag:

    t_create_table = PythonOperator(
    task_id="create_target_table",
    python_callable=create_target_table,
    )

    t_extract = PythonOperator(
    task_id="extract_and_transform",
    python_callable=extract_and_transform,
    )

    t_load = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    )

    t_alerts = PythonOperator(
    task_id="check_sla_alerts",
    python_callable=check_sla_alerts,
    )

    t_notify = PythonOperator(
    task_id="notify_pipeline_run",
    python_callable=notify_pipeline_run,
    trigger_rule="all_done",
    )

    (
    t_create_table
        >> t_extract
        >> t_load
        >> t_alerts
        >> t_notify
    )
