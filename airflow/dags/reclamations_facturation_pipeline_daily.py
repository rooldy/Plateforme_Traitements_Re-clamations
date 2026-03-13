"""
reclamations_facturation_pipeline_daily.py
Pipeline spécialisé FACTURATION — SLA 240h (10 jours).

Responsabilités :
  1. Filtre les réclamations FACTURATION depuis reclamations_processed
  2. Enrichit avec données factures (montant contesté, type d'erreur)
  3. Calcule les métriques financières et statut SLA
  4. Exporte vers reclamations.reclamations_facturation_detail
  5. Log dans pipeline_runs

Schedule : quotidien 03:45 (après reclamation_pipeline 02:00)
"""

import logging
import glob
import io
import csv as csv_mod
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SLA_HEURES, SPARK_JAR, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "reclamations_facturation_pipeline_daily"
TYPE_RECLAMATION = "FACTURATION"
SLA_HEURES_TYPE = SLA_HEURES[TYPE_RECLAMATION]  # 240h

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def create_target_table(**ctx):
    ddl = """
        CREATE SCHEMA IF NOT EXISTS reclamations;
        CREATE TABLE IF NOT EXISTS reclamations.reclamations_facturation_detail (
            reclamation_id          VARCHAR(50) PRIMARY KEY,
            client_id               VARCHAR(50),
            region                  VARCHAR(100),
            departement             VARCHAR(10),
            date_creation           TIMESTAMP WITH TIME ZONE,
            date_cloture            TIMESTAMP WITH TIME ZONE,
            statut                  VARCHAR(50),
            priorite                VARCHAR(20),
            montant_conteste        NUMERIC(12, 2),
            type_erreur_facturation VARCHAR(100),
            duree_heures            NUMERIC(10, 2),
            sla_heures              INTEGER DEFAULT 240,
            sla_respect             BOOLEAN,
            risque_depassement      BOOLEAN,
            heures_restantes        NUMERIC(10, 2),
            agent_id                VARCHAR(50),
            export_date             DATE,
            created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_fact_region   ON reclamations.reclamations_facturation_detail(region);
        CREATE INDEX IF NOT EXISTS idx_fact_statut   ON reclamations.reclamations_facturation_detail(statut);
        CREATE INDEX IF NOT EXISTS idx_fact_sla      ON reclamations.reclamations_facturation_detail(sla_respect);
        CREATE INDEX IF NOT EXISTS idx_fact_montant  ON reclamations.reclamations_facturation_detail(montant_conteste);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.reclamations_facturation_detail prête")
    finally:
        conn.close()

def extract_and_transform(**ctx):
    from pyspark.sql import functions as F
    from dag_utils import get_spark_session, drop_partition_cols
    import os

    run_date = (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d")
    spark = get_spark_session("FacturationPipeline")

    try:
        parquet_path = "/opt/airflow/data/processed/reclamations"
        factures_path = "/opt/airflow/data/processed/factures"

        if not os.path.exists(parquet_path):
            log.warning("Parquet réclamations absent — skip transformation Spark")
            ctx["ti"].xcom_push(key="row_count", value=0)
            return

        df = spark.read.parquet(parquet_path)
        df = df.filter(F.col("type_reclamation") == TYPE_RECLAMATION)

        if df.count() == 0:
            log.info("Aucune réclamation FACTURATION pour %s", run_date)
            ctx["ti"].xcom_push(key="row_count", value=0)
            return

        # Enrichissement données factures (optionnel)
        try:
            if os.path.exists(factures_path):
                df_factures = spark.read.parquet(factures_path)
                df = df.join(
                    df_factures.select("client_id", "montant_conteste", "type_erreur_facturation"),
                    on="client_id",
                    how="left",
                )
                log.info("Jointure factures réussie")
            else:
                df = df.withColumn("montant_conteste", F.lit(None).cast("double"))
                df = df.withColumn("type_erreur_facturation", F.lit(None).cast("string"))
        except Exception as exc:
            log.warning("Données factures non disponibles (%s) — colonnes nulles utilisées", exc)
            df = df.withColumn("montant_conteste", F.lit(None).cast("double"))
            df = df.withColumn("type_erreur_facturation", F.lit(None).cast("string"))

        # Calcul durée et SLA
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
        # Risque dépassement à <24h pour facturation (moins urgent que coupure)
        df = df.withColumn(
            "risque_depassement",
            (F.col("statut").isin("OUVERT", "EN_COURS")) &
            (F.col("heures_restantes") < 24) &
            (F.col("heures_restantes") >= 0)
        )
        df = df.withColumn("export_date", F.lit(run_date))

        df = drop_partition_cols(df)

        export_path = f"/opt/airflow/exports/facturation_{run_date}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(export_path)

        row_count = df.count()
        ctx["ti"].xcom_push(key="export_path", value=export_path)
        ctx["ti"].xcom_push(key="row_count", value=row_count)
        log.info("Export Spark facturation : %d lignes → %s", row_count, export_path)

    finally:
        spark.stop()

def load_to_postgres(**ctx):
    export_path = ctx["ti"].xcom_pull(key="export_path", task_ids="extract_and_transform")
    row_count = ctx["ti"].xcom_pull(key="row_count", task_ids="extract_and_transform") or 0

    if not export_path or row_count == 0:
        log.info("Aucune donnée à charger (row_count=%d)", row_count)
        ctx["ti"].xcom_push(key="rows_loaded", value=0)
        return

    csv_files = glob.glob(f"{export_path}/part-*.csv")
    if not csv_files:
        log.warning("Aucun CSV trouvé dans %s", export_path)
        return

    columns = [
        "reclamation_id", "client_id", "region", "departement",
        "date_creation", "date_cloture", "statut", "priorite",
        "montant_conteste", "type_erreur_facturation",
        "duree_heures", "sla_heures", "sla_respect",
        "risque_depassement", "heures_restantes", "agent_id", "export_date",
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM reclamations.reclamations_facturation_detail WHERE export_date = %s",
                ((ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"),)
            )
            with open(csv_files[0], "r", encoding="utf-8") as f:
                reader = csv_mod.DictReader(f)
                rows = [tuple(row.get(c, "") for c in columns) for row in reader]
            total = len(rows)
            buf = io.StringIO()
            csv_mod.writer(buf).writerows(rows)
            buf.seek(0)
            cur.copy_from(buf, "reclamations.reclamations_facturation_detail",
                          sep=",", columns=columns, null="")
        conn.commit()
        log.info("COPY → reclamations_facturation_detail : %d lignes", total)
        ctx["ti"].xcom_push(key="rows_loaded", value=total)
    finally:
        conn.close()

def compute_financial_metrics(**ctx):
    """Calcule et log les métriques financières agrégées (montants contestés)."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                          AS total,
                    SUM(montant_conteste)             AS montant_total,
                    AVG(montant_conteste)             AS montant_moyen,
                    COUNT(*) FILTER (WHERE NOT sla_respect AND date_cloture IS NOT NULL) AS hors_sla,
                    COUNT(*) FILTER (WHERE risque_depassement)                           AS en_risque
                FROM reclamations.reclamations_facturation_detail
                WHERE export_date = %s
            """, ((ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"),))
            row = cur.fetchone()
        total, montant_total, montant_moyen, hors_sla, en_risque = row
        log.info(
            "Métriques FACTURATION [%s] : total=%d | montant_total=%.2f€ | "
            "montant_moyen=%.2f€ | hors_sla=%d | en_risque=%d",
            (ctx.get("logical_date") or ctx.get("data_interval_start") or __import__("datetime").datetime.now()).strftime("%Y-%m-%d"), total or 0, montant_total or 0,
            montant_moyen or 0, hors_sla or 0, en_risque or 0,
        )
    finally:
        conn.close()

def notify_pipeline_run(**ctx):
    rows = ctx["ti"].xcom_pull(key="rows_loaded", task_ids="load_to_postgres") or 0
    log_pipeline_run(
        dag_id=DAG_ID,
        status="SUCCESS",
        rows_processed=rows,
        duration_seconds=0,
        message=f"Pipeline FACTURATION OK — {rows} réclamations traitées [{(ctx.get('logical_date') or ctx.get('data_interval_start') or __import__('datetime').datetime.now()).strftime('%Y-%m-%d')}]",
    )

with DAG(
    dag_id=DAG_ID,
    description="Pipeline spécialisé FACTURATION — SLA 240h (10 jours)",
    schedule="45 3 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reclamations", "métier", "facturation"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_target_table", python_callable=create_target_table)
    t_extract = PythonOperator(task_id="extract_and_transform", python_callable=extract_and_transform)
    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t_metrics = PythonOperator(task_id="compute_financial_metrics", python_callable=compute_financial_metrics)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_extract >> t_load >> t_metrics >> t_notify
