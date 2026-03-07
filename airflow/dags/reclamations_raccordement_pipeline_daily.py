"""
reclamations_raccordement_pipeline_daily.py
Pipeline spécialisé RACCORDEMENT_RESEAU — SLA 360h (15 jours).

Responsabilités :
  1. Filtre les réclamations RACCORDEMENT_RESEAU depuis reclamations_processed
  2. Enrichit avec données d'intervention terrain (techniciens assignés, zones)
  3. Calcule le statut SLA et les indicateurs de planification
  4. Génère un rapport des interventions planifiées vs réalisées
  5. Exporte vers reclamations.reclamations_raccordement_detail
  6. Log dans pipeline_runs

Schedule : quotidien 04:00 (après reclamation_pipeline 02:00)
"""

import logging
import glob
import io
import csv as csv_mod
from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import DB_CONFIG, SLA_HEURES, log_pipeline_run

log = logging.getLogger(__name__)

DAG_ID = "reclamations_raccordement_pipeline_daily"
TYPE_RECLAMATION = "RACCORDEMENT_RESEAU"
SLA_HEURES_TYPE = SLA_HEURES[TYPE_RECLAMATION]  # 360h = 15 jours

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
        CREATE TABLE IF NOT EXISTS reclamations.reclamations_raccordement_detail (
            reclamation_id          VARCHAR(50) PRIMARY KEY,
            client_id               VARCHAR(50),
            region                  VARCHAR(100),
            departement             VARCHAR(10),
            date_creation           TIMESTAMP WITH TIME ZONE,
            date_cloture            TIMESTAMP WITH TIME ZONE,
            date_intervention_prevue TIMESTAMP WITH TIME ZONE,
            statut                  VARCHAR(50),
            priorite                VARCHAR(20),
            type_raccordement       VARCHAR(100),
            technicien_id           VARCHAR(50),
            zone_intervention       VARCHAR(100),
            duree_heures            NUMERIC(10, 2),
            sla_heures              INTEGER DEFAULT 360,
            sla_respect             BOOLEAN,
            risque_depassement      BOOLEAN,
            heures_restantes        NUMERIC(10, 2),
            intervention_planifiee  BOOLEAN,
            export_date             DATE,
            created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_racc_region    ON reclamations.reclamations_raccordement_detail(region);
        CREATE INDEX IF NOT EXISTS idx_racc_technicien ON reclamations.reclamations_raccordement_detail(technicien_id);
        CREATE INDEX IF NOT EXISTS idx_racc_sla       ON reclamations.reclamations_raccordement_detail(sla_respect);
        CREATE INDEX IF NOT EXISTS idx_racc_date      ON reclamations.reclamations_raccordement_detail(date_intervention_prevue);
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        log.info("Table reclamations.reclamations_raccordement_detail prête")
    finally:
        conn.close()

def extract_and_transform(**ctx):
    from pyspark.sql import functions as F
    from dag_utils import get_spark_session, drop_partition_cols
    import os

    run_date = ctx["ds"]
    spark = get_spark_session("RaccordementPipeline")

    try:
        parquet_path = "/opt/airflow/data/processed/reclamations"
        interventions_path = "/opt/airflow/data/processed/interventions"

        if not os.path.exists(parquet_path):
            log.warning("Parquet absent — skip")
            ctx["ti"].xcom_push(key="row_count", value=0)
            return

        df = spark.read.parquet(parquet_path)
        df = df.filter(F.col("type_reclamation") == TYPE_RECLAMATION)

        row_count = df.count()
        if row_count == 0:
            log.info("Aucune réclamation RACCORDEMENT_RESEAU pour %s", run_date)
            ctx["ti"].xcom_push(key="row_count", value=0)
            return

        # Enrichissement données interventions terrain (optionnel)
        try:
            if os.path.exists(interventions_path):
                df_interv = spark.read.parquet(interventions_path)
                df = df.join(
                    df_interv.select(
                        "reclamation_id", "technicien_id", "zone_intervention",
                        "date_intervention_prevue", "type_raccordement",
                    ),
                    on="reclamation_id",
                    how="left",
                )
            else:
                for col_name in ["technicien_id", "zone_intervention",
                                 "date_intervention_prevue", "type_raccordement"]:
                    df = df.withColumn(col_name, F.lit(None).cast("string"))
        except Exception as exc:
            log.warning("Données interventions non disponibles (%s)", exc)
            for col_name in ["technicien_id", "zone_intervention",
                             "date_intervention_prevue", "type_raccordement"]:
                df = df.withColumn(col_name, F.lit(None).cast("string"))

        # Indicateur intervention planifiée
        df = df.withColumn(
            "intervention_planifiee",
            F.col("technicien_id").isNotNull() & F.col("date_intervention_prevue").isNotNull()
        )

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
        # Risque à <48h pour raccordement
        df = df.withColumn(
            "risque_depassement",
            (F.col("statut").isin("OUVERT", "EN_COURS")) &
            (F.col("heures_restantes") < 48) &
            (F.col("heures_restantes") >= 0)
        )
        df = df.withColumn("export_date", F.lit(run_date))

        df = drop_partition_cols(df)

        export_path = f"/opt/airflow/exports/raccordement_{run_date}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(export_path)

        final_count = df.count()
        ctx["ti"].xcom_push(key="export_path", value=export_path)
        ctx["ti"].xcom_push(key="row_count", value=final_count)
        log.info("Export raccordement : %d lignes → %s", final_count, export_path)

    finally:
        spark.stop()

def load_to_postgres(**ctx):
    export_path = ctx["ti"].xcom_pull(key="export_path", task_ids="extract_and_transform")
    row_count = ctx["ti"].xcom_pull(key="row_count", task_ids="extract_and_transform") or 0

    if not export_path or row_count == 0:
        ctx["ti"].xcom_push(key="rows_loaded", value=0)
        return

    csv_files = glob.glob(f"{export_path}/part-*.csv")
    if not csv_files:
        return

    columns = [
        "reclamation_id", "client_id", "region", "departement",
        "date_creation", "date_cloture", "date_intervention_prevue",
        "statut", "priorite", "type_raccordement",
        "technicien_id", "zone_intervention",
        "duree_heures", "sla_heures", "sla_respect",
        "risque_depassement", "heures_restantes",
        "intervention_planifiee", "export_date",
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM reclamations.reclamations_raccordement_detail WHERE export_date = %s",
                (ctx["ds"],)
            )
            with open(csv_files[0], "r", encoding="utf-8") as f:
                reader = csv_mod.DictReader(f)
                rows = [tuple(row.get(c, "") for c in columns) for row in reader]
            total = len(rows)
            buf = io.StringIO()
            csv_mod.writer(buf).writerows(rows)
            buf.seek(0)
            cur.copy_from(buf, "reclamations.reclamations_raccordement_detail",
                          sep=",", columns=columns, null="")
        conn.commit()
        log.info("COPY → reclamations_raccordement_detail : %d lignes", total)
        ctx["ti"].xcom_push(key="rows_loaded", value=total)
    finally:
        conn.close()

def generate_planning_report(**ctx):
    """Génère un rapport synthétique des interventions planifiées vs non planifiées."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    region,
                    COUNT(*)                                              AS total,
                    COUNT(*) FILTER (WHERE intervention_planifiee)       AS avec_planification,
                    COUNT(*) FILTER (WHERE NOT intervention_planifiee)   AS sans_planification,
                    COUNT(*) FILTER (WHERE risque_depassement)           AS en_risque_sla,
                    ROUND(AVG(duree_heures)::numeric, 1)                 AS duree_moyenne_h
                FROM reclamations.reclamations_raccordement_detail
                WHERE export_date = %s
                GROUP BY region
                ORDER BY en_risque_sla DESC, total DESC
            """, (ctx["ds"],))
            rows = cur.fetchall()

        log.info("=== Rapport Raccordement [%s] ===", ctx["ds"])
        for region, total, avec, sans, risque, duree in rows:
            log.info(
                "  %s : total=%d | planifiées=%d | non_planifiées=%d | "
                "en_risque=%d | durée_moy=%.1fh",
                region, total, avec, sans, risque, duree or 0
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
        message=f"Pipeline RACCORDEMENT_RESEAU OK — {rows} réclamations [{ctx['ds']}]",
    )

with DAG(
    dag_id=DAG_ID,
    description="Pipeline spécialisé RACCORDEMENT_RESEAU — SLA 360h (15 jours)",
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reclamations", "métier", "raccordement"],
    doc_md=__doc__,
) as dag:

    t_create = PythonOperator(task_id="create_target_table", python_callable=create_target_table)
    t_extract = PythonOperator(task_id="extract_and_transform", python_callable=extract_and_transform)
    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t_report = PythonOperator(task_id="generate_planning_report", python_callable=generate_planning_report)
    t_notify = PythonOperator(task_id="notify_pipeline_run", python_callable=notify_pipeline_run, trigger_rule="all_done")

    t_create >> t_extract >> t_load >> t_report >> t_notify
