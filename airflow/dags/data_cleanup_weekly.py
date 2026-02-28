"""
DAG Airflow : Nettoyage Automatique des Données Anciennes
Stratégie de rétention différenciée selon la nature des données.

Politique de rétention (pattern entreprise) :
┌─────────────────────────┬──────────┬─────────────────────────────────────────┐
│ Table                   │ Rétention│ Raison                                  │
├─────────────────────────┼──────────┼─────────────────────────────────────────┤
│ reclamations_cleaned    │  90 jours│ Données personnelles clients → RGPD     │
│ kpis_daily              │   3 ans  │ Données agrégées/anonymes → valeur métier│
│ pipeline_runs           │   1 an   │ Logs techniques → audit & debug         │
└─────────────────────────┴──────────┴─────────────────────────────────────────┘

Auteur: Data Engineering Portfolio
Date: Février 2026
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import psycopg2

default_args = {
    'owner': 'data-ops',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_cleanup_weekly',
    default_args=default_args,
    description='Nettoyage hebdomadaire — rétention différenciée (RGPD)',
    schedule='0 3 * * 0',  # Dimanche 3h du matin
    catchup=False,
    tags=['maintenance', 'cleanup', 'rgpd'],
)

# ==============================================================================
# POLITIQUE DE RÉTENTION
# ==============================================================================

RETENTION_POLICY = {
    # Données personnelles → obligation RGPD : 90 jours max
    'reclamations.reclamations_cleaned': {
        'days': 90,
        'date_column': 'date_creation',
        'reason': 'Données personnelles clients (RGPD)',
    },
    # Données agrégées anonymes → valeur métier long terme : 3 ans
    # ⚠️ kpis_daily intentionnellement EXCLU du cleanup :
    #    pas de données personnelles, utilisé pour reporting historique
    # Logs techniques → audit & debug : 1 an
    'reclamations.pipeline_runs': {
        'days': 365,
        'date_column': 'created_at',
        'reason': 'Logs techniques (audit)',
    },
}

# ==============================================================================
# TÂCHE 1 : Nettoyage données personnelles (RGPD - 90 jours)
# ==============================================================================

def cleanup_personal_data(**context):
    """
    Supprime les données personnelles de plus de 90 jours.
    Conformité RGPD : reclamations_cleaned contient nom, adresse, client_id.
    Les kpis_daily sont CONSERVÉS (données agrégées, pas de données perso).
    """
    start_time = datetime.now()

    conn = psycopg2.connect(
        host="reclamations-postgres",
        database="reclamations_db",
        user="airflow",
        password="airflow_local_dev"
    )
    cur = conn.cursor()

    total_deleted = 0
    results = []

    for table, policy in RETENTION_POLICY.items():
        cutoff_date = datetime.now() - timedelta(days=policy['days'])
        date_col = policy['date_column']

        # Compter avant suppression
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {date_col} < %s", (cutoff_date,))
        count_to_delete = cur.fetchone()[0]

        if count_to_delete > 0:
            cur.execute(f"DELETE FROM {table} WHERE {date_col} < %s", (cutoff_date,))
            deleted = cur.rowcount
        else:
            deleted = 0

        total_deleted += deleted
        results.append({
            'table': table,
            'deleted': deleted,
            'retention_days': policy['days'],
            'reason': policy['reason'],
            'cutoff_date': cutoff_date.date(),
        })

        print(f"🗑️  {table} : {deleted:,} lignes supprimées "
              f"(>{policy['days']}j — {policy['reason']})")

    conn.commit()

    # Vérifier que kpis_daily est intact (données agrégées précieuses)
    cur.execute("SELECT COUNT(*) FROM reclamations.kpis_daily")
    kpis_count = cur.fetchone()[0]

    duration = (datetime.now() - start_time).total_seconds()

    # Logger le run
    cur.execute("""
        INSERT INTO reclamations.pipeline_runs 
        (dag_id, run_id, status, start_date, end_date, duration_seconds, message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        'data_cleanup_weekly',
        context['run_id'],
        'SUCCESS',
        start_time,
        datetime.now(),
        duration,
        f"🗑️ {total_deleted:,} lignes supprimées | kpis_daily conservés : {kpis_count:,} lignes"
    ))

    conn.commit()

    print(f"""
╔══════════════════════════════════════════════════════╗
║         ✅ NETTOYAGE RGPD TERMINÉ                   ║
╠══════════════════════════════════════════════════════╣
║  Données supprimées                                  ║
║  • reclamations_cleaned : 90j (RGPD)                ║
║  • pipeline_runs        : 365j (logs techniques)    ║
║                                                      ║
║  Données CONSERVÉES                                  ║
║  • kpis_daily : {kpis_count:,} lignes (agrégées, 3 ans)  
╚══════════════════════════════════════════════════════╝
    """)

    cur.close()
    conn.close()


task_cleanup = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_personal_data,
    dag=dag,
)

dag.doc_md = """
# Nettoyage Hebdomadaire — Rétention Différenciée

## 🎯 Politique de rétention

| Table | Rétention | Raison |
|-------|-----------|--------|
| `reclamations_cleaned` | **90 jours** | Données personnelles → RGPD obligatoire |
| `kpis_daily` | **3 ans** | Données agrégées anonymes → reporting historique |
| `pipeline_runs` | **1 an** | Logs techniques → audit & debug |

## ⚠️ Principe clé

Les `kpis_daily` ne contiennent **aucune donnée personnelle** (agrégations par région/type).
Ils sont volontairement exclus du cleanup pour permettre le reporting historique
et la comparaison des performances année par année.

## 📅 Fréquence

Exécution chaque **dimanche à 3h AM**.
"""
