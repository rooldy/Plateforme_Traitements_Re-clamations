"""
Extension des schémas PySpark pour les 6 nouvelles tables V2.
À ajouter dans spark_config.py

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, BooleanType
)

# ============================================================================
# SCHÉMAS ÉTENDUS V2
# ============================================================================

# SCHÉMA 1 : Compteurs Linky
SCHEMA_COMPTEURS_LINKY = StructType([
    StructField("reference_linky", StringType(), False),
    StructField("type_compteur", StringType(), False),
    StructField("puissance_souscrite_kva", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("departement", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("date_installation", DateType(), False),
    StructField("statut", StringType(), False),
    StructField("est_defectueux", BooleanType(), False),
    StructField("nombre_incidents", IntegerType(), False)
])

# SCHÉMA 2 : Postes Sources
SCHEMA_POSTES_SOURCES = StructType([
    StructField("id_poste", StringType(), False),
    StructField("nom_poste", StringType(), False),
    StructField("type_poste", StringType(), False),
    StructField("region", StringType(), False),
    StructField("departement", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("capacite_mva", IntegerType(), False),
    StructField("taux_charge_moyen", DoubleType(), True),
    StructField("nombre_clients_desservis", IntegerType(), True),
    StructField("statut", StringType(), False),
    StructField("date_mise_service", DateType(), True)
])

# SCHÉMA 3 : Historique Relevés
SCHEMA_HISTORIQUE_RELEVES = StructType([
    StructField("reference_linky", StringType(), False),
    StructField("date_releve", DateType(), False),
    StructField("index_hp", IntegerType(), True),
    StructField("index_hc", IntegerType(), True),
    StructField("consommation_kwh", IntegerType(), False),
    StructField("type_releve", StringType(), False),
    StructField("anomalie_detectee", BooleanType(), False),
    StructField("type_anomalie", StringType(), True)
])

# SCHÉMA 4 : Historique Interventions
SCHEMA_HISTORIQUE_INTERVENTIONS = StructType([
    StructField("id_intervention", StringType(), False),
    StructField("reference_linky", StringType(), False),
    StructField("type_intervention", StringType(), False),
    StructField("date_intervention", StringType(), False),  # Timestamp as string
    StructField("technicien_id", StringType(), False),
    StructField("duree_minutes", IntegerType(), False),
    StructField("statut", StringType(), False),
    StructField("resolu_premier_passage", BooleanType(), True),
    StructField("cout_intervention_euros", DoubleType(), True),
    StructField("commentaire", StringType(), True)
])

# SCHÉMA 5 : Factures
SCHEMA_FACTURES = StructType([
    StructField("numero_facture", StringType(), False),
    StructField("reference_linky", StringType(), False),
    StructField("date_emission", DateType(), False),
    StructField("date_echeance", DateType(), False),
    StructField("consommation_kwh", IntegerType(), False),
    StructField("montant_ht", DoubleType(), False),
    StructField("montant_tva", DoubleType(), False),
    StructField("montant_ttc", DoubleType(), False),
    StructField("statut_paiement", StringType(), False),
    StructField("date_paiement", DateType(), True),
    StructField("est_contestee", BooleanType(), False),
    StructField("motif_contestation", StringType(), True)
])

# SCHÉMA 6 : Météo Quotidienne
SCHEMA_METEO_QUOTIDIENNE = StructType([
    StructField("date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("temperature_min_celsius", DoubleType(), True),
    StructField("temperature_max_celsius", DoubleType(), True),
    StructField("condition_meteo", StringType(), False),
    StructField("precipitations_mm", DoubleType(), False),
    StructField("vitesse_vent_kmh", DoubleType(), False),
    StructField("evenement_extreme", BooleanType(), False),
    StructField("alerte_meteo", BooleanType(), False)
])


# ============================================================================
# DICTIONNAIRE GLOBAL DES SCHÉMAS (à ajouter dans SparkConfig)
# ============================================================================

ALL_SCHEMAS_V2 = {
    # Existants
    "reclamations": None,  # Déjà défini dans spark_config.py
    "incidents": None,     # Déjà défini dans spark_config.py
    "clients": None,       # Déjà défini dans spark_config.py
    
    # Nouveaux V2
    "compteurs_linky": SCHEMA_COMPTEURS_LINKY,
    "postes_sources": SCHEMA_POSTES_SOURCES,
    "historique_releves": SCHEMA_HISTORIQUE_RELEVES,
    "historique_interventions": SCHEMA_HISTORIQUE_INTERVENTIONS,
    "factures": SCHEMA_FACTURES,
    "meteo_quotidienne": SCHEMA_METEO_QUOTIDIENNE
}


# ============================================================================
# INSTRUCTIONS D'INTÉGRATION
# ============================================================================

"""
Pour intégrer ces schémas dans spark_config.py, ajoutez :

1. Importer les nouveaux schémas en haut du fichier :
   from spark_config_extended import (
       SCHEMA_COMPTEURS_LINKY, SCHEMA_POSTES_SOURCES,
       SCHEMA_HISTORIQUE_RELEVES, SCHEMA_HISTORIQUE_INTERVENTIONS,
       SCHEMA_FACTURES, SCHEMA_METEO_QUOTIDIENNE
   )

2. Dans la classe SparkConfig, ajouter une méthode :
   
   @staticmethod
   def get_schema(table_name: str) -> StructType:
       '''Retourne le schéma pour une table donnée.'''
       schemas = {
           "reclamations": SCHEMA_RECLAMATIONS,
           "incidents": SCHEMA_INCIDENTS,
           "clients": SCHEMA_CLIENTS,
           "compteurs_linky": SCHEMA_COMPTEURS_LINKY,
           "postes_sources": SCHEMA_POSTES_SOURCES,
           "historique_releves": SCHEMA_HISTORIQUE_RELEVES,
           "historique_interventions": SCHEMA_HISTORIQUE_INTERVENTIONS,
           "factures": SCHEMA_FACTURES,
           "meteo_quotidienne": SCHEMA_METEO_QUOTIDIENNE
       }
       return schemas.get(table_name)
"""
