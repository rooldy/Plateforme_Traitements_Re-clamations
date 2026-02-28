"""
Configuration Architecture Médaillon (Bronze/Silver/Gold).
Définit les chemins et règles pour chaque couche.

Auteur: Data Engineering Portfolio
Date: Février 2025
"""

import os
from enum import Enum
from typing import Dict


class MedallionLayer(Enum):
    """Énumération des couches médaillon."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class MedallionConfig:
    """Configuration centralisée de l'architecture médaillon."""
    
    # Répertoire de base
    BASE_PATH = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    DATA_ROOT = f"{BASE_PATH}/data"
    
    # ========================================================================
    # COUCHE BRONZE : Données brutes (as-is)
    # ========================================================================
    BRONZE_PATH = f"{DATA_ROOT}/bronze"
    
    BRONZE_TABLES = {
        "reclamations": f"{BRONZE_PATH}/reclamations",
        "incidents": f"{BRONZE_PATH}/incidents",
        "clients": f"{BRONZE_PATH}/clients",
        "compteurs_linky": f"{BRONZE_PATH}/compteurs_linky",
        "postes_sources": f"{BRONZE_PATH}/postes_sources",
        "historique_releves": f"{BRONZE_PATH}/historique_releves",
        "historique_interventions": f"{BRONZE_PATH}/historique_interventions",
        "factures": f"{BRONZE_PATH}/factures",
        "meteo_quotidienne": f"{BRONZE_PATH}/meteo_quotidienne"
    }
    
    # Caractéristiques BRONZE
    BRONZE_FORMAT = "parquet"
    BRONZE_MODE = "overwrite"
    BRONZE_COMPRESSION = "snappy"
    BRONZE_DESCRIPTION = """
    Couche BRONZE : Données brutes
    - Format : Parquet (conversion depuis CSV)
    - Validation : Schéma strict uniquement
    - Transformation : Aucune
    - Qualité : Pas de nettoyage
    - Usage : Archive, retraitement, audit
    """
    
    # ========================================================================
    # COUCHE SILVER : Données nettoyées & validées
    # ========================================================================
    SILVER_PATH = f"{DATA_ROOT}/silver"
    
    SILVER_TABLES = {
        "reclamations_cleaned": f"{SILVER_PATH}/reclamations_cleaned",
        "incidents_validated": f"{SILVER_PATH}/incidents_validated",
        "clients_deduplicated": f"{SILVER_PATH}/clients_deduplicated",
        "compteurs_verified": f"{SILVER_PATH}/compteurs_verified",
        "postes_validated": f"{SILVER_PATH}/postes_validated",
        "releves_anomalies_detected": f"{SILVER_PATH}/releves_anomalies_detected",
        "interventions_cleaned": f"{SILVER_PATH}/interventions_cleaned",
        "factures_validated": f"{SILVER_PATH}/factures_validated",
        "meteo_cleaned": f"{SILVER_PATH}/meteo_cleaned"
    }
    
    # Caractéristiques SILVER
    SILVER_FORMAT = "delta"  # Delta Lake pour ACID + time travel
    SILVER_MODE = "merge"
    SILVER_PARTITION_BY = ["region", "year", "month"]
    SILVER_DESCRIPTION = """
    Couche SILVER : Données nettoyées
    - Format : Delta Lake (ACID, time travel)
    - Validation : Quality checks (score >90%)
    - Transformation : Nettoyage, déduplication, validation
    - Qualité : Haute (>95% complétude)
    - Usage : Analytics, ML, reporting
    """
    
    # ========================================================================
    # COUCHE GOLD : Données business-ready
    # ========================================================================
    GOLD_PATH = f"{DATA_ROOT}/gold"
    
    GOLD_TABLES = {
        # Tables métier enrichies
        "reclamations_enriched": f"{GOLD_PATH}/reclamations_enriched",
        "reclamations_linky_detailed": f"{GOLD_PATH}/reclamations_linky_detailed",
        
        # KPIs et agrégations
        "kpis_daily": f"{GOLD_PATH}/kpis_daily",
        "kpis_weekly": f"{GOLD_PATH}/kpis_weekly",
        "kpis_monthly": f"{GOLD_PATH}/kpis_monthly",
        
        # Analytics
        "analytics_by_region": f"{GOLD_PATH}/analytics_by_region",
        "analytics_by_type": f"{GOLD_PATH}/analytics_by_type",
        "sla_compliance": f"{GOLD_PATH}/sla_compliance",
        
        # Reporting
        "dashboard_volumetrie": f"{GOLD_PATH}/dashboard_volumetrie",
        "dashboard_performance": f"{GOLD_PATH}/dashboard_performance",
        
        # Machine Learning ready
        "ml_features": f"{GOLD_PATH}/ml_features",
        "ml_training_data": f"{GOLD_PATH}/ml_training_data"
    }
    
    # Caractéristiques GOLD
    GOLD_FORMAT = "delta"
    GOLD_MODE = "merge"
    GOLD_PARTITION_BY = ["date", "region"]
    GOLD_DESCRIPTION = """
    Couche GOLD : Données business-ready
    - Format : Delta Lake (optimisé requêtes)
    - Validation : Business rules appliquées
    - Transformation : Enrichissements, corrélations, KPIs
    - Qualité : Production (>98%)
    - Usage : Dashboards, BI, ML, API
    """
    
    # ========================================================================
    # RÈGLES DE QUALITÉ PAR COUCHE
    # ========================================================================
    QUALITY_THRESHOLDS = {
        MedallionLayer.BRONZE: {
            "completeness": 0.70,  # 70% minimum (données brutes)
            "validity": 0.80,
            "consistency": 0.75
        },
        MedallionLayer.SILVER: {
            "completeness": 0.90,  # 90% minimum (nettoyé)
            "validity": 0.95,
            "consistency": 0.90,
            "uniqueness": 0.98
        },
        MedallionLayer.GOLD: {
            "completeness": 0.98,  # 98% minimum (production)
            "validity": 0.99,
            "consistency": 0.98,
            "uniqueness": 0.99,
            "accuracy": 0.95
        }
    }
    
    # ========================================================================
    # RETENTION POLICIES
    # ========================================================================
    RETENTION_DAYS = {
        MedallionLayer.BRONZE: 730,   # 2 ans (archive)
        MedallionLayer.SILVER: 365,   # 1 an (analytics)
        MedallionLayer.GOLD: 90       # 3 mois (dashboards actualisés)
    }
    
    # ========================================================================
    # HELPERS
    # ========================================================================
    
    @classmethod
    def get_path(cls, layer: MedallionLayer, table: str) -> str:
        """
        Retourne le chemin pour une table dans une couche donnée.
        
        Args:
            layer: Couche médaillon (BRONZE, SILVER, GOLD)
            table: Nom de la table
            
        Returns:
            Chemin complet vers la table
        """
        if layer == MedallionLayer.BRONZE:
            return cls.BRONZE_TABLES.get(table, f"{cls.BRONZE_PATH}/{table}")
        elif layer == MedallionLayer.SILVER:
            return cls.SILVER_TABLES.get(table, f"{cls.SILVER_PATH}/{table}")
        elif layer == MedallionLayer.GOLD:
            return cls.GOLD_TABLES.get(table, f"{cls.GOLD_PATH}/{table}")
        else:
            raise ValueError(f"Couche inconnue: {layer}")
    
    @classmethod
    def get_quality_threshold(cls, layer: MedallionLayer) -> Dict[str, float]:
        """Retourne les seuils de qualité pour une couche."""
        return cls.QUALITY_THRESHOLDS.get(layer, {})
    
    @classmethod
    def get_retention_days(cls, layer: MedallionLayer) -> int:
        """Retourne la durée de rétention pour une couche."""
        return cls.RETENTION_DAYS.get(layer, 90)
    
    @classmethod
    def validate_layer(cls, layer: str) -> MedallionLayer:
        """Valide et retourne l'énumération de couche."""
        try:
            return MedallionLayer(layer.lower())
        except ValueError:
            raise ValueError(f"Couche invalide: {layer}. Valeurs possibles: bronze, silver, gold")


# ============================================================================
# DOCUMENTATION ARCHITECTURE MÉDAILLON
# ============================================================================

MEDALLION_ARCHITECTURE_DOC = """
# Architecture Médaillon - Plateforme Réclamations

## 🏗️ Vue d'Ensemble

L'architecture médaillon organise les données en 3 couches de qualité croissante :

```
CSV Sources
    ↓
┌──────────────────────────────────────┐
│  BRONZE : Données brutes (as-is)    │
│  - Format : Parquet                  │
│  - Qualité : 70-80%                  │
│  - Usage : Archive, retraitement     │
└──────────────┬───────────────────────┘
               ↓ (Quality checks)
┌──────────────────────────────────────┐
│  SILVER : Données nettoyées          │
│  - Format : Delta Lake               │
│  - Qualité : 90-95%                  │
│  - Usage : Analytics, ML             │
└──────────────┬───────────────────────┘
               ↓ (Enrichissements)
┌──────────────────────────────────────┐
│  GOLD : Données business-ready       │
│  - Format : Delta Lake optimisé      │
│  - Qualité : 98-99%                  │
│  - Usage : Dashboards, BI, API       │
└──────────────────────────────────────┘
```

## 📊 Flux de Données

### BRONZE → SILVER
**Transformations :**
- Validation schéma strict
- Détection et suppression doublons
- Détection valeurs nulles
- Validation formats (dates, références Linky)
- Détection anomalies statistiques

**Quality Gate :** Score global ≥ 90%

### SILVER → GOLD
**Transformations :**
- Enrichissements métier (durées, scores)
- Corrélations géographiques (incidents)
- Détection récurrence
- Calcul KPIs business
- Agrégations par région/type/date

**Quality Gate :** Score global ≥ 98%

## 🎯 Avantages

1. **Séparation des préoccupations** : Chaque couche a un rôle clair
2. **Qualité progressive** : Amélioration à chaque étape
3. **Reproductibilité** : Bronze = source de vérité
4. **Performance** : Gold optimisé pour requêtes
5. **Audit** : Traçabilité complète des transformations
6. **Time Travel** : Delta Lake permet de revenir en arrière

## 🔄 Retraitement

Si erreur détectée dans SILVER/GOLD :
1. Corriger la logique
2. Relire depuis BRONZE (source de vérité)
3. Recalculer SILVER puis GOLD

**Bronze = immuable, SILVER/GOLD = recalculables**
"""


def print_architecture_summary():
    """Affiche un résumé de l'architecture médaillon."""
    print("="*80)
    print("🏗️  ARCHITECTURE MÉDAILLON - CONFIGURATION")
    print("="*80)
    print(f"\n📂 Chemins configurés :")
    print(f"   BRONZE : {MedallionConfig.BRONZE_PATH}")
    print(f"   SILVER : {MedallionConfig.SILVER_PATH}")
    print(f"   GOLD   : {MedallionConfig.GOLD_PATH}")
    
    print(f"\n📊 Nombre de tables par couche :")
    print(f"   BRONZE : {len(MedallionConfig.BRONZE_TABLES)} tables")
    print(f"   SILVER : {len(MedallionConfig.SILVER_TABLES)} tables")
    print(f"   GOLD   : {len(MedallionConfig.GOLD_TABLES)} tables")
    
    print(f"\n✅ Seuils de qualité :")
    for layer in MedallionLayer:
        thresholds = MedallionConfig.get_quality_threshold(layer)
        print(f"   {layer.value.upper():8s} : {thresholds}")
    
    print(f"\n🗄️  Rétention :")
    for layer in MedallionLayer:
        days = MedallionConfig.get_retention_days(layer)
        print(f"   {layer.value.upper():8s} : {days} jours")
    
    print("="*80)


if __name__ == "__main__":
    print_architecture_summary()
