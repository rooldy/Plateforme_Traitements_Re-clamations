# ⚡ Guide de Démarrage Rapide - 5 Minutes

Ce guide vous permet de démarrer le projet en moins de 5 minutes.

---

## 📋 Prérequis

✅ Docker et Docker Compose installés  
✅ 8GB RAM minimum  
✅ 10GB d'espace disque libre  

---

## 🚀 Démarrage en 4 étapes

### Étape 1 : Cloner et configurer

```bash
# Cloner le projet
git clone <URL_DU_REPO>
cd Plateforme_Traitement_Reclamations

# Utiliser le script de démarrage automatique
./start.sh
```

**Le script `start.sh` fait automatiquement :**
- ✅ Vérification des prérequis
- ✅ Création de la structure de répertoires
- ✅ Copie de `.env.example` vers `.env`
- ✅ Génération des données synthétiques (20k réclamations)
- ✅ Démarrage de tous les conteneurs Docker
- ✅ Attente que les services soient prêts

---

### Étape 2 : Accéder à Airflow

```
URL      : http://localhost:8080
Username : admin
Password : admin
```

---

### Étape 3 : Lancer le pipeline

1. Dans l'interface Airflow, activer le toggle du DAG
2. Cliquer sur "Trigger DAG" (▶️)
3. Suivre l'exécution dans la vue Graph

---

### Étape 4 : Vérifier les résultats

```bash
# Se connecter à PostgreSQL
docker exec -it reclamations_postgres psql -U airflow -d reclamations_db

# Compter les réclamations
SELECT COUNT(*) FROM reclamations.reclamations_cleaned;

# Voir les KPIs
SELECT * FROM reclamations.kpis_daily LIMIT 5;
```

---

## 🛠️ Commandes utiles

```bash
# Voir les logs
cd docker && docker-compose logs -f

# Arrêter
cd docker && docker-compose down

# Redémarrer
cd docker && docker-compose restart

# Nettoyer complètement
cd docker && docker-compose down -v
```

---

## 🐛 Problèmes fréquents

### Port 8080 déjà utilisé
```bash
# Trouver le processus
lsof -i :8080

# Changer le port dans docker-compose.yml
ports:
  - "8081:8080"  # Au lieu de 8080:8080
```

### Pas assez de mémoire
```bash
# Réduire les limites dans docker-compose.yml
memory: 1G  # Au lieu de 2G
```

### Les données n'existent pas
```bash
# Générer manuellement
cd spark/utils
python3 data_generator.py
```

---

## 📚 Documentation complète

Pour plus de détails, consultez le [README principal](README.md).

---

**🚀 Vous êtes prêt !**
