#!/bin/bash
# Script de démarrage rapide pour la plateforme de traitement de réclamations
# Ce script automatise toutes les étapes nécessaires au premier lancement

set -e  # Arrêt en cas d'erreur

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonction d'affichage
print_step() {
    echo -e "${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Banner
echo -e "${BLUE}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════╗
║   Plateforme de Traitement de Réclamations Clients       ║
║   Script de Démarrage Automatique                        ║
╚═══════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Vérification des prérequis
print_step "1. Vérification des prérequis"

if ! command -v docker &> /dev/null; then
    print_error "Docker n'est pas installé. Veuillez l'installer d'abord."
    exit 1
fi
print_success "Docker installé : $(docker --version)"

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose n'est pas installé. Veuillez l'installer d'abord."
    exit 1
fi
print_success "Docker Compose installé : $(docker-compose --version)"

if ! command -v python3 &> /dev/null; then
    print_warning "Python3 n'est pas installé. Le générateur de données ne pourra pas être exécuté."
    SKIP_DATA_GENERATION=true
else
    print_success "Python3 installé : $(python3 --version)"
    SKIP_DATA_GENERATION=false
fi

# Création des répertoires
print_step "2. Création de la structure de répertoires"

mkdir -p data/{raw,processed,raw_parquet}
mkdir -p logs
print_success "Répertoires créés"

# Configuration
print_step "3. Configuration de l'environnement"

if [ ! -f .env ]; then
    cp .env.example .env
    print_success "Fichier .env créé à partir de .env.example"
else
    print_warning "Le fichier .env existe déjà, conservation de la configuration actuelle"
fi

# Génération des données
if [ "$SKIP_DATA_GENERATION" = false ]; then
    print_step "4. Génération des données synthétiques"
    
    if [ -f data/raw/reclamations.csv ]; then
        print_warning "Les données existent déjà. Souhaitez-vous les régénérer ? (y/N)"
        read -r response
        if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
            cd spark/utils
            python3 data_generator.py
            cd ../..
            print_success "Données régénérées avec succès"
        else
            print_warning "Utilisation des données existantes"
        fi
    else
        cd spark/utils
        python3 data_generator.py
        cd ../..
        print_success "Données générées avec succès"
    fi
else
    print_warning "Génération de données ignorée (Python3 non disponible)"
fi

# Vérification de la présence des données
print_step "5. Vérification des données"

if [ ! -f data/raw/reclamations.csv ]; then
    print_error "Le fichier reclamations.csv est manquant dans data/raw/"
    print_warning "Vous devez générer les données manuellement avec :"
    echo "  cd spark/utils && python3 data_generator.py"
    exit 1
fi

RECLAMATIONS_COUNT=$(wc -l < data/raw/reclamations.csv)
print_success "Réclamations : $RECLAMATIONS_COUNT lignes"

if [ -f data/raw/incidents.csv ]; then
    INCIDENTS_COUNT=$(wc -l < data/raw/incidents.csv)
    print_success "Incidents : $INCIDENTS_COUNT lignes"
fi

if [ -f data/raw/clients.csv ]; then
    CLIENTS_COUNT=$(wc -l < data/raw/clients.csv)
    print_success "Clients : $CLIENTS_COUNT lignes"
fi

# Démarrage des conteneurs
print_step "6. Démarrage de l'infrastructure Docker"

cd docker

# Vérifier si les conteneurs existent déjà
if [ "$(docker ps -a -q -f name=reclamations_)" ]; then
    print_warning "Des conteneurs existent déjà. Souhaitez-vous les recréer ? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_warning "Arrêt et suppression des conteneurs existants..."
        docker-compose down -v
    fi
fi

echo "Démarrage des services (cela peut prendre quelques minutes)..."
docker-compose up -d

cd ..

# Attente que les services soient prêts
print_step "7. Attente de la disponibilité des services"

echo "Attente de PostgreSQL..."
RETRY=0
MAX_RETRY=30
until docker exec reclamations_postgres pg_isready -U airflow > /dev/null 2>&1 || [ $RETRY -eq $MAX_RETRY ]; do
    echo -n "."
    RETRY=$((RETRY+1))
    sleep 2
done

if [ $RETRY -eq $MAX_RETRY ]; then
    print_error "PostgreSQL n'a pas démarré dans les temps"
    exit 1
fi
print_success "PostgreSQL est prêt"

echo "Attente d'Airflow (cela peut prendre 60-90 secondes)..."
RETRY=0
MAX_RETRY=60
until curl -s http://localhost:8080/health > /dev/null 2>&1 || [ $RETRY -eq $MAX_RETRY ]; do
    echo -n "."
    RETRY=$((RETRY+1))
    sleep 3
done

if [ $RETRY -eq $MAX_RETRY ]; then
    print_warning "Airflow semble long à démarrer. Vérifiez les logs avec :"
    echo "  cd docker && docker-compose logs airflow-webserver"
else
    print_success "Airflow est prêt"
fi

# Résumé final
print_step "8. Résumé et prochaines étapes"

print_success "✅ Infrastructure démarrée avec succès !"
echo ""
echo -e "${GREEN}🌐 Accès aux services :${NC}"
echo -e "  - Airflow Web UI : ${BLUE}http://localhost:8080${NC}"
echo -e "    Username : ${YELLOW}admin${NC}"
echo -e "    Password : ${YELLOW}admin${NC}"
echo ""
echo -e "  - PostgreSQL : ${BLUE}localhost:5432${NC}"
echo -e "    Database : ${YELLOW}reclamations_db${NC}"
echo -e "    Username : ${YELLOW}airflow${NC}"
echo -e "    Password : ${YELLOW}airflow${NC}"
echo ""
echo -e "${GREEN}📋 Prochaines étapes :${NC}"
echo "  1. Ouvrir http://localhost:8080 dans votre navigateur"
echo "  2. Se connecter avec admin/admin"
echo "  3. Activer le DAG 'reclamation_pipeline'"
echo "  4. Cliquer sur 'Trigger DAG' pour lancer le pipeline"
echo ""
echo -e "${GREEN}🔧 Commandes utiles :${NC}"
echo "  - Voir les logs : cd docker && docker-compose logs -f"
echo "  - Arrêter       : cd docker && docker-compose down"
echo "  - Redémarrer    : cd docker && docker-compose restart"
echo ""
echo -e "${BLUE}📚 Documentation complète dans README.md${NC}"
echo ""

print_success "🚀 Bon développement !"
