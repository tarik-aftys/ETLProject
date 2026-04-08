# ETLProject

## Description
Ce projet est une solution complète d'ETL (Extract, Transform, Load) pour un pipeline de données e-commerce. Il inclut des étapes d'ingestion, de transformation, d'orchestration et de visualisation des données.

## Fonctionnalités principales
- **Ingestion des données** : Extraction des données brutes à partir de fichiers CSV et d'API.
- **Transformation des données** : Utilisation de dbt pour créer un modèle en étoile.
- **Orchestration** : Gestion des workflows avec Apache Airflow.
- **Visualisation** : Tableau de bord interactif avec Streamlit.

## Structure du projet
```
ETLProject/
├── data/
│   ├── raw/                # Données brutes
│   ├── processed/          # Données transformées
├── dags/                   # Workflows Airflow
├── ecommerce_dbt/          # Projet dbt pour les transformations
├── scripts/                # Scripts Python pour l'ingestion
├── dashboard_app.py        # Application Streamlit
├── docker-compose.yml      # Configuration Docker
└── README.md               # Documentation du projet
```

## Prérequis
- Python 3.8+
- Docker
- PostgreSQL

## Installation
1. Clonez le dépôt :
   ```bash
   git clone https://github.com/tarik-aftys/ETLProject.git
   ```
2. Accédez au répertoire du projet :
   ```bash
   cd ETLProject
   ```
3. Installez les dépendances Python :
   ```bash
   pip install -r requirements.txt
   ```

## Utilisation
1. Lancez les conteneurs Docker :
   ```bash
   docker-compose up
   ```
2. Accédez au tableau de bord Streamlit :
   ```bash
   streamlit run dashboard_app.py
   ```

