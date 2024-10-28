# Python et Data Engineering : DAG Airflow

### Table des Matières

1. [Introduction](#Introduction)

## Introduction

Ce repertoire contient mon travail pour l'exercice Python et Data Engineering.

Dans cet exercice, j'ai implémenté une pipeline qui permet traiter des données pour générer les relations entre les médicaments et leur mention dans les journaux scientifiques en utilisant en utilisant Google BigQuery et Airflow pour l'orchestration. Cette pipeline charge les données depuis Google Cloud Storage (GCS), les prétraite dans BigQuery et génère un graphe final des mentions, qui est ensuite exporté vers GCS.

Cette approche est la plus adaptée pour des données de grande taille (plusieurs To).

## Structure du projet

La structure du projet est organisée comme suit:

```plaintext
airflow_home/
├── dags/
│   └── drug_mentions_pipeline_v3.py      # Fichier DAG orchestrant la pipeline
├── queries/                              # Dossier contenant les requêtes SQL pour le prétraitement et la génération du graphe de mentions
│   ├── preprocess_pubmed.sql             # Script SQL pour le prétraitement des données PubMed
│   ├── preprocess_trials.sql             # Script SQL pour le prétraitement des données d'essais cliniques
│   └── generate_mentions_graph.sql       # Script SQL pour générer le graphe final des mentions
└── data/
    └── raw/                              # Dossier de données brutes (fichiers d'entrée dans GCS)
```

## Exécution de La Pipeline

### Étape 1 : Configurer les Variables dans Airflow

Définissez les variables suivantes dans Airflow avec les détails de votre projet :

- `project_id`, `raw_dataset`, `staging_dataset`, `datawarehouse_dataset`, `composer_bucket`.

### Étape 2 : Placer les Fichiers dans GCS

Placer tous les fichiers de ce repertoire (`data/*` et `dags/*`) dans le dossier root de Airflow (par exemple, ~/airflow, ou directement dans le Bucket GCS si vous utilisez Cloud Composer).

### Étape 3 : Exécuter le DAG

Déclenchez le DAG manuellement pour démarrer le pipeline.

L'image ci-dessous montre le graphe de la pipeline dans Airflow :

![DAG Graph](images/dag_graph.png)

## Resusltats

Après l'exécution du DAG, on obtient une table BigQuery contenant les relations entre les médicaments et les journaux scientifiques, ainsi qu'un fichier JSON exporté dans GCS représentant le graphe des mentions.

L'image ci-dessous montre une partie de la table BigQuery générée :

![BigQuery Graph Table](images/bigquery_graph_table.png)

#### Remarque

J'ai du transformer le fichier `data/raw/pubmed.json` en NEWLINE_DELIMITED_JSON pour pouvoir l'importer dans BigQuery. Pour ce faire, j'ai utilisé la commande suivante (commande bash fonctionne egalement sur Windows avec Git Bash) :

```bash
jq -c '.[]' pubmed.json > pubmed_newline.json && mv pubmed_newline.json pubmed.json
```
