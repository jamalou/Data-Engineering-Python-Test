# Python et Data Engineering : DAG Airflow

### Table des Matières

1. [Introduction](#Introduction)
2. [Structure du projet](#Structure-du-projet)
3. [Workflow de la Pipeline](#Workflow-de-la-Pipeline)
4. [Considérations pour la Production](#Considérations-pour-la-Production)
5. [Exécution du DAG](#Exécution-du-DAG)
6. [Améliorations](#Améliorations)

## Introduction

Ce repertoire contient mon travail pour l'exercice Python et Data Engineering.

Dans cet exercice, j'ai implémenté une pipeline qui permet traiter des données pour générer les relations entre les médicaments et leur mention dans les journaux scientifiques en utilisant un DAG (Directed Acyclic Graph) avec Apache Airflow.

## Structure du projet

La structure du projet est organisée comme suit:

```plaintext
airflow_home/
├── dags/
│   └── drug_mentions_pipeline_v2.py           # Fichier DAG orchestrant la pipeline
├── data/
│   └── raw/                                   # Dossier de données brutes (fichiers d'entrée)
└── plugins/
    ├── custom_operators/
    │   └── scientific_mentions_operators.py   # Opérateurs personnalisés pour traiter les données
    └── includes/
        └── utils.py                           # Fonctions utilitaires
```

## Workflow de la Pipeline

Le fichier DAG (drug_mentions_pipeline_v2.py) définit un workflow de traitement des données. Voici les étapes principales de la pipeline:

### 1. Tâches de Chargement et de Traitement des Données

- `load_and_process_drugs` : Charge **drugs.csv**, met les noms de médicaments en majuscules, supprime les doublons et enregistre les données nettoyées dans le dossier intermédiaire.
- `load_and_process_pubmed` (Custom operator) : Charge les fichiers PubMed CSV et JSON, les concatène, standardise les formats de date et enregistre les données traitées.
- `load_and_process_trials` (Custom operator) : Charge le fichier des essais cliniques, renomme la colonne scientific_title en title, standardise les dates et enregistre le fichier traité.

### 2. Tâches de Traitement des Mentions

- `process_pubmed_mentions` (Custom operator) : Lit les données traitées des médicaments et de PubMed, identifie les mentions, crée un graphe reliant les médicaments aux journaux scientifiques, et enregistre les résultats en JSON.
- `process_trials_mentions` (Custom operator) : De la même manière, traite les mentions dans les données des essais cliniques et enregistre les résultats en JSON.

### 3. Tâches de Fusion des Mentions et de Dessin du Graphe

- `merge_pubmed_and_trials_mentions` : Fusionne les graphes de mentions de PubMed et des essais cliniques dans un fichier JSON final.
- `draw_graph_as_image` : Utilise le JSON final pour dessiner un graphe visuel des mentions de médicaments.

### 4. Tâche de Nettoyage

- `clean_up_intermediate_files` : Supprime les fichiers intermédiaires pour garder le dossier de sortie propre et éviter l'accumulation de données.

## Considérations pour la Production

- Opérateurs Personnalisés : Encapsulent des fonctionnalités spécifiques, rendant le code modulaire.
- Logs et Gestion des Erreurs
- Persistance des Données : Airflow n'est pas concu pour envoyer des données entre les tâches, donc les données intermédiaires sont sauvegardées sur le disque permettant à chaque étape de s'appuyer sur les sorties précédentes. Ainsi, si une tâche échoue, elle peut être relancée sans avoir à retraiter les données.
- Tâche de Nettoyage : Efface les fichiers intermédiaires.

## Exécution du DAG

### Étape 1 :

Placer tous les fichiers de ce repertoire (`data`, `dags` et `plugins`) dans le dossier root de Airflow (par exemple, ~/airflow, ou directement dans le Bucket GCS si vous utilisez Cloud Composer).

### Etape 3 :

Exécuter le DAG Dans l’interface Airflow

### Sorties

Les sorties de la pipeline seront enregistrées dans le dossier `outputs` du répertoire `data`.
`merged_mentions_graph.json` : Fichier JSON représentant le graphe fusionné des mentions
`drug_mentions_graph.png` : Représentation visuelle du graphe des mentions

## Améliorations

Airflow est un outil puissant pour la gestion des workflows de données, mais il n'est pas censé être utilisé pour le traitement de données dans ces wrokers. Pour des volumes de données plus importants, il serait préférable d'utiliser des outils de traitement de données comme BigQuery, Spark et d'utiliser Airflow pour orchestrer les tâches qui utilisent ces outils.

### PS :

Les fichiers de données sont disponibles dans le dossier `Python Data Engineering/python-pipeline-v2/data/raw` du repo GitHub.

Ce n'est pas une bonne pratique de les stocker dans le repo, mais puisque les données ne sont pas sensibles, et ne sont pas volumineuses, je les ai inclus dans le repo pour faciliter l'exécution de la pipeline.
