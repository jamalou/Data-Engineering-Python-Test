# Python et Data Engineering

Ce repertoire contient mon travail pour l'exercice Python et Data Engineering.

Dans cet exercice, on a implémenté un pipeline de traitement de données qui permet traiter des données pour générer les relations entre les médicaments et leur mention dans les journaux scientifiques.

## Table des matières:

- [Structure du projet](#structure-du-projet)
- [Étapes de Traitement et de Nettoyage des Données](#étapes-de-traitement-et-de-nettoyage-des-données)
- [Exécution de la Pipeline](#exécution-de-la-pipeline)

## Structure du projet

La structure du projet est organisée comme suit:

```plaintext
python-pipeline/
├── data/                              # Dossier contenant les fichiers de données (e.g., drugs.csv, pubmed.csv, clinical_trials.csv)
├── src/                               # Dossier principal du code source
│   ├── __init__.py
│   ├── data_pipeline.py               # Script principal pour exécuter la pipeline
│   └── utils.py                       # Fonctions utilitaires, incluant le nettoyage des données et le parsing des dates
├── tests/                             # Dossier pour les tests unitaires
│   ├── __init__.py
│   └── test_utils.py                  # Tests unitaires pour les fonctions utilitaires
├── outputs/                           # Dossier pour les outputs générés par la pipeline (e.g., drugs_mentions_graph.json, drugs_mentions_graph.png)
│   ├── drugs_mentions_graph.json      # Fichier json contenant les relations entre les médicaments et leur mention dans les journaux scientifiques
│   └── drugs_mentions_graph.png       # Image du graphe des relations entre les médicaments et leur mention dans les journaux scientifiques
└── README.md                          # Documentation du projet
```

## Étapes de Traitement et de Nettoyage des Données

### 1. Chargement des Données

Les données sont chargées depuis des fichiers CSV et JSON (e.g., drugs.csv, pubmed.csv, clinical_trials.csv) avec pandas.

### 2. Nettoyage des Données

- **Parsing des Dates :** La fonction parse_date standardise les formats de date variés pour assurer une représentation uniforme des dates.
- **Suppression des Duplicatas :** Les duplicatas sont supprimés en fonction de la colonne title pour éviter les entrées redondantes dans les analyses.
- **Renommage des Colonnes :** Les noms de colonnes sont normalisés pour la cohérence (e.g., scientific_title devient title), ce qui facilite l'accès aux champs à travers différentes sources de données.

### 3. Transformation des Données

La pipeline organise les données dans une structure basée sur un dictionnaire où chaque médicament est une clé. Pour chaque médicament, les journaux sont organisés en sous-clés, et chaque mention est catégorisée par source (PubMed ou Clinical Trial). Cette structure permet un requetage efficace et une représentation claire des relations entre les médicaments, les journaux et les sources.

**Exemple de Structure de Données Finale :**

```json
{
  "Tetracycline": {
    "Journal of food protection": {
      "PubMed": [
        {
          "title": "Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains.",
          "date": "2020-01-01"
        }
      ],
      "Clinical Trial": [
        {
          "title": "Tranexamic Acid Versus Epinephrine During Exploratory Tympanotomy",
          "date": "2020-04-27"
        }
      ]
    },
    "American journal of veterinary research": {
      "PubMed": [
        {
          "title": "Appositional Tetracycline bone formation rates in the Beagle.",
          "date": "2020-01-02"
        }
      ],
      "Clinical Trial": []
    }
  },
  "Ethanol": {
    "Psychopharmacology": {
      "PubMed": [
        {
          "title": "Rapid reacquisition of contextual fear following extinction in mice",
          "date": "2020-01-01"
        }
      ],
      "Clinical Trial": []
    }
  }
}
```

### 4. Sortie en JSON et Génération de Graphes (Optionnel)

La structure finale est exportée au format JSON. Optionnellement, un graphe des relations entre les médicaments et leur mention dans les journaux scientifiques est généré et sauvegardé en tant qu'image PNG (si vous avez installé `graphviz`).

## Exécution de la Pipeline

### Depot des fichiers de données dans le dossier `data/` :

- `drugs.csv`
- `pubmed.csv`
- `pubmed.json`
- `clinical_trials.csv`

### Changement de Répertoire :

```bash
cd "Python Data Engineering/python"
```

### Installation des Dépendances :

```bash
pip install -r requirements.txt
```

### Exécution de la Pipeline :

```bash
python src/data_pipeline.py
```

### Exécution des Tests :

```bash
python -m unittest discover -s tests
```
