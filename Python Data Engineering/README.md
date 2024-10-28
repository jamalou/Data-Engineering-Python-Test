# Python et Data Engineering

Ce répertoire contient mon travail pour l'exercice Python et Data Engineering.

Dans cet exercice, j'ai choisit de faire trois versions de la pipeline:

1. [Version 1](./python-pipeline): pipeline python simple avec pandas, appropiée pour des petits volumes de données
2. [Version 2](./python-pipeline-v2): pipeline python avec Airflow, permettant de gérer de planifier et de gérer les tasks de manière plus robuste (e.g., retry, logging, execution en parallèle des tâches indépendantes, ...)
3. [Version 3](./python-pipeline-v3): pipeline python avec BigQuery et Airflow, permettant de gérer de gros volumes de données en utilisant BigQuery pour le stockage et le traitement des données.
