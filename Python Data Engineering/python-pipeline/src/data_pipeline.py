"""
Data pipeline script to process drug mentions from PubMed and Clinical Trials data.
"""
import json
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

from src.utils import parse_date, process_mentions, merge_mention_graphs, draw_graph

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
OUTPUTS_DIR = Path(__file__).resolve().parents[1] / 'outputs'

def load_data(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load data from CSV and JSON files.

    :param data_dir: Path: Path to the directory containing the input data files
    :return: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: DataFrames containing the input data
    """

    # check the presence of the data directory
    if not data_dir.exists():
        logging.error("Data directory not found.")
        raise FileNotFoundError(f"Data directory not found at {data_dir}")

    # check the presence of the drugs data file
    if not (data_dir / 'drugs.csv').exists():
        logging.error("Drugs data file not found.")
        raise FileNotFoundError(f"Drugs data file not found at {data_dir / 'drugs.csv'}")

    # check the presence of pubmed data files (CSV or JSON)
    if not (data_dir / 'pubmed.csv').exists() and not (data_dir / 'pubmed.json').exists():
        logging.error("PubMed data files not found.")
        raise FileNotFoundError(
            f"PubMed data files not found at {data_dir / 'pubmed.csv'} and {data_dir / 'pubmed.json'}"
        )

    # check the presence of the clinical trials data file
    if not (data_dir / 'clinical_trials.csv').exists():
        logging.error("Clinical Trials data file not found.")
        raise FileNotFoundError(f"Clinical Trials data file not found at {data_dir / 'clinical_trials.csv'}")

    # pandas may fail to infer date columns or infer them badly
    # so for now we will not parse the date column as string and handle it later
    drugs_df = pd.read_csv(data_dir / 'drugs.csv')
    pubmed_csv_df = pd.read_csv(data_dir / 'pubmed.csv', dtype=str)
    pubmed_json_df = pd.read_json(data_dir / 'pubmed.json', dtype=str, convert_dates=False)
    clinical_trials_df = pd.read_csv(data_dir / 'clinical_trials.csv', dtype=str)

    # Concatenate PubMed data from CSV and JSON files
    pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df])

    return drugs_df, pubmed_df, clinical_trials_df

def preprocess_data(
        drugs_df: pd.DataFrame,
        pubmed_df: pd.DataFrame,
        clinical_trials_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Preprocess data.
    * Capitalize drug names
    * Rename columns in clinical_trials_df
    * Remove duplicates from pubmed_df and clinical_trials_df
    * Parse dates in PubMed and Clinical Trials data

    :param drugs_df: pd.DataFrame: DataFrame containing drug names
    :param pubmed_df: pd.DataFrame: DataFrame containing PubMed data
    :param clinical_trials_df: pd.DataFrame: DataFrame containing Clinical Trials data
    :return: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Cleaned DataFrames
    """
    # Capitalize drug names
    drugs_df['drug'] = drugs_df['drug'].str.capitalize()
    logging.info("Drug names capitalized successfully.")

    # Rename columns in clinical_trials_df
    clinical_trials_df = clinical_trials_df.rename(columns={'scientific_title': 'title'})
    logging.info("Column renamed successfully.")

    # remove duplicates from pubmed_df and clinical_trials_df
    pubmed_df = pubmed_df.drop_duplicates(subset='title').reset_index(drop=True)
    clinical_trials_df = clinical_trials_df.drop_duplicates(subset='title').reset_index(drop=True)
    logging.info("Duplicates removed successfully.")

    # Parse dates in PubMed and Clinical Trials data
    pubmed_df['date'] = pubmed_df['date'].apply(parse_date)
    clinical_trials_df['date'] = clinical_trials_df['date'].apply(parse_date)
    logging.info("Dates parsed successfully.")

    return drugs_df, pubmed_df, clinical_trials_df

def pipeline(data_dir: str, outputs_dir: str) -> None:
    """
    Data pipeline to process drug mentions from PubMed and Clinical Trials data.

    :param data_dir: str: Path to the directory containing the input data files
    :param outputs_dir: str: Path to the directory to save the output files
    """
    ####################################################################################################
    # Step 1: Load data
    ####################################################################################################
    logging.info("Loading data from CSV and JSON files...")
    drugs_df, pubmed_df, clinical_trials_df = load_data(data_dir)
    logging.info("Data loaded successfully.")

    ####################################################################################################
    # Step 2: Data Cleaning and preprocessing
    ####################################################################################################
    logging.info("Cleaning and preprocessing data...")
    drugs_df, pubmed_df, clinical_trials_df = preprocess_data(drugs_df, pubmed_df, clinical_trials_df)
    logging.info("Data cleaned and preprocessed successfully.")

    ####################################################################################################
    # Step 3: Process drug mentions
    ####################################################################################################
    # Get list of drugs
    drugs = drugs_df['drug'].tolist()

    logging.info("Processing drug mentions from PubMed and Clinical Trials data...")
    pubmed_mentions_graph = process_mentions(pubmed_df, drugs_list=drugs, source_name='PubMed')
    logging.info("Processed PubMed mentions successfully.")
    trials_mentions_graph = process_mentions(clinical_trials_df, drugs_list=drugs, source_name='Clinical Trial')
    logging.info("Processed Clinical Trials mentions successfully.")

    # Merge PubMed and Clinical Trials mentions into one final output
    mentions_graph = merge_mention_graphs(pubmed_mentions_graph, trials_mentions_graph)
    logging.info("Drug mentions processed successfully.")

    ####################################################################################################
    # Step 4: Save output
    ####################################################################################################
    with open(outputs_dir / 'drug_mentions_graph.json', 'w', encoding='utf-8') as output_file:
        json.dump(mentions_graph, output_file, indent=4, ensure_ascii=False)

    logging.info("Data pipeline executed successfully. Output saved to 'drug_mentions_graph.json'.")

    ####################################################################################################
    # Step 5: Draw graph with graphviz
    ####################################################################################################
    # This step is optional and can be done if you have graphviz installed
    # and want to visualize the drug mentions graph
    draw_graph(mentions_graph, output_dir=outputs_dir)
    logging.info("Graph generated and saved as 'drug_mentions_graph.png'.")

if __name__ == "__main__":
    pipeline(DATA_DIR, OUTPUTS_DIR)
