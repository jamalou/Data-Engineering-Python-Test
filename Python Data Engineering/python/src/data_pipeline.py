"""
Data pipeline script to process drug mentions from PubMed and Clinical Trials data.
"""
import json
import logging
from pathlib import Path

from src.utils import load_data, parse_date, process_mentions, merge_mention_graphs, draw_graph

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
OUTPUTS_DIR = Path(__file__).resolve().parents[1] / 'outputs'


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
    # Capitalize drug names
    drugs_df['drug'] = drugs_df['drug'].str.capitalize()
    logging.info("Drug names capitalized successfully.")

    # TODO: remove duplicates from pubmed_df and clinical_trials_df
    pubmed_df = pubmed_df.drop_duplicates(subset='title')
    clinical_trials_df = clinical_trials_df.drop_duplicates(subset='scientific_title')
    logging.info("Duplicates removed successfully.")

    # Parse dates in PubMed and Clinical Trials data
    pubmed_df['date'] = pubmed_df['date'].apply(parse_date)
    clinical_trials_df['date'] = clinical_trials_df['date'].apply(parse_date)
    logging.info("Dates parsed successfully.")

    ####################################################################################################
    # Step 3: Process drug mentions
    ####################################################################################################
    # Get list of drugs
    drugs = drugs_df['drug'].tolist()

    logging.info("Processing drug mentions from PubMed and Clinical Trials data...")
    pubmed_mentions_graph = process_mentions(pubmed_df, drugs, 'PubMed')
    logging.info("Processed PubMed mentions successfully.")
    trials_mentions_graph = process_mentions(
        clinical_trials_df, drugs, 'Clinical Trial', title_col='scientific_title'
    )
    logging.info("Processed Clinical Trials mentions successfully.")

    # Merge PubMed and Clinical Trials mentions into one final output
    mentions_graph = merge_mention_graphs(pubmed_mentions_graph, trials_mentions_graph)                     
    logging.info("Drug mentions processed successfully.")

    ####################################################################################################
    # Step 4: Save output
    ####################################################################################################
    with open(outputs_dir / 'drug_mentions_graph_with_sources.json', 'w', encoding='utf-8') as f:
        json.dump(mentions_graph, f, indent=4, ensure_ascii=False)

    logging.info("Data pipeline executed successfully. Output saved to 'drug_mentions_graph_with_sources.json'.")

    ####################################################################################################
    # Step 5: Draw graph with graphviz
    ####################################################################################################
    # This step is optional and can be done if you have graphviz installed
    # and want to visualize the drug mentions graph
    draw_graph(mentions_graph, output_dir=outputs_dir)
    logging.info("Graph generated and saved as 'drug_mentions_graph.png'.")

if __name__ == "__main__":
    pipeline(DATA_DIR, OUTPUTS_DIR)
