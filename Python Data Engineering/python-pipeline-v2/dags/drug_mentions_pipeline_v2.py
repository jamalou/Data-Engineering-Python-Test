"""
This is the DAG file for the mentions graph pipeline.
The pipeline processes PubMed and Clinical Trials data to create a graph of drug mentions.
"""
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd

from custom_operators.scientific_mentions_operators import LoadAndPreprocessScientificDataOperator, ProcessMentionsOperator
from includes.utils import merge_mention_graphs, draw_graph


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

AIRFLOW_HOME = Path(__file__).resolve().parents[1]
DATA_DIR = Path(AIRFLOW_HOME / 'data/raw')
INTERMEDIATE_DIR = Path(AIRFLOW_HOME / 'data/intermediate')
OUTPUTS_DIR = Path(AIRFLOW_HOME / 'data/outputs')

DATA_DIR.mkdir(parents=True, exist_ok=True)
INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# Default arguments for the DAG
default_args = {
    'owner': 'Mohamed JAMEL EDDINE',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'drug_mentions_pipeline_v2',
    default_args=default_args,
    description='A DAG that processes each data file in separate tasks',
    schedule_interval="@once",
    start_date=datetime(2024, 10, 27),
    catchup=False,
) as dag:

    @task
    def load_and_process_drugs():
        """
        Load and process drugs data
        * Capitalize drug names
        * Drop duplicates
        * Save processed data to a new CSV file in the intermediate directory
        """
        logging.info('Loading and processing drugs data...')
        drugs_df = pd.read_csv(DATA_DIR / 'drugs.csv')
        logging.info('Capitalizing drug names...')
        drugs_df['drug'] = drugs_df['drug'].str.capitalize()
        logging.info('Dropping duplicates...')
        drugs_df = drugs_df.drop_duplicates(subset='drug')
        drugs_df.to_csv(INTERMEDIATE_DIR / 'processed_drugs.csv', index=False)
        logging.info('Processed drugs data saved to processed_drugs.csv')

    @task
    def merge_pubmed_and_trials_mentions():
        """ Merge PubMed and Clinical Trials mentions into one graph """
        with (
            open(INTERMEDIATE_DIR / 'pubmed_mentions.json') as f1,
            open(INTERMEDIATE_DIR / 'clinical_trial_mentions.json') as f2,
        ):
            pubmed_mentions_graph = json.load(f1)
            trials_mentions_graph = json.load(f2)
            mentions_graph = merge_mention_graphs(pubmed_mentions_graph, trials_mentions_graph)
            with open(OUTPUTS_DIR / 'merged_mentions_graph.json', 'w') as f_out:
                json.dump(mentions_graph, f_out, indent=4)

    @task(task_id='draw_graph')
    def draw_graph_as_image():
        """ Draw the merged mentions graph as an image """
        with open(OUTPUTS_DIR / 'merged_mentions_graph.json') as f:
            mentions_graph = json.load(f)
            draw_graph(mentions_graph, OUTPUTS_DIR)

    @task
    def clean_up_intermediate_files():
        """ Clean up intermediate files """
        for file in INTERMEDIATE_DIR.iterdir():
            file.unlink()

    # instantiate the tasks
    start_task = DummyOperator(task_id='start_task')

    load_and_process_drugs_op = load_and_process_drugs()

    load_and_process_pubmed_op = LoadAndPreprocessScientificDataOperator(
        task_id='load_and_process_pubmed',
        data_dir=DATA_DIR,
        out_dir=INTERMEDIATE_DIR,
        base_name='pubmed',
        extensions=['.csv', '.json'],
    )

    load_and_process_trials_op = LoadAndPreprocessScientificDataOperator(
        task_id='load_and_process_trials',
        data_dir=DATA_DIR,
        out_dir=INTERMEDIATE_DIR,
        base_name='clinical_trials',
        extensions=['.csv'],
        rename_cols={'scientific_title': 'title'},
    )

    process_pubmed_mentions_op = ProcessMentionsOperator(
        task_id='process_pubmed_mentions',
        drugs_file_path=INTERMEDIATE_DIR / 'processed_drugs.csv',
        scientific_data_path = INTERMEDIATE_DIR/'processed_pubmed.csv',
        source_name='PubMed',
        out_dir=INTERMEDIATE_DIR,
    )
    
    process_trials_mentions_op = ProcessMentionsOperator(
        task_id='process_clinical_trials_mentions',
        drugs_file_path=INTERMEDIATE_DIR/'processed_drugs.csv',
        scientific_data_path = INTERMEDIATE_DIR/'processed_clinical_trials.csv',
        source_name='Clinical Trial',
        out_dir=INTERMEDIATE_DIR,
    )

    merge_mentions_op = merge_pubmed_and_trials_mentions()

    draw_graph_op = draw_graph_as_image()

    clean_up_intermediate_files_op = clean_up_intermediate_files()

    end_task = DummyOperator(task_id='end_task')

    # Set task dependencies
    start_task >> [load_and_process_drugs_op, load_and_process_pubmed_op] >> process_pubmed_mentions_op
    start_task >> [load_and_process_drugs_op, load_and_process_trials_op] >> process_trials_mentions_op
    (
        [process_pubmed_mentions_op, process_trials_mentions_op]
        >> merge_mentions_op
        >> [draw_graph_op, clean_up_intermediate_files_op]
        >> end_task
    )
