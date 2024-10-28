"""
This is the DAG file for the mentions graph pipeline.
The pipeline processes PubMed and Clinical Trials data to create a graph of drug mentions using BigQuery.
"""
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# Define constants
PROJECT_ID = Variable.get('project_id')
RAW_DATASET = Variable.get('raw_dataset')
STAGING_DATASET = Variable.get('staging_dataset')
DATAWAREHOUSE_DATASET = Variable.get('datawarehouse_dataset')
GCS_BUCKET = Variable.get('composer_bucket')

RAW_DATA_PATH = Path('data/raw/')
OUTPUT_DATA_PATH = Path('data/output/')

default_args = {
    'owner': 'Mohamed JAMEL EDDINE',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'drug_mentions_pipeline_v3',
    default_args=default_args,
    description='Pipeline to load and process data in BigQuery',
    schedule_interval="@once",
    start_date=datetime(2024, 10, 27),
    catchup=False,
    template_searchpath='/home/airflow/gcs/dags/queries',
) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    load_drugs_to_bq = GCSToBigQueryOperator(
        task_id='load_drugs_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[(RAW_DATA_PATH/'drugs*.csv').as_posix()],
        destination_project_dataset_table='{{ var.value.project_id }}.{{ var.value.raw_dataset }}.drugs',
        schema_fields=[
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'drug', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    load_json_pubmed_to_bq = GCSToBigQueryOperator(
        task_id='load_json_pubmed_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[(RAW_DATA_PATH/'pubmed*.json').as_posix()],
        destination_project_dataset_table='{{ var.value.project_id }}.{{ var.value.raw_dataset }}.pubmed_json',
        source_format='NEWLINE_DELIMITED_JSON',
        schema_fields=[
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'journal', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',

    )

    load_csv_pubmed_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_pubmed_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[(RAW_DATA_PATH/'pubmed*.csv').as_posix()],
        destination_project_dataset_table='{{ var.value.project_id }}.{{ var.value.raw_dataset }}.pubmed_csv',
        source_format='CSV',
        schema_fields=[
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'journal', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    load_trials_to_bq = GCSToBigQueryOperator(
        task_id='load_clinical_trials_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[(RAW_DATA_PATH/'clinical_trials*.csv').as_posix()],
        destination_project_dataset_table='{{ var.value.project_id }}.{{ var.value.raw_dataset }}.clinical_trials',
        source_format='CSV',
        schema_fields=[
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'journal', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    preprocess_pubmed_tables = BigQueryInsertJobOperator(
        task_id='preprocess_pubmed_tables',
        configuration={
            "query": {
                "query": "preprocess_pubmed.sql",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ var.value.project_id }}",
                    "datasetId": "{{ var.value.staging_dataset }}",
                    "tableId": "pubmed",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            },
        }
    )

    preprocess_trials_table = BigQueryInsertJobOperator(
        task_id='preprocess_trials_table',
        configuration={
            "query": {
                "query": "preprocess_trials.sql",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ var.value.project_id }}",
                    "datasetId": "{{ var.value.staging_dataset }}",
                    "tableId": "clinical_trials",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            },
        }
    )

    process_mentions_in_bq = BigQueryInsertJobOperator(
        task_id='generate_mentions_graph',
        configuration={
            "query": {
                "query": "generate_mentions_graph.sql",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ var.value.project_id }}",
                    "datasetId": "{{ var.value.datawarehouse_dataset }}",
                    "tableId": "mentions_graph",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            },
            
        }
    )

    export_processed_data_to_gcs = BigQueryToGCSOperator(
        task_id='export_processed_data_to_gcs',
        source_project_dataset_table='{{ var.value.project_id }}.{{ var.value.datawarehouse_dataset }}.mentions_graph',
        destination_cloud_storage_uris=[f'gs://{GCS_BUCKET}/{OUTPUT_DATA_PATH}/mentions_graph.json'],
        export_format='NEWLINE_DELIMITED_JSON',
    )

    end_pipeline = DummyOperator(task_id='end_pipeline')

    # Set task dependencies
    start_pipeline >> [load_drugs_to_bq, load_json_pubmed_to_bq, load_csv_pubmed_to_bq, load_trials_to_bq]
    [load_json_pubmed_to_bq, load_csv_pubmed_to_bq] >> preprocess_pubmed_tables
    load_trials_to_bq >> preprocess_trials_table >> process_mentions_in_bq
    [load_drugs_to_bq, preprocess_pubmed_tables] >> process_mentions_in_bq
    process_mentions_in_bq >> export_processed_data_to_gcs >> end_pipeline
