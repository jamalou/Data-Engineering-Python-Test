"""
This module contains custom operators for the scientific mentions pipeline.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import re
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


class LoadAndPreprocessScientificDataOperator(BaseOperator):
    """ loads and preprocesses scientific data """
    def __init__(
        self,
        data_dir: Path,
        out_dir: Path,
        base_name: str,
        extensions: List[str] = ['.csv'],
        rename_cols: Dict[str, str] = None,
        *args, **kwargs
    ) -> None:
        """
        Initialize the LoadAndPreprocessScientificDataOperator.

        :param data_dir: Path: Path to the directory containing the input data files
        :param out_dir: Path: Path to the directory to save the processed data
        """
        super().__init__(*args, **kwargs)
        self.data_dir = data_dir
        self.out_dir = out_dir
        self.base_name = base_name
        self.extensions = extensions
        self.rename_cols = rename_cols

    def parse_date(self, date_str: str) -> pd.Timestamp:
        """
        This function parses a date string by trying to match one of the date formats

        :param date_str: str: Date string
        :return: str: Parsed date string
        :raises: ValueError: If the date string does not match any of the date formats
        """
        # possible date formats
        formats = ["%d/%m/%Y", "%Y-%m-%d", "%d %B %Y"]
        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
        # If none of the formats match, raise an error
        raise AirflowException(f"Date format for {date_str} not recognized")

    def execute(self, context):
        """
        Load and preprocess scientific data
        """
        logging.info("%s data has the following extensions: %s", self.base_name, self.extensions)

        data_dfs = []
        for ext in self.extensions:
            file_path = self.data_dir / f'{self.base_name}{ext}'
            if not file_path.exists():
                raise FileNotFoundError(f"File not found at {file_path}")

            logging.info("Loading %s data...", file_path)
            if ext == '.csv':
                data_df = pd.read_csv(file_path, dtype=str)
            elif ext == '.json':
                data_df = pd.read_json(file_path, dtype=str)
            else:
                raise AirflowException(f"Unsupported file extension: {ext}")
            logging.info("%s data loaded successfully.", file_path)

            data_dfs.append(data_df)

        concatenated_df = pd.concat(data_dfs)
        if self.rename_cols:
            concatenated_df = concatenated_df.rename(columns=self.rename_cols)
        concatenated_df['date'] = concatenated_df['date'].apply(self.parse_date)
        concatenated_df.to_csv(self.out_dir / f'processed_{self.base_name}.csv', index=False)

        logging.info(
            "Preprocessed %s data saved successfully to %s/processed_%s.csv.",
            self.base_name, self.out_dir, self.base_name
        )


class ProcessMentionsOperator(BaseOperator):
    """
    Custom operator to process drug mentions in a DataFrame and return a dict of drug mentions with journals and dates.
    """
    @apply_defaults
    def __init__(
        self,
        drugs_file_path: Path,
        scientific_data_path: Path,
        source_name: str,
        out_dir: Path,
        title_col: str = 'title',
        *args, **kwargs
    ) -> None:
        """
        Initialize the ProcessMentionsOperator.

        :param df: pd.DataFrame: DataFrame containing drug mentions
        :param drugs_list: list: List of all drugs
        :param source_name: str: Name of the source
        """
        super().__init__(*args, **kwargs)
        self.drugs_file_path = drugs_file_path
        self.scientific_data_path = scientific_data_path
        self.source_name = source_name
        self.out_dir = out_dir
        self.title_col = title_col

    def find_drugs_in_title(self, title: str, drug_list: List[str]) -> List[str]:
        """
        This function finds drugs mentioned in a title

        :param title: str: Title of the article
        :param drug_list: list: List of all drugs
        :return: list: List of drugs found in the title
        """
        drugs_found = []
        title_lower = title.lower()
        for drug in drug_list:
            if re.search(r'\b' + re.escape(drug.lower()) + r'\b', title_lower):
                drugs_found.append(drug)
        return drugs_found

    def process_mentions(
            self,
            df: pd.DataFrame, # pylint: disable=invalid-name
            drugs_list: list,
            source_name: str,
            graph: dict=None,
            title_col: str='title'
        ) -> Dict[str, Any]:
        """
        This function processes drug mentions in a DataFrame and
        returns a dict of drug mentions with journals and dates.

        :param df: pd.DataFrame: DataFrame containing drug mentions
        :param drugs_list: list: List of all drugs
        :param source_name: str: Name of the source
        :param title_col: str: Name of the column containing the title
        :param graph: dict: Graph to store drug mentions
        :return: dict: Graph of drug mentions with journals and dates
        """
        if graph is None:
            graph = {}
        for _, row in df.iterrows():
            found_drugs = self.find_drugs_in_title(row[title_col], drugs_list)
            for drug in found_drugs:
                # Initialize the drug entry if it doesn't exist
                if drug not in graph:
                    graph[drug] = {}

                # Initialize the journal entry if it doesn't exist under this drug
                if row['journal'] not in graph[drug]:
                    graph[drug][row['journal']] = {"PubMed": [], "Clinical Trial": []}

                # Add mention to the appropriate source with title and date
                mention_entry = {"title": row[title_col], "date": str(row['date'])}
                graph[drug][row['journal']][source_name].append(mention_entry)
        return graph

    def execute(self, context):
        """
        Process drug mentions in the DataFrame and return a dict of drug mentions with journals and dates.
        """
        logging.info("Loading drugs data and %s data...", self.source_name)
        drugs_df = pd.read_csv(self.drugs_file_path)
        trials_df = pd.read_csv(self.scientific_data_path)
        logging.info("Data loaded successfully.")

        drugs = drugs_df['drug'].tolist()
        logging.info("Processing drug mentions from %s data...", self.source_name)
        trials_mentions_graph = self.process_mentions(trials_df, drugs_list=drugs, source_name=self.source_name)
        logging.info("Processed %s mentions successfully.", self.source_name)
        logging.info("Saving mentions graph to %s/%s.json ...", self.out_dir, self.source_name)
        with open(self.out_dir / f'{self.source_name.lower().replace(" ", "_")}_mentions.json', 'w') as f:
            json.dump(trials_mentions_graph, f, indent=4)
        logging.info("Graph saved successfully to %s/trials_mentions.json.", self.out_dir)
