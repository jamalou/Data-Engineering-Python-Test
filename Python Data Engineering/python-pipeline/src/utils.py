"""
This module contains utility functions
"""
from __future__ import annotations
import logging
from pathlib import Path
import re
from typing import Any, Dict, List

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


def find_drugs_in_title(title: str, drug_list: List[str]) -> List[str]:
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

def parse_date(date_str: str) -> pd.Timestamp:
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
    raise ValueError(f"Date format for {date_str} not recognized")

def process_mentions(
        df: pd.DataFrame, # pylint: disable=invalid-name
        drugs_list: list,
        source_name: str,
        graph: dict=None,
    ) -> Dict[str, Any]:
    """
    This function processes drug mentions in a DataFrame and returns a dict of drug mentions with journals and dates.

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
        found_drugs = find_drugs_in_title(row['title'], drugs_list)
        for drug in found_drugs:
            # Initialize the drug entry if it doesn't exist
            if drug not in graph:
                graph[drug] = {}

            # Initialize the journal entry if it doesn't exist under this drug
            if row['journal'] not in graph[drug]:
                graph[drug][row['journal']] = {"PubMed": [], "Clinical Trial": []}

            # Add mention to the appropriate source with title and date
            mention_entry = {"title": row['title'], "date": str(row['date'].date())}
            graph[drug][row['journal']][source_name].append(mention_entry)
    return graph

def merge_mention_graphs(
        graph_1: Dict[str, Any],
        graph_2: Dict[str, Any],
    ) -> List[Dict[str, List[Dict[str, str]]]]:
    """
    This function merges two drug mention graphs into one.

    :param graph_1: dict: First drug mention graph
    :param graph_2: dict: Second drug mention graph
    :return: dict: Merged drug mention graph
    """
    mentions_graph = graph_1
    for drug, drug_mentions in graph_2.items():
        if drug not in mentions_graph:
            mentions_graph[drug] = drug_mentions
            continue

        for journal, sources in drug_mentions.items():
            if journal not in mentions_graph[drug]:
                mentions_graph[drug][journal] = sources
                continue
            for source, mentions in sources.items():
                if source not in mentions_graph[drug][journal]:
                    mentions_graph[drug][journal][source] = mentions
                else:
                    mentions_graph[drug][journal][source].extend(mentions)
    return mentions_graph

def draw_graph(mentions_graph: Dict[str, Any], output_dir: Path) -> None:
    """
    This function draws a graph of drug mentions with journals and dates using Graphviz.

    :param mentions_graph: dict: Graph of drug mentions with journals and dates
    """
    try:
        from graphviz import Digraph # pylint: disable=import-outside-toplevel
        dot = Digraph(comment='Drug Mentions in Journals')

        for drug, journals in mentions_graph.items():
            # Add drug node
            dot.node(drug, drug, shape='ellipse', color='lightblue', style='filled')

            for journal, sources in journals.items():
                # Add journal node
                dot.node(journal, journal, shape='box', color='lightyellow', style='filled')

                # Add edges for each source with date and source as label
                for source, mentions in sources.items():
                    for mention in mentions:
                        edge_label = f"{source} ({mention['date']})"
                        dot.edge(drug, journal, label=edge_label)

        # Render and visualize the graph
        dot.render(output_dir/'drug_mentions_graph', format='png', cleanup=True)  # Outputs a PNG file
    except ImportError:
        logging.error("Graphviz not installed. Skipping graph drawing.")
        return
