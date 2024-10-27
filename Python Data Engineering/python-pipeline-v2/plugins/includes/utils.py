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
        dot.render(output_dir/'drug_mentions_graph', format='png', cleanup=True)
    except ImportError:
        logging.error("Graphviz not installed. Skipping graph drawing.")
        return
