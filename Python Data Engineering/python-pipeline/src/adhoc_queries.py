"""
Ad-hoc queries to answer questions about the data
"""
from __future__ import annotations
from collections import defaultdict
import json
from pathlib import Path
from typing import Tuple


def get_journal_with_most_unique_drugs(mentions_graph: Path|dict) -> Tuple[str, int]:
    """
    Find the journal that mentions the most unique drugs

    :param mentions_graph_path: Path to the drug mentions graph
    :return: Tuple: Journal with most unique drugs and the number of unique drugs
    """
    if isinstance(mentions_graph, Path):
        with open(mentions_graph, 'r', encoding="utf-8") as f: # pylint: disable=invalid-name
            mentions_graph = json.load(f)

    journal_drugs = defaultdict(set)

    # loop through each drug and journal in the mentions graph
    for drug, journals in mentions_graph.items():
        for journal in journals.keys():
            journal_drugs[journal].add(drug.lower())

    max_journal = max(journal_drugs, key=lambda journal: len(journal_drugs[journal]))

    return max_journal, len(journal_drugs[max_journal])

def get_related_drugs_in_pubmed_only(mentions_graph: Path|dict, target_drug: str) -> set[str]:
    """
    Find drugs mentioned in the same journals as `target_drug` for PubMed only

    :param mentions_graph_path: Path|dict: Path to the drug mentions graph or the mentions graph dictionary
    :param target_drug: str: The target drug to find related drugs for
    :return: The set of related drugs mentioned in the same journals as `target_drug` for PubMed only
    """
    def refereced_in_pubmed_only(target_drug, journal):
        """"""
        if journal not in mentions_graph.get(target_drug, {}):
            return False
        return (
            mentions_graph[target_drug][journal].get('PubMed', [])
            and not mentions_graph[target_drug][journal].get('Clinical Trial', [])
        )

    if isinstance(mentions_graph, Path):
        with open(mentions_graph, 'r', encoding="utf-8") as f: # pylint: disable=invalid-name
            mentions_graph = json.load(f)

    pubmed_journals = set()

    # Find journals where the target drug is mentioned in PubMed only
    for journal in mentions_graph.get(target_drug, {}):
        if refereced_in_pubmed_only(target_drug, journal):
            pubmed_journals.add(journal)

    related_drugs = set()

    # Find drugs mentioned in the same journals as the target drug for PubMed only
    for drug in mentions_graph:
        if drug == target_drug:
            continue

        for journal in pubmed_journals:
            if refereced_in_pubmed_only(drug, journal):
                related_drugs.add(drug)
    return related_drugs
