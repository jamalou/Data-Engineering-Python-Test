"""Unit tests for the utils module."""
import unittest

import pandas as pd

from src.utils import find_drugs_in_title, parse_date, merge_mention_graphs, process_mentions

class TestParseDate(unittest.TestCase):
    """Test the parse_date function"""
    def test_parse_date_dd_mm_yyyy(self):
        self.assertEqual(parse_date("01/01/2020"), pd.to_datetime("2020-01-01"))
        
    def test_parse_date_yyyy_mm_dd(self):
        self.assertEqual(parse_date("2020-01-01"), pd.to_datetime("2020-01-01"))
        
    def test_parse_date_d_month_year(self):
        self.assertEqual(parse_date("1 January 2020"), pd.to_datetime("2020-01-01"))
        
    def test_parse_date_invalid_format(self):
        self.assertRaises(ValueError, parse_date, "01-01-2020")


class TestFindDrugsInTitle(unittest.TestCase):
    """Test the find_drugs_in_title function"""
    def test_find_drugs_in_title(self):
        self.assertEqual(
            find_drugs_in_title("This is a title with Paracetamol", ["Paracetamol", "Ibuprofen"]),
            ["Paracetamol"]
        )
        
    def test_find_drugs_in_title_no_drugs(self):
        self.assertEqual(find_drugs_in_title("This is a title with no drugs", ["Paracetamol", "Ibuprofen"]), [])
        
    def test_find_drugs_in_title_multiple_drugs(self):
        self.assertEqual(
            find_drugs_in_title("This is a title with Paracetamol and Ibuprofen", ["Paracetamol", "Ibuprofen"]),
            ["Paracetamol", "Ibuprofen"]
        )

    def test_find_drugs_in_title_case_insensitive(self):
        self.assertEqual(
            find_drugs_in_title("This is a title with paracetamol", ["Paracetamol", "Ibuprofen"]),
            ["Paracetamol"]
        )

    def test_find_drugs_in_title_partial_match(self):
        self.assertEqual(
            find_drugs_in_title("This is a title with Paracetamole", ["Paracetamol", "Ibuprofen"]),
            []
        )

class TestMergeMentionGraphs(unittest.TestCase):
    """Test the merge_mention_graphs function"""
    def test_merge_mention_graphs(self):
        graph1 = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [{"title": "Title1", "date": "01/01/2020"}]
                }
            }
        }
        graph2 = {
            "Drug1": {
                "Journal1": {
                    "Clinical Trial": [{"title": "Title2", "date": "01/01/2020"}]
                }
            }
        }
        merged_graph = merge_mention_graphs(graph1, graph2)
        expected_graph = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [{"title": "Title1", "date": "01/01/2020"}],
                    "Clinical Trial": [{"title": "Title2", "date": "01/01/2020"}]
                }
            }
        }
        self.assertEqual(merged_graph, expected_graph)

    def test_merge_mention_graphs_no_overlap(self):
        graph1 = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [{"title": "Title1", "date": "01/01/2020"}]
                }
            }
        }
        graph2 = {
            "Drug2": {
                "Journal2": {
                    "Clinical Trial": [{"title": "Title2", "date": "01/01/2020"}]
                }
            }
        }
        merged_graph = merge_mention_graphs(graph1, graph2)
        expected_graph = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [{"title": "Title1", "date": "01/01/2020"}]
                }
            },
            "Drug2": {
                "Journal2": {
                    "Clinical Trial": [{"title": "Title2", "date": "01/01/2020"}]
                }
            }
        }
        self.assertEqual(merged_graph, expected_graph)

    def test_merge_mention_graphs_empty_graph(self):
        graph1 = {}
        graph2 = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [{"title": "Title1", "date": "01/01/2020"}]
                }
            }
        }
        merged_graph = merge_mention_graphs(graph1, graph2)
        self.assertEqual(merged_graph, graph2)

    def test_merge_mention_graphs_empty_graphs(self):
        graph1 = {}
        graph2 = {}
        merged_graph = merge_mention_graphs(graph1, graph2)
        self.assertEqual(merged_graph, {})

    def test_merge_mention_graphs_empty_second_graph(self):
        graph1 = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [{"title": "Title1", "date": "01/01/2020"}]
                }
            }
        }
        graph2 = {}
        merged_graph = merge_mention_graphs(graph1, graph2)
        self.assertEqual(merged_graph, graph1)

class TestProcessMentions(unittest.TestCase):
    """Test the process_mentions function"""
    def test_process_mentions(self):
        df = pd.DataFrame({
            "title": ["About Drug1", "About Drug1 and Drug2", "About Drug2"],
            "journal": ["Journal1", "Journal1", "Journal2"],
            "date": [pd.Timestamp("2020-01-01"), pd.Timestamp("2021-01-01"), pd.Timestamp("2021-06-25")]
        })
        drugs_list = ["Drug1", "Drug2"]
        source_name = "PubMed"
        graph = process_mentions(df, drugs_list, source_name)
        expected_graph = {
            "Drug1": {
                "Journal1": {
                    "PubMed": [
                        {"title": "About Drug1", "date": "2020-01-01"},
                        {"title": "About Drug1 and Drug2", "date": "2021-01-01"}
                    ],
                    "Clinical Trial": []
                }
            },
            "Drug2": {
                "Journal1": {"PubMed": [{"title": "About Drug1 and Drug2", "date": "2021-01-01"}], "Clinical Trial": []},
                "Journal2": {"PubMed": [{"title": "About Drug2", "date": "2021-06-25"}], "Clinical Trial": []}
            }
        }
        self.assertEqual(graph, expected_graph)

if __name__ == '__main__':
    unittest.main()
