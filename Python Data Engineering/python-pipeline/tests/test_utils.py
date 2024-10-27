"""Unit tests for the utils module."""
import unittest

import pandas as pd

from src.utils import find_drugs_in_title, parse_date, merge_mention_graphs, process_mentions

class TestParseDate(unittest.TestCase):
    """Test the parse_date function"""
    def test_parse_date_dd_mm_yyyy(self):
        """Test parsing a date in the format dd/mm/yyyy"""
        self.assertEqual(parse_date("01/01/2020"), pd.to_datetime("2020-01-01"))

    def test_parse_date_yyyy_mm_dd(self):
        """Test parsing a date in the format yyyy-mm-dd"""
        self.assertEqual(parse_date("2020-01-01"), pd.to_datetime("2020-01-01"))

    def test_parse_date_d_month_year(self):
        """Test parsing a date in the format d Month year"""
        self.assertEqual(parse_date("1 January 2020"), pd.to_datetime("2020-01-01"))

    def test_parse_date_invalid_format(self):
        """Test that an invalid date format raises a ValueError"""
        self.assertRaises(ValueError, parse_date, "01-01-2020")


class TestFindDrugsInTitle(unittest.TestCase):
    """Test the find_drugs_in_title function"""
    def test_find_drugs_in_title(self):
        """Test that a drug is returned"""
        self.assertEqual(
            find_drugs_in_title("This is a title with Paracetamol", ["Paracetamol", "Ibuprofen"]),
            ["Paracetamol"]
        )

    def test_find_drugs_in_title_no_drugs(self):
        """Test that no drugs are returned"""
        self.assertEqual(find_drugs_in_title("This is a title with no drugs", ["Paracetamol", "Ibuprofen"]), [])

    def test_find_drugs_in_title_multiple_drugs(self):
        """Test that multiple drugs are returned"""
        self.assertEqual(
            find_drugs_in_title("This is a title with Paracetamol and Ibuprofen", ["Paracetamol", "Ibuprofen"]),
            ["Paracetamol", "Ibuprofen"]
        )

    def test_find_drugs_in_title_case_insensitive(self):
        """Test that the search is case insensitive"""
        self.assertEqual(
            find_drugs_in_title("This is a title with paracetamol", ["Paracetamol", "Ibuprofen"]),
            ["Paracetamol"]
        )

    def test_find_drugs_in_title_partial_match(self):
        """Test that a partial match is not returned"""
        self.assertEqual(
            find_drugs_in_title("This is a title with Paracetamole", ["Paracetamol", "Ibuprofen"]),
            []
        )

class TestMergeMentionGraphs(unittest.TestCase):
    """Test the merge_mention_graphs function"""
    def test_merge_mention_graphs(self):
        """Test merging two mention graphs"""
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
        """Test merging with no overlap in the keys"""
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
        """Test merging with an empty first graph"""
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
        """Test merging with empty graphs"""
        graph1 = {}
        graph2 = {}
        merged_graph = merge_mention_graphs(graph1, graph2)
        self.assertEqual(merged_graph, {})

    def test_merge_mention_graphs_empty_second_graph(self):
        """Test merging with an empty second graph"""
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
        """Test the process_mentions function"""
        df = pd.DataFrame({ # pylint: disable=invalid-name
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
                "Journal1": {
                    "PubMed": [{"title": "About Drug1 and Drug2", "date": "2021-01-01"}],
                    "Clinical Trial": []
                },
                "Journal2": {"PubMed": [{"title": "About Drug2", "date": "2021-06-25"}], "Clinical Trial": []}
            }
        }
        self.assertEqual(graph, expected_graph)

if __name__ == '__main__':
    unittest.main()
