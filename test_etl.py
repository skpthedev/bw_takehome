"""
Unit tests for ETL functions in the etl module.

This module contains test cases for various ETL functions used in data processing
and cleaning operations.
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, date

import pandas as pd
import numpy as np

from etl import clean_phone, parse_date, get_ages_served, extract_title, geocode_address


class TestETLFunctions(unittest.TestCase):
    """Test cases for ETL functions."""

    def test_clean_phone(self):
        """Test the clean_phone function with various input formats."""
        self.assertEqual(clean_phone("(123) 456-7890"), "1234567890")
        self.assertEqual(clean_phone("123-456-7890"), "1234567890")
        self.assertEqual(clean_phone("123.456.7890"), "1234567890")
        self.assertEqual(clean_phone("123 456 7890"), "1234567890")
        self.assertEqual(clean_phone("1234567890"), "1234567890")

    def test_parse_date(self):
        """Test the parse_date function with various input formats."""
        self.assertEqual(parse_date("01/15/22"), date(2022, 1, 15))
        self.assertEqual(parse_date("12/31/99"), date(1999, 12, 31))
        self.assertIsNone(parse_date("Invalid Date"))
        self.assertIsNone(parse_date(None))
        self.assertEqual(parse_date(datetime(2022, 1, 15)), date(2022, 1, 15))

    def test_get_ages_served(self):
        """Test the get_ages_served function with various input formats."""
        # Test with pandas Series
        row_series = pd.Series({'Infant': 'Y', 'Toddler': 'Y', 'Preschool': 'N', 'School': 'N'})
        self.assertEqual(get_ages_served(row_series), ('Infants, Toddlers', 0, 23))

        # Test with dictionary
        row_dict = {'Ages Accepted 1': 'Toddlers (12-23 months)', 'AA2': 'Preschool (24-48 months)'}
        self.assertEqual(get_ages_served(row_dict), ('Toddlers, Preschool', 12, 59))

        row_school_age = {'Ages Accepted 1': 'School-age (5 years and up)'}
        self.assertEqual(get_ages_served(row_school_age), ('School-age', 60, None))

        # Test with empty Series
        empty_series = pd.Series({})
        self.assertEqual(get_ages_served(empty_series), ('', None, None))

        # Test with numpy array
        np_array = np.array(['Y', 'N', 'Y', 'N'])
        self.assertEqual(get_ages_served(np_array), ('Infants, Preschool', 0, 59))

        # Test with all None values
        none_series = pd.Series({'Infant': None, 'Toddler': None, 'Preschool': None, 'School': None})
        self.assertEqual(get_ages_served(none_series), ('', None, None))

    def test_extract_title(self):
        """Test the extract_title function with various input formats."""
        self.assertEqual(extract_title("John Doe - Director"), "Director")
        self.assertEqual(extract_title("Jane Smith, Owner"), "Owner")
        self.assertEqual(extract_title("Primary Caregiver: Alice Johnson"), "Primary Caregiver")
        self.assertEqual(extract_title("Bob Wilson (Other)"), "Other")
        self.assertIsNone(extract_title("No Title Here"))
        self.assertIsNone(extract_title(None))

    @patch('etl.requests.get')
    def test_geocode_address(self, mock_get):
        """Test the geocode_address function with mocked API response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'results': [{
                'address': {
                    'countrySubdivision': 'Texas',
                    'municipality': 'Austin',
                    'postalCode': '78701'
                }
            }]
        }
        mock_get.return_value = mock_response

        self.assertEqual(
            geocode_address("123 Main St, Austin, TX 78701"),
            ('Texas', 'Austin', '78701')
        )

        # Test for failed API call
        mock_response.status_code = 404
        self.assertEqual(
            geocode_address("Invalid Address"),
            (None, None, None)
        )


if __name__ == '__main__':
    unittest.main()
