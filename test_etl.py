import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime, date
from etl import clean_phone, parse_date, get_ages_served, extract_title, geocode_address

class TestETLFunctions(unittest.TestCase):

    def test_clean_phone(self):
        self.assertEqual(clean_phone("(123) 456-7890"), "1234567890")
        self.assertEqual(clean_phone("123-456-7890"), "1234567890")
        self.assertEqual(clean_phone("123.456.7890"), "1234567890")
        self.assertEqual(clean_phone("123 456 7890"), "1234567890")
        self.assertEqual(clean_phone("1234567890"), "1234567890")

    def test_parse_date(self):
        self.assertEqual(parse_date("01/15/22"), date(2022, 1, 15))
        self.assertEqual(parse_date("12/31/99"), date(1999, 12, 31))
        self.assertEqual(parse_date("Invalid Date"), None)
        self.assertEqual(parse_date(None), None)
        self.assertEqual(parse_date(datetime(2022, 1, 15)), date(2022, 1, 15))

    def test_get_ages_served(self):
        # Test with pandas Series
        row1 = pd.Series({'Infant': 'Y', 'Toddler': 'Y', 'Preschool': 'N', 'School': 'N'})
        self.assertEqual(get_ages_served(row1), ('Infants, Toddlers', 0, 23))

        # Test with dictionary
        row2 = {'Ages Accepted 1': 'Toddlers (12-23 months)', 'AA2': 'Preschool (24-48 months)'}
        self.assertEqual(get_ages_served(row2), ('Toddlers, Preschool', 12, 59))

        row3 = {'Ages Accepted 1': 'School-age (5 years and up)'}
        self.assertEqual(get_ages_served(row3), ('School-age', 60, None))

        # Test with empty Series
        row4 = pd.Series({})
        self.assertEqual(get_ages_served(row4), ('', None, None))

        # Test with numpy array
        row5 = np.array(['Y', 'N', 'Y', 'N'])
        self.assertEqual(get_ages_served(row5), ('Infants, Preschool', 0, 59))

        # Test with all None values
        row6 = pd.Series({'Infant': None, 'Toddler': None, 'Preschool': None, 'School': None})
        self.assertEqual(get_ages_served(row6), ('', None, None))

    def test_extract_title(self):
        self.assertEqual(extract_title("John Doe - Director"), "Director")
        self.assertEqual(extract_title("Jane Smith, Owner"), "Owner")
        self.assertEqual(extract_title("Primary Caregiver: Alice Johnson"), "Primary Caregiver")
        self.assertEqual(extract_title("Bob Wilson (Other)"), "Other")
        self.assertEqual(extract_title("No Title Here"), None)
        self.assertEqual(extract_title(None), None)

    @patch('etl.requests.get')
    def test_geocode_address(self, mock_get):
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