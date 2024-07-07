
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import unittest
from anonymizer.generator import MockDataGenerator
from pathlib import Path
import csv
import os

class TestMockDataGenerator(unittest.TestCase):

    def setUp(self):
        self.output_file = Path('test_mock_data.csv')

    def tearDown(self):
        # Clean up the file after test
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def test_generate_mock_data(self):
        num_rows = 10
        generator = MockDataGenerator(num_rows, self.output_file)
        generator.generate_mock_data()

        with open(self.output_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = list(reader)

        self.assertEqual(len(data), num_rows)
        for row in data:
            self.assertIn('first_name', row)
            self.assertIn('last_name', row)
            self.assertIn('address', row)
            self.assertIn('date_of_birth', row)
            self.assertTrue(row['first_name'])
            self.assertTrue(row['last_name'])
            self.assertTrue(row['address'])
            self.assertTrue(row['date_of_birth'])

if __name__ == '__main__':
    unittest.main()
