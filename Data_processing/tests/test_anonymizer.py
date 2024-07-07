
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import unittest
from anonymizer.anonymizer import Anonymizer
from pathlib import Path
import csv
import os

class TestAnonymizer(unittest.TestCase):

    def setUp(self):
        # Create a sample input file
        self.input_file = Path('test_input.csv')
        self.output_file = Path('test_output.csv')
        with open(self.input_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['first_name', 'last_name', 'address', 'date_of_birth'])
            writer.writeheader()
            writer.writerow({'first_name': 'John', 'last_name': 'Doe', 'address': '123 Main St', 'date_of_birth': '01/01/1990'})
            writer.writerow({'first_name': 'Jane', 'last_name': 'Smith', 'address': '456 Oak St', 'date_of_birth': '02/02/1980'})

    def tearDown(self):
        # Clean up the files after test
        if os.path.exists(self.input_file):
            os.remove(self.input_file)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def test_anonymize_data(self):
        fields_to_anonymize = ['first_name', 'last_name', 'address']
        anonymizer = Anonymizer(self.input_file, self.output_file, fields_to_anonymize)
        anonymizer.anonymize_data()

        with open(self.output_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = list(reader)

        self.assertEqual(len(data), 2)
        self.assertNotEqual(data[0]['first_name'], 'John')
        self.assertNotEqual(data[0]['last_name'], 'Doe')
        self.assertNotEqual(data[0]['address'], '123 Main St')
        self.assertEqual(data[0]['date_of_birth'], '01/01/1990')

if __name__ == '__main__':
    unittest.main()
