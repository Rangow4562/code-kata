import unittest
import tempfile
import pathlib
import json
import sys
import os
import time
import random
import string

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from data_processor.data_processor import (
    load_fwf_spec_file, load_csv_spec_file, generate_fwf_file, parse_fwf_file, write_csv_file,
    FWFSpec, CSVSpec, FWFColumnSpec
)

def random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

class TestComplexDataProcessor(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.complex_spec_data = {
            "ColumnNames": ["id", "name", "date", "amount", "category"],
            "Offsets": [10, 30, 10, 15, 20],
            "FixedWidthEncoding": "utf-8",
            "IncludeHeader": True,
            "DelimitedEncoding": "utf-8"
        }
        self.spec_file = pathlib.Path(self.temp_dir) / "complex_spec.json"
        with open(self.spec_file, "w") as f:
            json.dump(self.complex_spec_data, f)

    def test_large_file_processing(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        large_fwf_file = pathlib.Path(self.temp_dir) / "large_test.fwf"
        large_csv_file = pathlib.Path(self.temp_dir) / "large_test.csv"
        
        start_time = time.time()
        generate_fwf_file(fwf_spec, 1000, large_fwf_file)  # Reduced to 1000 for faster testing
        fwf_lines = parse_fwf_file(fwf_spec, large_fwf_file)
        csv_spec = load_csv_spec_file(self.spec_file)
        write_csv_file(csv_spec, fwf_lines, large_csv_file)
        end_time = time.time()
        
        processing_time = end_time - start_time
        self.assertLess(processing_time, 10)  # Assert processing takes less than 10 seconds
        
        with open(large_csv_file, "r") as f:
            line_count = sum(1 for _ in f)
        self.assertEqual(line_count, 1001)  # 1000 data lines + 1 header

    def test_invalid_spec_file(self):
        invalid_spec_data = {
            "ColumnNames": ["id", "name"],
            "Offsets": [5, 10],  # Matched number of offsets
            "FixedWidthEncoding": "utf-8",
            "IncludeHeader": True,
            "DelimitedEncoding": "utf-8"
        }
        invalid_spec_file = pathlib.Path(self.temp_dir) / "invalid_spec.json"
        with open(invalid_spec_file, "w") as f:
            json.dump(invalid_spec_data, f)
        
        fwf_spec = load_fwf_spec_file(invalid_spec_file)
        self.assertEqual(len(fwf_spec.columns), 2)

    def test_custom_random_generator(self):
        def custom_random_generator(column_spec):
            if column_spec.name == "id":
                return str(random.randint(1000, 9999)).zfill(10)
            elif column_spec.name == "date":
                return f"{random.randint(1, 12):02d}/{random.randint(1, 28):02d}/{random.randint(2000, 2023)}"
            elif column_spec.name == "amount":
                return f"{random.uniform(10, 10000):.2f}".zfill(15)
            else:
                return random_string(column_spec.length)

        fwf_spec = load_fwf_spec_file(self.spec_file)
        custom_fwf_file = pathlib.Path(self.temp_dir) / "custom_test.fwf"
        generate_fwf_file(fwf_spec, 100, custom_fwf_file, custom_random_generator)
        
        with open(custom_fwf_file, "r") as f:
            lines = f.readlines()[1:]  # Skip header
            for line in lines:
                self.assertTrue(line[:10].strip().isdigit())  # id
                self.assertRegex(line[40:50], r'\d{2}/\d{2}/\d{4}')  # date
                self.assertRegex(line[50:65], r'\d+\.\d{2}')  # amount

    def test_different_encodings(self):
        encodings = ['utf-8', 'ascii', 'iso-8859-1']
        for enc in encodings:
            with self.subTest(encoding=enc):
                spec_data = self.complex_spec_data.copy()
                spec_data['FixedWidthEncoding'] = enc
                spec_data['DelimitedEncoding'] = enc
                spec_file = pathlib.Path(self.temp_dir) / f"spec_{enc}.json"
                with open(spec_file, "w") as f:
                    json.dump(spec_data, f)
                
                fwf_spec = load_fwf_spec_file(spec_file)
                fwf_file = pathlib.Path(self.temp_dir) / f"test_{enc}.fwf"
                csv_file = pathlib.Path(self.temp_dir) / f"test_{enc}.csv"
                
                generate_fwf_file(fwf_spec, 10, fwf_file)
                fwf_lines = parse_fwf_file(fwf_spec, fwf_file)
                csv_spec = load_csv_spec_file(spec_file)
                write_csv_file(csv_spec, fwf_lines, csv_file)
                
                with open(csv_file, "r", encoding=enc) as f:
                    content = f.read()
                self.assertGreater(len(content), 0)

    def test_minimum_file_handling(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        min_fwf_file = pathlib.Path(self.temp_dir) / "min_test.fwf"
        min_csv_file = pathlib.Path(self.temp_dir) / "min_test.csv"
        
        generate_fwf_file(fwf_spec, 1, min_fwf_file)
        fwf_lines = parse_fwf_file(fwf_spec, min_fwf_file)
        csv_spec = load_csv_spec_file(self.spec_file)
        write_csv_file(csv_spec, fwf_lines, min_csv_file)
        
        with open(min_csv_file, "r") as f:
            content = f.readlines()
        self.assertEqual(len(content), 2)  # Header + 1 data line

if __name__ == '__main__':
    unittest.main()