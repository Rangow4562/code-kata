import unittest
import tempfile
import pathlib
import json
import sys
import os
import csv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from data_processor.data_processor import (
    load_fwf_spec_file, load_csv_spec_file, generate_fwf_file, parse_fwf_file, write_csv_file,
    FWFSpec, CSVSpec, FWFColumnSpec
)

class TestAdvancedDataProcessor(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.spec_data = {
            "ColumnNames": ["id", "name", "age", "city"],
            "Offsets": [5, 20, 3, 15],
            "FixedWidthEncoding": "utf-8",
            "IncludeHeader": True,
            "DelimitedEncoding": "utf-8"
        }
        self.spec_file = pathlib.Path(self.temp_dir) / "advanced_spec.json"
        with open(self.spec_file, "w") as f:
            json.dump(self.spec_data, f)

    def test_fwf_spec_creation(self):
        spec = load_fwf_spec_file(self.spec_file)
        self.assertIsInstance(spec, FWFSpec)
        self.assertEqual(len(spec.columns), 4)
        self.assertEqual(spec.columns[1].name, "name")
        self.assertEqual(spec.columns[1].length, 20)

    def test_csv_spec_creation(self):
        spec = load_csv_spec_file(self.spec_file)
        self.assertIsInstance(spec, CSVSpec)
        self.assertEqual(spec.column_names, ["id", "name", "age", "city"])
        self.assertEqual(spec.encoding, "utf-8")

    def test_generate_fwf_file_with_header(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        fwf_file = pathlib.Path(self.temp_dir) / "test_with_header.fwf"
        generate_fwf_file(fwf_spec, 5, fwf_file)
        
        with open(fwf_file, "r") as f:
            lines = f.readlines()
        
        self.assertEqual(len(lines), 6)  # 5 data lines + 1 header
        self.assertTrue(lines[0].startswith("id   name"))

    def test_generate_fwf_file_without_header(self):
        spec_data_no_header = self.spec_data.copy()
        spec_data_no_header["IncludeHeader"] = False
        spec_file_no_header = pathlib.Path(self.temp_dir) / "spec_no_header.json"
        with open(spec_file_no_header, "w") as f:
            json.dump(spec_data_no_header, f)
        
        fwf_spec = load_fwf_spec_file(spec_file_no_header)
        fwf_file = pathlib.Path(self.temp_dir) / "test_without_header.fwf"
        generate_fwf_file(fwf_spec, 5, fwf_file)
        
        with open(fwf_file, "r") as f:
            lines = f.readlines()
        
        self.assertEqual(len(lines), 5)  # 5 data lines, no header
        self.assertFalse(lines[0].startswith("id   name"))

    def test_parse_fwf_file_content(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        fwf_file = pathlib.Path(self.temp_dir) / "test_parse.fwf"
        generate_fwf_file(fwf_spec, 5, fwf_file)
        
        parsed_lines = list(parse_fwf_file(fwf_spec, fwf_file))
        self.assertEqual(len(parsed_lines), 5)
        
        for line in parsed_lines:
            line_list = list(line)
            self.assertEqual(len(line_list), 4)
            self.assertTrue(all(isinstance(item, str) for item in line_list))

    def test_write_csv_file_content(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        csv_spec = load_csv_spec_file(self.spec_file)
        
        fwf_file = pathlib.Path(self.temp_dir) / "test_write.fwf"
        csv_file = pathlib.Path(self.temp_dir) / "test_write.csv"
        
        generate_fwf_file(fwf_spec, 5, fwf_file)
        fwf_lines = parse_fwf_file(fwf_spec, fwf_file)
        write_csv_file(csv_spec, fwf_lines, csv_file)
        
        with open(csv_file, "r", newline='') as f:
            csv_reader = csv.reader(f)
            csv_lines = list(csv_reader)
        
        self.assertEqual(len(csv_lines), 6)  # 5 data lines + 1 header
        self.assertEqual(csv_lines[0], ["id", "name", "age", "city"])
        self.assertEqual(len(csv_lines[1]), 4)

if __name__ == '__main__':
    unittest.main()

