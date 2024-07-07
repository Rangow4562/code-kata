import unittest
import tempfile
import pathlib
import json
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from data_processor.data_processor import (
    load_fwf_spec_file, load_csv_spec_file, generate_fwf_file, parse_fwf_file, write_csv_file
)

class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.spec_data = {
            "ColumnNames": ["f1", "f2", "f3"],
            "Offsets": [5, 7, 3],
            "FixedWidthEncoding": "utf-8",
            "IncludeHeader": True,
            "DelimitedEncoding": "utf-8"
        }
        self.temp_dir = tempfile.mkdtemp()
        self.spec_file = pathlib.Path(self.temp_dir) / "random_spec.json"
        with open(self.spec_file, "w") as f:
            json.dump(self.spec_data, f)

    def test_load_fwf_spec_file(self):
        spec = load_fwf_spec_file(self.spec_file)
        self.assertEqual(len(spec.columns), 3)
        self.assertEqual(spec.columns[0].name, "f1")
        self.assertEqual(spec.columns[0].length, 5)

    def test_load_csv_spec_file(self):
        spec = load_csv_spec_file(self.spec_file)
        self.assertEqual(spec.column_names, ["f1", "f2", "f3"])
        self.assertTrue(spec.header)

    def test_generate_and_parse_fwf_file(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        fwf_file = pathlib.Path(self.temp_dir) / "test.fwf"
        generate_fwf_file(fwf_spec, 10, fwf_file)
        
        lines = list(parse_fwf_file(fwf_spec, fwf_file))
        self.assertEqual(len(lines), 10)
        self.assertEqual(len(list(lines[0])), 3)  # Convert the generator to a list

    def test_generate_fwf_and_write_csv(self):
        fwf_spec = load_fwf_spec_file(self.spec_file)
        csv_spec = load_csv_spec_file(self.spec_file)
        
        fwf_file = pathlib.Path(self.temp_dir) / "test.fwf"
        csv_file = pathlib.Path(self.temp_dir) / "test.csv"
        
        generate_fwf_file(fwf_spec, 10, fwf_file)
        fwf_lines = parse_fwf_file(fwf_spec, fwf_file)
        write_csv_file(csv_spec, fwf_lines, csv_file)
        
        with open(csv_file, "r") as f:
            csv_content = f.read()
        
        self.assertIn("f1,f2,f3", csv_content)  # Check header
        self.assertEqual(len(csv_content.splitlines()), 11)  # 10 data lines + 1 header

if __name__ == '__main__':
    unittest.main()