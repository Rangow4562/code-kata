#!/usr/bin/env python3
import argparse
import pathlib
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from data_processor.data_processor import write_csv_file, parse_fwf_file, load_csv_spec_file, load_fwf_spec_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse user command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generates CSV file based on given Fixed width file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--spec_file", type=pathlib.Path, required=True, help="Fixed width and CSV spec file path"
    )
    parser.add_argument(
        "--fwf_file", type=pathlib.Path, required=True, help="Fixed width data file path"
    )
    parser.add_argument(
        "--csv_file", type=pathlib.Path, required=True, help="Generated CSV file path"
    )
    return parser.parse_args()

def main() -> int:
    args = parse_args()
    try:
        fwf_spec = load_fwf_spec_file(args.spec_file)
        csv_spec = load_csv_spec_file(args.spec_file)
        fwf_lines = parse_fwf_file(fwf_spec, args.fwf_file)
        write_csv_file(csv_spec, fwf_lines, args.csv_file)
    except Exception as e:
        logger.error(f"Failed to generate CSV file: {e}")
        return 1  # Return 1 for failure
    return 0  # Return 0 for success

if __name__ == "__main__":
    sys.exit(main())