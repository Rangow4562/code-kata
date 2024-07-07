#!/usr/bin/env python3
import argparse
import pathlib
import sys
import os
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from data_processor.data_processor import generate_fwf_file, load_fwf_spec_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse user command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generates fixed-width data file based on given spec file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--spec_file", type=pathlib.Path, required=True, help="Fixed width spec file path"
    )
    parser.add_argument(
        "--fwf_file", type=pathlib.Path, required=True, help="Fixed width data file path"
    )
    parser.add_argument(
        "-n", type=int, required=True, help="Number of lines to generate"
    )
    return parser.parse_args()

def main() -> int:
    args = parse_args()
    try:
        spec = load_fwf_spec_file(args.spec_file)
        generate_fwf_file(spec, args.n, args.fwf_file)
    except Exception as e:
        logger.error(f"Failed to generate FWF file: {e}")
        return 1  # Return 1 for failure
    return 0  # Return 0 for success

if __name__ == "__main__":
    sys.exit(main())