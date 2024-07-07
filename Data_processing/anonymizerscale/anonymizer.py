# anonymizer.py

import csv
import random
import string
from typing import Dict, Iterator
from pathlib import Path
import logging
import multiprocessing as mp
from itertools import islice

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Anonymizer:
    def __init__(self, input_file: Path, output_file: Path, chunk_size: int = 500000):
        self.input_file = input_file
        self.output_file = output_file
        self.chunk_size = chunk_size

    def anonymize_data(self) -> None:
        try:
            with mp.Pool(processes=mp.cpu_count()) as pool:
                self._process_in_parallel(pool)
            logging.info(f"Data anonymized successfully. Output saved to {self.output_file}")
        except Exception as e:
            logging.error(f"Error during anonymization: {str(e)}")
            raise

    def _process_in_parallel(self, pool: mp.Pool) -> None:
        with open(self.input_file, 'r', newline='') as infile, \
             open(self.output_file, 'w', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            while True:
                chunk = list(islice(reader, self.chunk_size))
                if not chunk:
                    break
                results = pool.map(self._anonymize_row, chunk, chunksize=10000)
                writer.writerows(results)

    @staticmethod
    def _anonymize_row(row: Dict[str, str]) -> Dict[str, str]:
        return {
            'first_name': Anonymizer._anonymize_name(row['first_name']),
            'last_name': Anonymizer._anonymize_name(row['last_name']),
            'address': Anonymizer._anonymize_address(row['address']),
            'date_of_birth': row['date_of_birth']
        }

    @staticmethod
    def _anonymize_name(name: str) -> str:
        return f"{name[0]}{''.join(random.choices(string.ascii_lowercase, k=5))}"

    @staticmethod
    def _anonymize_address(address: str) -> str:
        parts = address.split()
        return f"{random.randint(1, 99)} {''.join(random.choices(string.ascii_lowercase, k=5))} {parts[-1]}"

