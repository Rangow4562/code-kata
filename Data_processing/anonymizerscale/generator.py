# generator.py

import csv
from typing import Dict, Iterator
from pathlib import Path
import random
from datetime import datetime, timedelta
import logging
import multiprocessing as mp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MockDataGenerator:
    FIRST_NAMES = ['Olivia', 'Liam', 'Emma', 'Noah', 'Ava', 'Oliver', 'Sophia', 'Elijah', 'Isabella', 
                'James', 'Mia', 'Benjamin', 'Charlotte', 'Lucas', 'Amelia', 'Henry', 'Harper', 
                'Alexander', 'Evelyn', 'Sebastian', 'Ella', 'Jack']
    LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 
                'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 
                'Moore', 'Jackson', 'Martin', 'Lee']
    ADDRESSES = ['Pine St', 'Maple Ave', 'Cedar Lane', 'Oak Blvd', 'Birch Rd', 'Spruce Way', 
                'Fir Dr', 'Willow St', 'Elm Ave', 'Ash Ln', 'Cherry Blvd', 'Poplar Rd', 
                'Dogwood Way', 'Hickory Dr']


    def __init__(self, num_rows: int, output_file: Path, chunk_size: int = 500000):
        self.num_rows = num_rows
        self.output_file = output_file
        self.chunk_size = chunk_size

    def generate_mock_data(self) -> None:
        try:
            self._write_data()
            logging.info(f"Mock data generated successfully. Output saved to {self.output_file}")
        except Exception as e:
            logging.error(f"Error during mock data generation: {str(e)}")
            raise

    def _write_data(self) -> None:
        with open(self.output_file, 'w', newline='') as csvfile:
            fieldnames = ['first_name', 'last_name', 'address', 'date_of_birth']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            with mp.Pool(processes=mp.cpu_count()) as pool:
                for chunk in self._generate_data_chunks():
                    results = pool.map(self._generate_row, chunk, chunksize=10000)
                    writer.writerows(results)

    def _generate_data_chunks(self) -> Iterator[range]:
        for i in range(0, self.num_rows, self.chunk_size):
            yield range(min(self.chunk_size, self.num_rows - i))

    def _generate_row(self, _: int) -> Dict[str, str]:
        return {
            'first_name': random.choice(self.FIRST_NAMES),
            'last_name': random.choice(self.LAST_NAMES),
            'address': self._generate_address(),
            'date_of_birth': self._generate_date()
        }

    def _generate_address(self) -> str:
        return f"{random.randint(1, 999)} {random.choice(self.ADDRESSES)}"

    @staticmethod
    def _generate_date() -> str:
        start_date = datetime(1950, 1, 1)
        end_date = datetime(2005, 12, 31)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + timedelta(days=random_number_of_days)
        return random_date.strftime("%d/%m/%Y")

