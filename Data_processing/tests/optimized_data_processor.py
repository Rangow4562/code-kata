import csv
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import random
import string
from typing import List, Tuple
from pathlib import Path
import logging
import multiprocessing as mp
import time
from array import array
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared data structures
FIRST_NAMES = ['Olivia', 'Liam', 'Emma', 'Noah', 'Ava', 'Oliver', 'Sophia', 'Elijah', 'Isabella', 
                'James', 'Mia', 'Benjamin', 'Charlotte', 'Lucas', 'Amelia', 'Henry', 'Harper', 
                'Alexander', 'Evelyn', 'Sebastian', 'Ella', 'Jack']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 
                'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 
                'Moore', 'Jackson', 'Martin', 'Lee']
ADDRESSES = ['Pine St', 'Maple Ave', 'Cedar Lane', 'Oak Blvd', 'Birch Rd', 'Spruce Way', 
                'Fir Dr', 'Willow St', 'Elm Ave', 'Ash Ln', 'Cherry Blvd', 'Poplar Rd', 
                'Dogwood Way', 'Hickory Dr']

# Precompute random choices
RANDOM_CHARS = array('u', string.ascii_lowercase)
RANDOM_STREETS = [f"{random.randint(1, 999)} {addr}" for addr in ADDRESSES]

# Precompute dates
START_DATE = datetime(1950, 1, 1)
DATE_RANGE = (datetime(2005, 12, 31) - START_DATE).days

def generate_date():
    return (START_DATE + timedelta(days=random.randint(0, DATE_RANGE))).strftime('%d/%m/%Y')

def generate_row(_: int) -> Tuple[str, str, str, str]:
    return (
        random.choice(FIRST_NAMES),
        random.choice(LAST_NAMES),
        random.choice(RANDOM_STREETS),
        generate_date()
    )

def anonymize_row(row: Tuple[str, str, str, str]) -> Tuple[str, str, str, str]:
    return (
        f"{row[0][0]}{''.join(random.choices(RANDOM_CHARS, k=5))}",
        f"{row[1][0]}{''.join(random.choices(RANDOM_CHARS, k=5))}",
        f"{random.randint(1, 99)} {''.join(random.choices(RANDOM_CHARS, k=5))} {row[2].split()[-1]}",
        row[3]
    )

def process_chunk(chunk: List[Tuple[str, str, str, str]], anonymize: bool) -> List[Tuple[str, str, str, str]]:
    return [anonymize_row(row) if anonymize else row for row in chunk]

def write_chunks(file_path: Path, chunks: List[List[Tuple[str, str, str, str]]]):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])
        for chunk in chunks:
            writer.writerows(chunk)

def process_data(num_rows: int, output_file: Path, anonymize: bool = False, chunk_size: int = 1000000):
    with mp.Pool(processes=mp.cpu_count()) as pool:
        chunks = []
        for i in range(0, num_rows, chunk_size):
            chunk = pool.map(generate_row, range(min(chunk_size, num_rows - i)), chunksize=10000)
            if anonymize:
                chunk = pool.apply(process_chunk, args=(chunk, anonymize))
            chunks.append(chunk)
        
        write_chunks(output_file, chunks)

def main():
    try:
        output_dir = Path('./output')
        output_dir.mkdir(exist_ok=True)

        mock_data_file = output_dir / 'people.csv'
        anonymized_file = output_dir / 'anonymized_people.csv'

        num_rows = 20_000_000  # Adjust this number to achieve desired file size

        # Generate mock data
        start_time = time.time()
        process_data(num_rows, mock_data_file)
        gen_time = time.time() - start_time
        logging.info(f"Data generation completed in {gen_time:.2f} seconds")

        # Anonymize data
        start_time = time.time()
        process_data(num_rows, anonymized_file, anonymize=True)
        anon_time = time.time() - start_time
        logging.info(f"Data anonymization completed in {anon_time:.2f} seconds")

        logging.info(f"Total processing time: {gen_time + anon_time:.2f} seconds")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()