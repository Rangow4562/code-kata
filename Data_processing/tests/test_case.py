from pathlib import Path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from anonymizerscale.anonymizer import Anonymizer
from anonymizerscale.generator import MockDataGenerator
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        output_dir = Path('./output')
        output_dir.mkdir(exist_ok=True)

        mock_data_file = output_dir / 'data.csv'
        anonymized_file = output_dir / 'anonymized.csv'

        # Generate mock data (approximately 2GB)
        num_rows = 20_000_000  # Adjust this number to achieve desired file size
        start_time = time.time()
        generator = MockDataGenerator(num_rows=num_rows, output_file=mock_data_file)
        generator.generate_mock_data()
        gen_time = time.time() - start_time
        logging.info(f"Data generation completed in {gen_time:.2f} seconds")

        # Anonymize data
        start_time = time.time()
        anonymizer = Anonymizer(input_file=mock_data_file, output_file=anonymized_file)
        anonymizer.anonymize_data()
        anon_time = time.time() - start_time
        logging.info(f"Data anonymization completed in {anon_time:.2f} seconds")

        logging.info(f"Total processing time: {gen_time + anon_time:.2f} seconds")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()