# main.py
from pathlib import Path
from anonymizer.anonymizer import Anonymizer
from anonymizer.generator import MockDataGenerator
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        output_dir = Path('./output')
        output_dir.mkdir(exist_ok=True)

        mock_data_file = output_dir / 'data.csv'
        anonymized_file = output_dir / 'anonymized.csv'

        # Generate mock data
        generator = MockDataGenerator(num_rows=100, output_file=mock_data_file)
        generator.generate_mock_data()

        # Define fields to anonymize
        fields_to_anonymize = ['first_name', 'last_name', 'address']

        # Anonymize data with specified fields
        anonymizer = Anonymizer(input_file=mock_data_file, output_file=anonymized_file, fields_to_anonymize=fields_to_anonymize)
        anonymizer.anonymize_data()

        logging.info("Data generation and anonymization completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()
