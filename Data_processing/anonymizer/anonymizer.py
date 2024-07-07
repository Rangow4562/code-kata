import csv
import random
import string
from typing import Dict, List
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Anonymizer:
    """
    Anonymizes specified fields in a CSV file and writes the result to another CSV file.

    Attributes:
        input_file (Path): The path to the input CSV file.
        output_file (Path): The path to the output CSV file.
        fields_to_anonymize (List[str]): The list of fields to be anonymized.
    """

    def __init__(self, input_file: Path, output_file: Path, fields_to_anonymize: List[str]):
        self.input_file = input_file
        self.output_file = output_file
        self.fields_to_anonymize = fields_to_anonymize

    def anonymize_data(self) -> None:
        """
        Anonymizes the data from the input file and writes it to the output file.
        """
        try:
            data = self._read_data()
            anonymized_data = self._process_data(data)
            self._write_data(anonymized_data)
            logging.info(f"Data anonymized successfully. Output saved to {self.output_file}")
        except (IOError, csv.Error) as e:
            logging.error(f"File handling error: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            raise

    def _read_data(self) -> List[Dict[str, str]]:
        """
        Reads data from the input CSV file.

        Returns:
            List[Dict[str, str]]: The data read from the CSV file.
        """
        try:
            with open(self.input_file, 'r', newline='') as csvfile:
                return list(csv.DictReader(csvfile))
        except IOError as e:
            logging.error(f"Error reading input file: {str(e)}")
            raise

    def _process_data(self, data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Processes the data to anonymize specified fields.

        Args:
            data (List[Dict[str, str]]): The original data.

        Returns:
            List[Dict[str, str]]: The anonymized data.
        """
        return [self._anonymize_row(row) for row in data]

    def _anonymize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        Anonymizes specified fields in a single row.

        Args:
            row (Dict[str, str]): The original row.

        Returns:
            Dict[str, str]: The anonymized row.
        """
        for field in self.fields_to_anonymize:
            if field in row:
                row[field] = self._anonymize_field(field, row[field])
        return row

    def _anonymize_field(self, field: str, value: str) -> str:
        """
        Anonymizes a single field value based on the field name.

        Args:
            field (str): The field name.
            value (str): The original field value.

        Returns:
            str: The anonymized field value.
        """
        if field in ['first_name', 'last_name']:
            return self._anonymize_name(value)
        elif field == 'address':
            return self._anonymize_address(value)
        return value

    @staticmethod
    def _anonymize_name(name: str) -> str:
        """
        Anonymizes a name by keeping the first letter and replacing the rest with random letters.

        Args:
            name (str): The original name.

        Returns:
            str: The anonymized name.
        """
        return f"{name[0]}{''.join(random.choices(string.ascii_lowercase, k=5))}"

    @staticmethod
    def _anonymize_address(address: str) -> str:
        """
        Anonymizes an address by replacing the middle part with random letters.

        Args:
            address (str): The original address.

        Returns:
            str: The anonymized address.
        """
        parts = address.split()
        return f"{random.randint(1, 99)} {''.join(random.choices(string.ascii_lowercase, k=5))} {parts[-1]}"

    def _write_data(self, data: List[Dict[str, str]]) -> None:
        """
        Writes the anonymized data to the output CSV file.

        Args:
            data (List[Dict[str, str]]): The anonymized data.
        """
        try:
            with open(self.output_file, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        except IOError as e:
            logging.error(f"Error writing output file: {str(e)}")
            raise
