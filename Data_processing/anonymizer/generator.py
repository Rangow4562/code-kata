import csv
from typing import List, Dict
from pathlib import Path
import random
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MockDataGenerator:
    """
    Generates mock data and writes it to a CSV file.

    Attributes:
        num_rows (int): The number of rows to generate.
        output_file (Path): The path to the output CSV file.
        fields (List[str]): The list of fields to be generated.
    """

    FIRST_NAMES = ['Olivia', 'Liam', 'Emma', 'Noah', 'Ava', 'Oliver', 'Sophia', 'Elijah', 'Isabella', 
                'James', 'Mia', 'Benjamin', 'Charlotte', 'Lucas', 'Amelia', 'Henry', 'Harper', 
                'Alexander', 'Evelyn', 'Sebastian', 'Ella', 'Jack']
    LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 
                'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 
                'Moore', 'Jackson', 'Martin', 'Lee']
    ADDRESSES = ['Pine St', 'Maple Ave', 'Cedar Lane', 'Oak Blvd', 'Birch Rd', 'Spruce Way', 
                'Fir Dr', 'Willow St', 'Elm Ave', 'Ash Ln', 'Cherry Blvd', 'Poplar Rd', 
                'Dogwood Way', 'Hickory Dr']

    def __init__(self, num_rows: int, output_file: Path, fields: List[str] = None):
        self.num_rows = num_rows
        self.output_file = output_file
        self.fields = fields or ['first_name', 'last_name', 'address', 'date_of_birth']

    def generate_mock_data(self) -> None:
        """
        Generates the mock data and writes it to the output file.
        """
        try:
            data = self._generate_data()
            self._write_data(data)
            logging.info(f"Mock data generated successfully. Output saved to {self.output_file}")
        except (IOError, csv.Error) as e:
            logging.error(f"File handling error: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            raise

    def _generate_data(self) -> List[Dict[str, str]]:
        """
        Generates the specified number of rows of mock data.

        Returns:
            List[Dict[str, str]]: The generated data.
        """
        return [self._generate_row() for _ in range(self.num_rows)]

    def _generate_row(self) -> Dict[str, str]:
        """
        Generates a single row of mock data.

        Returns:
            Dict[str, str]: The generated row.
        """
        row = {}
        if 'first_name' in self.fields:
            row['first_name'] = random.choice(self.FIRST_NAMES)
        if 'last_name' in self.fields:
            row['last_name'] = random.choice(self.LAST_NAMES)
        if 'address' in self.fields:
            row['address'] = self._generate_address()
        if 'date_of_birth' in self.fields:
            row['date_of_birth'] = self._generate_date()
        return row

    def _generate_address(self) -> str:
        """
        Generates a random address.

        Returns:
            str: The generated address.
        """
        return f"{random.randint(1, 999)} {random.choice(self.ADDRESSES)}"

    @staticmethod
    def _generate_date() -> str:
        """
        Generates a random date of birth.

        Returns:
            str: The generated date of birth.
        """
        start_date = datetime(1950, 1, 1)
        end_date = datetime(2005, 12, 31)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + timedelta(days=random_number_of_days)
        return random_date.strftime("%d/%m/%Y")

    def _write_data(self, data: List[Dict[str, str]]) -> None:
        """
        Writes the generated data to the output CSV file.

        Args:
            data (List[Dict[str, str]]): The generated data.
        """
        try:
            with open(self.output_file, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        except IOError as e:
            logging.error(f"Error writing output file: {str(e)}")
            raise
