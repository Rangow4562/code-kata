from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import random
import string
import logging
from pathlib import Path
import time
from datetime import datetime, timedelta
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared data
FIRST_NAMES = ['Olivia', 'Liam', 'Emma', 'Noah', 'Ava', 'Oliver', 'Sophia', 'Elijah', 'Isabella', 
                'James', 'Mia', 'Benjamin', 'Charlotte', 'Lucas', 'Amelia', 'Henry', 'Harper', 
                'Alexander', 'Evelyn', 'Sebastian', 'Ella', 'Jack']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 
                'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 
                'Moore', 'Jackson', 'Martin', 'Lee']
ADDRESSES = ['Pine St', 'Maple Ave', 'Cedar Lane', 'Oak Blvd', 'Birch Rd', 'Spruce Way', 
                'Fir Dr', 'Willow St', 'Elm Ave', 'Ash Ln', 'Cherry Blvd', 'Poplar Rd', 
                'Dogwood Way', 'Hickory Dr']

# Date range
START_DATE = datetime(1950, 1, 1)
DATE_RANGE = (datetime(2005, 12, 31) - START_DATE).days

# UDFs for data generation and anonymization
@udf(StringType())
def generate_first_name():
    return random.choice(FIRST_NAMES)

@udf(StringType())
def generate_last_name():
    return random.choice(LAST_NAMES)

@udf(StringType())
def generate_address():
    return f"{random.randint(1, 999)} {random.choice(ADDRESSES)}"

@udf(StringType())
def generate_date():
    return (START_DATE + timedelta(days=random.randint(0, DATE_RANGE))).strftime('%d/%m/%Y')

@udf(StringType())
def anonymize_name(name):
    return f"{name[0]}{''.join(random.choices(string.ascii_lowercase, k=5))}"

@udf(StringType())
def anonymize_address(address):
    parts = address.split()
    return f"{random.randint(1, 99)} {''.join(random.choices(string.ascii_lowercase, k=5))} {parts[-1]}"

def create_spark_session():
    return (SparkSession.builder
            .appName("DataProcessor")
            .config("spark.sql.shuffle.partitions", "100")  # Adjust based on your cluster
            .config("spark.default.parallelism", "100")    # Adjust based on your cluster
            .getOrCreate())

def generate_data(spark, num_rows):
    return (spark.range(num_rows)
            .withColumn("first_name", generate_first_name())
            .withColumn("last_name", generate_last_name())
            .withColumn("address", generate_address())
            .withColumn("date_of_birth", generate_date()))

def anonymize_data(df):
    return (df.withColumn("first_name", anonymize_name(col("first_name")))
            .withColumn("last_name", anonymize_name(col("last_name")))
            .withColumn("address", anonymize_address(col("address"))))

def process_data(spark, num_rows, output_file, anonymize=False):
    df = generate_data(spark, num_rows)
    
    if anonymize:
        df = anonymize_data(df)
    
    df.write.csv(output_file, header=True, mode="overwrite")

def parse_args():
    parser = argparse.ArgumentParser(description="Generate and anonymize mock data using PySpark")
    parser.add_argument('--num_rows', type=int, default=20_000_000, help='Number of rows to generate')
    parser.add_argument('--output_dir', type=str, default='./output', help='Directory to save the output files')
    return parser.parse_args()

def main():
    args = parse_args()
    
    try:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(exist_ok=True)

        mock_data_file = str(output_dir / 'data')
        anonymized_file = str(output_dir / 'anonymized')

        num_rows = args.num_rows

        spark = create_spark_session()

        # Generate mock data
        start_time = time.time()
        process_data(spark, num_rows, mock_data_file)
        gen_time = time.time() - start_time
        logging.info(f"Data generation completed in {gen_time:.2f} seconds")

        # Anonymize data
        start_time = time.time()
        process_data(spark, num_rows, anonymized_file, anonymize=True)
        anon_time = time.time() - start_time
        logging.info(f"Data anonymization completed in {anon_time:.2f} seconds")

        logging.info(f"Total processing time: {gen_time + anon_time:.2f} seconds")

        spark.stop()

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == '__main__':
    main()
