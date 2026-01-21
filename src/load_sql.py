"""
Data Loading Module.

This script handles the 'Load' part of the ETL pipeline, moving cleaned 
CSV data into a local MySQL instance using SQLAlchemy for optimized 
batch inserts.

Author: [Your Name]
"""

import pandas as pd
from sqlalchemy import create_engine
import os

# Configuration MySQL connection
DB_USER = "root"
DB_PASS = "root"
DB_HOST = "localhost"
DB_NAME = "ecommerce_db"

def load_cleaned_data(file_name: str) -> None:
    """
    Reads a cleaned CSV and streams it into the MySQL 'raw_events' table.

    Args:
        file_name (str): The name of the cleaned file in data/cleaned/
    """
    file_path = os.path.join('data', 'cleaned', file_name)
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    # Create the connection engine
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(connection_string)

    print(f"Connecting to MySQL and loading {file_name}...")

    try:
        # Load the data in
        for chunk in pd.read_csv(file_path, chunksize=100000):
            chunk.to_sql('raw_events', con=engine, if_exists='append', index=False)
            print(f"Uploaded a chunk of 100,000 rows...")

        print(f"Successfully loaded {file_name} into the database!")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Load October first
    load_cleaned_data('cleaned_2019-Oct.csv')
    
    # Load November second
    load_cleaned_data('cleaned_2019-Nov.csv')