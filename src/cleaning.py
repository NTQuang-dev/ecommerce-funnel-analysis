"""
Data Cleaning Module for E-commerce Funnel Analysis.

This module provides functionality to process large-scale e-commerce event logs
from Kaggle, specifically designed for memory-constrained environments.
It performs filtering, type-casting, and deduplication to prepare data
for a conversion funnel analysis in MySQL/Power BI.

"""

import pandas as pd
import os

# Configuration Constants
RAW_DATA_PATH = "data/raw/"
CLEANED_DATA_PATH = "data/cleaned/"
FILES = ["2019-Oct.csv", "2019-Nov.csv"]


def clean_ecommerce_data(file_name: str) -> None:
    """
    Reads a raw CSV file in chunks, cleans it, and exports a refined version.

    The function optimizes memory by downcasting numeric types and filtering
    for specific event types (view, cart, purchase) early in the pipeline.

    Args:
        file_name (str): The name of the file located in RAW_DATA_PATH to process.

    Returns:
        None: Saves the cleaned DataFrame to CLEANED_DATA_PATH as a CSV.

    Raises:
        FileNotFoundError: If the input file does not exist in the raw directory.
        Exception: General errors during Pandas processing.
    """
    input_path = os.path.join(RAW_DATA_PATH, file_name)
    output_path = os.path.join(CLEANED_DATA_PATH, f"cleaned_{file_name}")

    print(f"Starting processing for {file_name}...")

    # Type definition for memory optimization
    dtypes = {
        "event_type": "category",
        "product_id": "int32",
        "category_id": "int64",
        "brand": "category",
        "price": "float32",
        "user_id": "int32",
        "user_session": "str",
    }

    chunk_list = []

    try:
        # read_csv using chunking to prevent memory errors
        for chunk in pd.read_csv(
            input_path, dtype=dtypes, chunksize=500000, parse_dates=["event_time"]
        ):
            # Filter for Funnel Analysis events
            important_events = ["view", "cart", "purchase"]
            chunk = chunk[chunk["event_type"].isin(important_events)]

            # Remove records without session IDs
            chunk = chunk.dropna(subset=["user_session"])

            chunk_list.append(chunk)

        # Combine processed chunks into a single DataFrame
        df = pd.concat(chunk_list)

        # Cleanup
        df = df.drop_duplicates()

        # Export to CSV 
        df.to_csv(output_path, index=False)
        print(f"Success: Cleaned file saved to {output_path}")

    except Exception as e:
        print(f"Error processing {file_name}: {str(e)}")


if __name__ == "__main__":
    """Entry point of the script for batch processing files."""
    for f in FILES:
        if os.path.exists(os.path.join(RAW_DATA_PATH, f)):
            clean_ecommerce_data(f)
        else:
            print(f"Warning: File {f} not found in {RAW_DATA_PATH}")
