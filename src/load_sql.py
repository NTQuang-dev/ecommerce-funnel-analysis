"""Batch-load the cleaned CSVs into the MySQL raw_events table."""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "ecommerce_db")

CLEANED_DATA_PATH = "data/cleaned/"
FILES = ["cleaned_2019-Oct.csv", "cleaned_2019-Nov.csv"]


def load_cleaned_data(file_name: str) -> None:
    file_path = os.path.join(CLEANED_DATA_PATH, file_name)
    if not os.path.exists(file_path):
        print(f"Skipped: {file_path} not found.")
        return

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    )

    print(f"Loading {file_name}...")
    rows_loaded = 0
    for chunk in pd.read_csv(file_path, chunksize=100_000):
        chunk.to_sql("raw_events", con=engine, if_exists="append", index=False)
        rows_loaded += len(chunk)
        print(f"  ...{rows_loaded:,} rows")

    print(f"Done: {file_name}")


if __name__ == "__main__":
    for f in FILES:
        load_cleaned_data(f)
