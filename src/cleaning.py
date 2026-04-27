"""Clean the raw monthly CSVs and write filtered copies to data/cleaned/."""

import os
import pandas as pd

RAW_DATA_PATH = "data/raw/"
CLEANED_DATA_PATH = "data/cleaned/"
FILES = ["2019-Oct.csv", "2019-Nov.csv"]

# Downcasting these keeps a 5GB CSV under a few hundred MB in memory.
DTYPES = {
    "event_type": "category",
    "product_id": "int32",
    "category_id": "int64",
    "brand": "category",
    "price": "float32",
    "user_id": "int32",
    "user_session": "str",
}

FUNNEL_EVENTS = ["view", "cart", "purchase"]


def clean_ecommerce_data(file_name: str) -> None:
    input_path = os.path.join(RAW_DATA_PATH, file_name)
    output_path = os.path.join(CLEANED_DATA_PATH, f"cleaned_{file_name}")

    print(f"Processing {file_name}...")

    chunks = []
    for chunk in pd.read_csv(
        input_path, dtype=DTYPES, chunksize=500_000, parse_dates=["event_time"]
    ):
        chunk = chunk[chunk["event_type"].isin(FUNNEL_EVENTS)]
        chunk = chunk.dropna(subset=["user_session"])
        chunks.append(chunk)

    df = pd.concat(chunks).drop_duplicates()
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df):,} rows to {output_path}")


if __name__ == "__main__":
    for f in FILES:
        if os.path.exists(os.path.join(RAW_DATA_PATH, f)):
            clean_ecommerce_data(f)
        else:
            print(f"Skipped: {f} not found in {RAW_DATA_PATH}")
