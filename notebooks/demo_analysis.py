"""
Demo Analysis Script for RetailData

This script demonstrates how to:
1. Download a dataset using retaildata.
2. Prepare it (convert to Parquet).
3. Load it efficiently using Polars.
"""

import retaildata.api as rd
import polars as pl
from pathlib import Path

def main():
    dataset_id = "test_http" # Using test dataset for demo purposes; replace with 'online_retail_ii' if creds set
    
    print(f"--- 1. Downloading and Preparing {dataset_id} ---")
    # This will download the dataset and convert it to Parquet
    rd.api.download(dataset_id, prepare=True)
    
    print(f"\n--- 2. Loading Data with Polars ---")
    # Path to prepared Parquet file
    # Note: In a real scenario, you might need to find the specific parquet file if there are many
    prepared_dir = Path(f"/home/josephus/.local/share/retaildata/prepared/{dataset_id}")
    parquet_files = list(prepared_dir.glob("*.parquet"))
    
    if not parquet_files:
        print("No parquet files found. Something went wrong.")
        return

    parquet_file = parquet_files[0]
    print(f"Loading {parquet_file}...")
    
    # Lazy evaluation is great for large datasets
    q = pl.scan_parquet(parquet_file)
    
    print("\n--- 3. Basic Inspection ---")
    # Show schema
    print("Schema:")
    print(q.schema)
    
    # Show first 5 rows
    print("\nPreview:")
    print(q.fetch(5))
    
    print("\n--- 4. Simple Aggregation ---")
    # Example aggregation (assuming iris dataset columns for test_http)
    if "species" in q.columns:
        print("Mean sepal_length by species:")
        print(
            q.group_by("species")
             .agg(pl.col("sepal_length").mean())
             .collect()
        )

if __name__ == "__main__":
    main()
