import sys
from unittest.mock import MagicMock, patch
from pathlib import Path
import polars as pl

# Mock dlt and Retail Express source
mock_dlt = MagicMock()
sys.modules["dlt"] = mock_dlt
sys.modules["dlt.sources.rest_api"] = MagicMock()
sys.modules["duckdb"] = MagicMock()

from retaildata.api import api

def test_dlt_integration():
    print("--- DLT Integration Verification Start ---")
    
    dataset_id = "retail_express"
    output_dir = Path("./test_m7")
    
    # 1. Mock the provider logic
    print(f"\n1. Mocking download for {dataset_id}...")
    
    with patch("retaildata.providers.retail_express.RetailExpressProvider.download") as mock_download:
        # We don't want to actually run dlt in this restricted environment
        # but we want to simulate its output.
        mock_download.side_effect = lambda ds, dest, meta_dir, **kw: (dest.mkdir(parents=True, exist_ok=True), (dest / f"{ds.id}.duckdb").touch())
        
        api.download(
            dataset_id, 
            data_dir=output_dir, 
            api_key="fake_key", 
            base_url="https://fake.retailexpress.com.au"
        )
        print("Download (mocked) completed.")

    # 2. Mock DuckDB and Polars read_database
    print("\n2. Testing load from DuckDB...")
    prepared_dir = output_dir / "prepared" / dataset_id
    prepared_dir.mkdir(parents=True, exist_ok=True)
    db_file = prepared_dir / f"{dataset_id}.duckdb"
    db_file.touch()
    
    # Create a mock dataframe to return
    mock_df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    
    with patch("duckdb.connect") as mock_conn:
        mock_instance = mock_conn.return_value
        mock_instance.execute.return_value.fetchall.return_value = [("customers",), ("orders",)]
        
        with patch("polars.read_database", return_value=mock_df):
            data = api.load(dataset_id, data_dir=output_dir)
            
            print(f"Loaded tables: {list(data.keys())}")
            assert "customers" in data
            assert "orders" in data
            print("Successfully loaded data from (mocked) DuckDB!")

    print("\n--- DLT Integration Verification Complete ---")

if __name__ == "__main__":
    test_dlt_integration()
