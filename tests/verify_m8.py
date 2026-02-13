import sys
from pathlib import Path
import polars as pl
import numpy as np
from datetime import datetime

# Setup paths
sys.path.append(str(Path(__file__).parent.parent))

from retaildata.api import api
from retaildata.utils.hierarchies import get_hierarchy_groups
from retaildata.utils.interventions import get_intervention_windows

def test_m8_utilities():
    print("--- M8 Bayesian Data Utilities Verification Start ---")
    
    # 1. Test Standardized Loading (Mocked)
    print("\n1. Testing standardized loading...")
    dataset_id = "m5"
    prepared_dir = Path("./test_m8/prepared") / dataset_id
    prepared_dir.mkdir(parents=True, exist_ok=True)
    
    # Create mock parquet files
    pl.DataFrame({"d": ["d_1", "d_2"], "sales": [10, 20]}).write_parquet(prepared_dir / "sales_train_evaluation.parquet")
    pl.DataFrame({"d": ["d_1", "d_2"], "date": ["2011-01-29", "2011-01-30"]}).write_parquet(prepared_dir / "calendar.parquet")
    
    data = api.load(dataset_id, data_dir=Path("./test_m8"), standardized=True)
    print(f"Standardized keys: {list(data.keys())}")
    assert "sales" in data
    assert "calendar" in data
    print("Standardization successful!")

    # 2. Test Temporal Splitting
    print("\n2. Testing temporal splitting...")
    df_sales = data["sales"]
    df_cal = data["calendar"]
    # Join them to get dates in sales
    df_joined = df_sales.join(df_cal, on="d")
    
    # We'll use a local mock for split_temporal testing
    # but let's test the api method itself
    split_res = api.split_temporal(dataset_id, date_col="date", split_date="2011-01-30", table_key="calendar", data_dir=Path("./test_m8"))
    print(f"Train size: {len(split_res['train'])}, Test size: {len(split_res['test'])}")
    assert len(split_res["train"]) == 1
    assert len(split_res["test"]) == 1
    print("Temporal splitting successful!")

    # 3. Test Hierarchies
    print("\n3. Testing hierarchy builders...")
    mock_hierarchy_df = pl.DataFrame({
        "item_id": ["A", "B", "C"],
        "dept_id": ["D1", "D1", "D2"]
    })
    groups = get_hierarchy_groups(mock_hierarchy_df, "item_id", "dept_id")
    print(f"Parent indices for items: {groups['parent_indices']}")
    # Item A -> D1 (index 0), Item B -> D1 (index 0), Item C -> D2 (index 1)
    assert np.array_equal(groups["parent_indices"], [0, 0, 1])
    print("Hierarchy building successful!")

    # 4. Test Intervention Windows
    print("\n4. Testing intervention windows...")
    windows = get_intervention_windows("store_sales") # Favorita
    print(f"Favorita windows: {list(windows.keys())}")
    assert "earthquake" in windows
    print("Intervention metadata successful!")

    print("\n--- M8 Bayesian Data Utilities Verification Complete ---")

if __name__ == "__main__":
    test_m8_utilities()
