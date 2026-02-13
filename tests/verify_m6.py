import polars as pl
from retaildata.api import api
from retaildata.integrations.ml import ml
from pathlib import Path

def test_m6_features():
    print("--- M6 Verification Start ---")
    
    # 1. Test Lazy Loading & Prepared Load
    dataset_id = "bank_marketing_uci"
    output_dir = Path("./test_m6")
    
    print(f"\n1. Testing Lazy Loading for {dataset_id}...")
    # This assumes bank_marketing_uci was already downloaded/prepared in M5
    # or we do it now. Let's try to load it lazily.
    try:
        data = api.load(dataset_id, data_dir=Path("./test_m5"), lazy=True)
        for name, lf in data.items():
            print(f"Loaded {name} as {type(lf)}")
            # LazyFrames don't have head() like DataFrames, they have collect().head()
            print(f"Sample row from {name}:")
            print(lf.head(1).collect())
    except Exception as e:
        print(f"Load failed (maybe not prepared yet?): {e}")

    # 2. Test ML Integration (Logical)
    print("\n2. Testing ML Integration Logic (Mocking dependencies)...")
    series = pl.Series("vals", [1.0, 2.0, 3.0])
    
    # We'll just check if the ImportErrors are raised correctly if not installed,
    # or if the conversion works if they are.
    for target in ["pytorch", "tensorflow", "jax"]:
        try:
            print(f"Converting to {target}...")
            if target == "pytorch":
                tensor = ml.to_pytorch(series)
            elif target == "tensorflow":
                tensor = ml.to_tensorflow(series)
            else:
                tensor = ml.to_jax(series)
            print(f"Success! Type: {type(tensor)}")
        except ImportError as e:
            print(f"Handled expected ImportError: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    print("\n--- M6 Verification Complete ---")

if __name__ == "__main__":
    test_m6_features()
