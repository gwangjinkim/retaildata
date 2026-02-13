from pathlib import Path
from typing import Optional
import polars as pl
import openml
from retaildata.datasets.registry import Dataset
from retaildata.providers.base import BaseProvider
from retaildata.postprocess.metadata import MetadataManager

class OpenMLProvider(BaseProvider):
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Downloads a dataset from OpenML using openml-python.
        """
        if dataset.openml_id is None:
            raise ValueError(f"Dataset {dataset.id} does not have an openml_id defined.")

        print(f"Fetching dataset {dataset.id} (OpenML ID: {dataset.openml_id}) from OpenML...")
        
        try:
            # openml.datasets.get_dataset returns a dataset object
            oml_dataset = openml.datasets.get_dataset(dataset.openml_id)
            
            # Get data as a pandas DataFrame (with categorical types handled)
            X, y, categorical_indicator, attribute_names = oml_dataset.get_data(
                target=oml_dataset.default_target_attribute,
                dataset_format="dataframe"
            )
            
            # Destination directory
            destination.mkdir(parents=True, exist_ok=True)
            
            # Convert to Polars
            df = pl.from_pandas(X)
            if y is not None:
                # If target is a Series, convert to DataFrame and concat or save separately
                df_y = pl.from_pandas(y.to_frame() if hasattr(y, "to_frame") else y)
                # Save both
                df.write_csv(destination / "features.csv")
                df_y.write_csv(destination / "targets.csv")
            else:
                df.write_csv(destination / "data.csv")

            print(f"Download complete: {destination}")

            # Save metadata
            MetadataManager.save_metadata(
                path=meta_dir / dataset.id / "metadata.json",
                dataset_id=dataset.id,
                provider="openml",
                source_url=f"https://www.openml.org/d/{dataset.openml_id}",
                openml_id=dataset.openml_id
            )
            
            checksums_path = meta_dir / dataset.id / "checksums.json"
            MetadataManager.save_checksums(destination, checksums_path)

        except Exception as e:
            print(f"Error fetching from OpenML: {e}")
            raise
