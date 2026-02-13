from pathlib import Path
from typing import Optional
import polars as pl
from ucimlrepo import fetch_ucirepo
from retaildata.datasets.registry import Dataset
from retaildata.providers.base import BaseProvider
from retaildata.postprocess.metadata import MetadataManager

class UCIProvider(BaseProvider):
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Downloads a dataset from UCI ML Repository using ucimlrepo.
        """
        if dataset.uci_id is None:
            raise ValueError(f"Dataset {dataset.id} does not have a uci_id defined.")

        print(f"Fetching dataset {dataset.id} (UCI ID: {dataset.uci_id}) from UCI ML Repository...")
        
        try:
            repo = fetch_ucirepo(id=dataset.uci_id)
            
            # Destination directory
            destination.mkdir(parents=True, exist_ok=True)
            
            # ucimlrepo provides data as pandas DataFrames in repo.data
            # We convert them to Polars and save as CSV (for raw) or directly to Parquet?
            # Our convention for 'raw' is usually the original format. 
            # But ucimlrepo returns DataFrames. Let's save them as CSV in raw/
            
            # Combine features and targets if targets exist
            df_features = pl.from_pandas(repo.data.features)
            if repo.data.targets is not None:
                df_targets = pl.from_pandas(repo.data.targets)
                # If they have the same length, we can horizontal concat or just save separately
                # Usually better to save as 'data.csv' or separate files
                df_features.write_csv(destination / "features.csv")
                df_targets.write_csv(destination / "targets.csv")
            else:
                df_features.write_csv(destination / "data.csv")

            print(f"Download complete: {destination}")

            # Save metadata
            MetadataManager.save_metadata(
                path=meta_dir / dataset.id / "metadata.json",
                dataset_id=dataset.id,
                provider="uci",
                source_url=f"https://archive.ics.uci.edu/dataset/{dataset.uci_id}",
                uci_id=dataset.uci_id
            )
            
            checksums_path = meta_dir / dataset.id / "checksums.json"
            MetadataManager.save_checksums(destination, checksums_path)

        except Exception as e:
            print(f"Error fetching from UCI: {e}")
            raise
