from pathlib import Path
from typing import Any, Dict, Optional
import polars as pl
from retaildata.datasets.registry import Dataset
from retaildata.providers.base import BaseProvider
from retaildata.postprocess.metadata import MetadataManager

class DLTProvider(BaseProvider):
    """
    Base provider for sources that use dlt (data load tool).
    """
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Runs a dlt pipeline.
        """
        try:
            import dlt
        except ImportError:
            raise ImportError(
                "dlt is not installed. Please install it with: pip install \"retaildata[dlt]\""
            )

        pipeline_name = f"{dataset.id}_pipeline"
        
        # We use duckdb as an intermediate destination for dlt
        # because it's fast and easy to query from Polars.
        db_path = destination / f"{dataset.id}.duckdb"
        
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            credentials=kwargs.get("credentials"),
            dataset_name=dataset.id,
        )
        
        # Specific source implementation
        source = self.get_source(dataset, **kwargs)
        
        print(f"Running dlt pipeline for {dataset.id}...")
        load_info = pipeline.run(source)
        print(load_info)
        
        # Save metadata
        MetadataManager.save_metadata(
            path=meta_dir / dataset.id / "metadata.json",
            dataset_id=dataset.id,
            provider="dlt",
            source_url=kwargs.get("source_url", "dlt-source")
        )
        
        # dlt creates a duckdb file. We'll store its path in the metadata 
        # so that load() can find it.
        # Actually, let's just ensure it's in the raw directory.
        print(f"Data locally available in DuckDB: {db_path}")

    def get_source(self, dataset: Dataset, **kwargs) -> Any:
        """
        Returns the dlt source to be run. Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement get_source")
