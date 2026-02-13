from pathlib import Path
from typing import Optional, List, Any, Dict
from retaildata.datasets.registry import Registry, Dataset
from retaildata.providers.http import HTTPProvider
from retaildata.providers.kaggle import KaggleProvider
from retaildata.config import settings
from rich import print as rprint

class RetailDataAPI:
    def list_datasets(self) -> List[Dataset]:
        """Lists all available datasets."""
        return Registry.list_all()

    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        """Gets a dataset by ID."""
        return Registry.get(dataset_id)

    def download(
        self, 
        dataset_id: str, 
        data_dir: Optional[Path] = None, 
        prepare: bool = False, 
        lazy: bool = False,
        sample_fraction: Optional[float] = None,
        stratify_col: Optional[str] = None,
        split_fraction: Optional[float] = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Downloads a dataset.
        
        Args:
            dataset_id: The ID of the dataset to download.
            data_dir: Optional directory to download to. Defaults to settings.final_data_dir.
            prepare: If True, convert the dataset to Parquet after download.
            sample_fraction: Optional fraction for sampling (0.0 to 1.0).
            stratify_col: Optional column name for stratified sampling.
            split_fraction: Optional fraction for train/test splitting (e.g. 0.8).
            **kwargs: Additional provider-specific arguments.
        """
        dataset = self.get_dataset(dataset_id)
        if not dataset:
            raise ValueError(f"Dataset '{dataset_id}' not found in registry.")
        
        target_dir = data_dir or settings.final_data_dir
        # Structure: <data_dir>/raw/<dataset_id>/...
        download_path = target_dir / "raw" / dataset.id
        meta_dir = target_dir / "meta"
        
        rprint(f"[bold blue]RetailData[/bold blue]: Downloading {dataset.id} to {download_path}")

        if dataset.provider == "http":
            provider = HTTPProvider()
            provider.download(dataset, download_path, meta_dir=meta_dir, **kwargs)
        elif dataset.provider == "kaggle":
            provider = KaggleProvider()
            provider.download(dataset, download_path, meta_dir=meta_dir, **kwargs)
        elif dataset.provider == "hf":
            from retaildata.providers.hf import HFProvider
            provider = HFProvider()
            provider.download(dataset, download_path, meta_dir=meta_dir, **kwargs)
        elif dataset.provider == "uci":
            from retaildata.providers.uci import UCIProvider
            provider = UCIProvider()
            provider.download(dataset, download_path, meta_dir=meta_dir, **kwargs)
        elif dataset.provider == "openml":
            from retaildata.providers.openml import OpenMLProvider
            provider = OpenMLProvider()
            provider.download(dataset, download_path, meta_dir=meta_dir, **kwargs)
        elif dataset.provider == "dlt":
            if dataset.id == "retail_express":
                from retaildata.providers.retail_express import RetailExpressProvider
                provider = RetailExpressProvider()
            else:
                from retaildata.providers.dlt import DLTProvider
                provider = DLTProvider()
            provider.download(dataset, download_path, meta_dir=meta_dir, **kwargs)
        else:
            raise NotImplementedError(f"Provider '{dataset.provider}' not yet supported.")
            
        rprint(f"[green]Successfully processed dataset '{dataset.id}'[/green]")
        
        if prepare:
            from retaildata.processing.manager import manager as processing_manager
            rprint(f"[bold blue]RetailData[/bold blue]: Preparing {dataset.id} (converting to Parquet)...")
            processing_manager.process_dataset(
                dataset.id, 
                data_dir=target_dir,
                sample_fraction=sample_fraction,
                stratify_col=stratify_col,
                split_fraction=split_fraction
            )
            return self.load(dataset.id, data_dir=target_dir, lazy=lazy)
        
        return None

    def get(
        self,
        dataset_id: str,
        data_dir: Optional[Path] = None,
        cache: bool = True,
        prepare: bool = False,
        lazy: bool = False,
        sample_fraction: Optional[float] = None,
        stratify_col: Optional[str] = None,
        split_fraction: Optional[float] = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """Alias for ``download`` matching the public API contract."""
        return self.download(
            dataset_id=dataset_id,
            data_dir=data_dir,
            cache=cache,
            prepare=prepare,
            lazy=lazy,
            sample_fraction=sample_fraction,
            stratify_col=stratify_col,
            split_fraction=split_fraction,
            **kwargs,
        )

    def load(
        self, 
        dataset_id: str, 
        data_dir: Optional[Path] = None, 
        lazy: bool = False,
        standardized: bool = False
    ) -> Dict[str, Any]:
        """
        Loads prepared Parquet files for a dataset.
        
        Args:
            dataset_id: The ID of the dataset to load.
            data_dir: Optional directory.
            lazy: If True, returns Polars LazyFrames.
            standardized: If True, uses the dataset's standard_mapping to rename keys (e.g. 'sales', 'calendar').
        
        Returns:
            A dictionary mapping keys to Polars DataFrames (or LazyFrames).
        """
        import polars as pl
        base_dir = data_dir or settings.final_data_dir
        prepared_dir = base_dir / "prepared" / dataset_id
        
        if not prepared_dir.exists():
            raise FileNotFoundError(f"Prepared data for '{dataset_id}' not found at {prepared_dir}")
            
        data = {}
        for file_path in prepared_dir.glob("*.parquet"):
            key = file_path.stem
            if lazy:
                data[key] = pl.scan_parquet(file_path)
            else:
                data[key] = pl.read_parquet(file_path)
        
        # Check for DuckDB files (M7: dlt integration)
        for file_path in prepared_dir.glob("*.duckdb"):
            import duckdb
            con = duckdb.connect(str(file_path))
            tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
            for (table_name,) in tables:
                if table_name.startswith("_dlt"):
                    continue
                if lazy:
                    data[table_name] = pl.read_database(f"SELECT * FROM {table_name}", connection=con).lazy()
                else:
                    data[table_name] = pl.read_database(f"SELECT * FROM {table_name}", connection=con)
            con.close()
        
        if standardized:
            dataset = self.get_dataset(dataset_id)
            if dataset and dataset.standard_mapping:
                mapped_data = {}
                for std_key, actual_key in dataset.standard_mapping.items():
                    # Handle direct match
                    if actual_key in data:
                        mapped_data[std_key] = data[actual_key]
                    # Handle train/test suffixes from processing_manager
                    elif f"{actual_key}_train" in data:
                        mapped_data[f"{std_key}_train"] = data[f"{actual_key}_train"]
                        mapped_data[f"{std_key}_test"] = data[f"{actual_key}_test"]
                return mapped_data

        return data

    def split_temporal(
        self, 
        dataset_id: str, 
        date_col: str, 
        split_date: str, 
        table_key: Optional[str] = None,
        data_dir: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Splits a specific table in the dataset into train and test sets based on a date.
        """
        import polars as pl
        data = self.load(dataset_id, data_dir=data_dir)
        
        # Identify which table to split
        if table_key:
            key = table_key
        else:
            # Try 'sales' from standard mapping, or fallback to first table
            dataset = self.get_dataset(dataset_id)
            if dataset and dataset.standard_mapping:
                key = dataset.standard_mapping.get("sales", list(data.keys())[0])
            else:
                key = list(data.keys())[0]

        df = data[key]
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
            
        # Ensure date_col is datetime/date
        if df[date_col].dtype not in [pl.Datetime, pl.Date]:
            df = df.with_columns(pl.col(date_col).str.to_datetime())

        # Convert split_date string to datetime for comparison
        split_dt = pl.lit(split_date).str.to_datetime()
        
        train = df.filter(pl.col(date_col) < split_dt)
        test = df.filter(pl.col(date_col) >= split_dt)
        
        return {"train": train, "test": test}


api = RetailDataAPI()


def list_datasets() -> List[Dataset]:
    """Return all dataset descriptors in the registry."""
    return api.list_datasets()


def get(dataset_id: str, data_dir: Optional[Path] = None, cache: bool = True, prepare: bool = False, **kwargs) -> Optional[Dict[str, Any]]:
    """Download a dataset and optionally prepare it."""
    return api.get(dataset_id=dataset_id, data_dir=data_dir, cache=cache, prepare=prepare, **kwargs)


def load(dataset_id: str, data_dir: Optional[Path] = None, lazy: bool = False, standardized: bool = False) -> Dict[str, Any]:
    """Load prepared dataset artifacts."""
    return api.load(dataset_id=dataset_id, data_dir=data_dir, lazy=lazy, standardized=standardized)


def purge(dataset_id: Optional[str] = None, all: bool = False) -> None:
    """Delete one dataset or purge all managed dataset data."""
    from retaildata.cache.manager import manager as cache_manager

    if all:
        cache_manager.purge_all()
        return
    if dataset_id:
        cache_manager.delete_dataset(dataset_id)
        return
    raise ValueError("Pass dataset_id or all=True to purge data")
