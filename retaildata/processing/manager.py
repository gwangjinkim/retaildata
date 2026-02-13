from pathlib import Path
from typing import List, Optional
import polars as pl
from retaildata.config import settings
from rich import print as rprint

class ProcessingManager:
    def __init__(self):
        self.data_dir = settings.final_data_dir

    def _get_path(self, dataset_id: str, subdir: str) -> Path:
        return self.data_dir / subdir / dataset_id

    def process_dataset(
        self, 
        dataset_id: str, 
        data_dir: Optional[Path] = None, 
        sample_fraction: Optional[float] = None,
        stratify_col: Optional[str] = None,
        split_fraction: Optional[float] = None
    ) -> bool:
        """
        Converts raw dataset files to Parquet format with optional sampling and splitting.
        """
        base_dir = data_dir or self.data_dir
        raw_dir = base_dir / "raw" / dataset_id
        target_dir = base_dir / "prepared" / dataset_id
        
        if not raw_dir.exists():
            rprint(f"[red]Raw data for {dataset_id} not found at {raw_dir}[/red]")
            return False

        target_dir.mkdir(parents=True, exist_ok=True)
        
        success = False
        files_processed = 0

        # Iterate over supported files
        for file_path in raw_dir.rglob("*"):
            if file_path.is_file():
                try:
                    df = None
                    suffix = file_path.suffix.lower()
                    
                    if suffix == ".csv":
                        rprint(f"Reading CSV: {file_path.name}")
                        df = pl.read_csv(file_path, ignore_errors=True, infer_schema_length=10000)
                    elif suffix in [".xls", ".xlsx"]:
                         rprint(f"Reading Excel: {file_path.name}")
                         df = pl.read_excel(file_path)
                    elif suffix == ".duckdb":
                        rprint(f"Reading DuckDB: {file_path.name}")
                        import duckdb
                        con = duckdb.connect(str(file_path))
                        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
                        for (table_name,) in tables:
                            if table_name.startswith("_dlt"): continue
                            rprint(f"  Processing table: {table_name}")
                            table_df = pl.read_database(f"SELECT * FROM {table_name}", connection=con)
                            # recursion-like call or just process here
                            self._process_dataframe(table_df, table_name, target_dir, sample_fraction, stratify_col, split_fraction)
                            files_processed += 1
                        con.close()
                        continue # Skip the normal writing logic below for the .duckdb file itself
                    
                    if df is not None:
                        self._process_dataframe(df, file_path.stem, target_dir, sample_fraction, stratify_col, split_fraction)
                        files_processed += 1
                        success = True
                        
                except Exception as e:
                    rprint(f"[red]Error processing {file_path.name}: {e}[/red]")

        if files_processed > 0:
            rprint(f"[green]Successfully processed {files_processed} files for {dataset_id}[/green]")
            return True
        else:
            rprint(f"[yellow]No suitable files found to process for {dataset_id}[/yellow]")
            return False

    def _process_dataframe(
        self, 
        df: pl.DataFrame, 
        stem: str, 
        target_dir: Path, 
        sample_fraction: Optional[float] = None,
        stratify_col: Optional[str] = None,
        split_fraction: Optional[float] = None
    ):
        # 1. Sampling / Stratified Sampling
        if sample_fraction is not None:
            rprint(f"Sampling {sample_fraction*100}% of data...")
            if stratify_col and stratify_col in df.columns:
                rprint(f"Stratifying by: {stratify_col}")
                # Grouped sampling to preserve distribution
                df = df.group_by(stratify_col).map_groups(
                    lambda group: group.sample(fraction=sample_fraction)
                )
            else:
                df = df.sample(fraction=sample_fraction, shuffle=True)

        # 2. Splitting
        if split_fraction is not None:
            rprint(f"Splitting into train/test (train={split_fraction*100}%)...")
            # Shuffle first
            df = df.sample(fraction=1.0, shuffle=True)
            
            train_size = int(len(df) * split_fraction)
            df_train = df.slice(0, train_size)
            df_test = df.slice(train_size)
            
            train_file = target_dir / f"{stem}_train.parquet"
            test_file = target_dir / f"{stem}_test.parquet"
            
            df_train.write_parquet(train_file)
            df_test.write_parquet(test_file)
            rprint(f"Saved split files: {train_file.name}, {test_file.name}")
        else:
            target_file = target_dir / f"{stem}.parquet"
            rprint(f"Writing Parquet: {target_file.name}")
            df.write_parquet(target_file)

manager = ProcessingManager()
