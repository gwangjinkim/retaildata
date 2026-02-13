import typer
import rich
from rich import print as rprint
from rich.console import Console
from rich.table import Table
from typing import Optional
from pathlib import Path
from retaildata.datasets.registry import Registry, Dataset
from retaildata.api import RetailDataAPI
from retaildata.config import settings

app = typer.Typer(help="RetailData: Unified dataset fetcher for retail datasets.")
console = Console()
api = RetailDataAPI()

@app.command(name="list")
def list_datasets(
    downloaded: bool = typer.Option(False, "--downloaded", "-d", help="Show only downloaded datasets"),
):
    """List all available datasets."""
    from retaildata.cache.manager import manager as cache_manager
    
    datasets = api.list_datasets()
    if not datasets:
        console.print("[yellow]No datasets found in registry.[/yellow]")
        return
    
    table = Table(title="Available Datasets")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Provider", style="magenta")
    table.add_column("Tags", style="green")
    table.add_column("Status", style="blue")
    table.add_column("Size", style="yellow")
    
    for ds in datasets:
        is_downloaded = cache_manager.is_downloaded(ds.id)
        if downloaded and not is_downloaded:
            continue
            
        status = "[green]Downloaded[/green]" if is_downloaded else "[dim]Remote[/dim]"
        size_str = ""
        if is_downloaded:
            size_bytes = cache_manager.get_size(ds.id)
            # Simple human readable
            if size_bytes < 1024:
                size_str = f"{size_bytes} B"
            elif size_bytes < 1024**2:
                size_str = f"{size_bytes/1024:.1f} KB"
            elif size_bytes < 1024**3:
                size_str = f"{size_bytes/1024**2:.1f} MB"
            else:
                size_str = f"{size_bytes/1024**3:.1f} GB"
        
        table.add_row(
            ds.id,
            ds.provider,
            ", ".join(ds.topic_tags),
            status,
            size_str
        )
    console.print(table)

@app.command(name="info")
def info(dataset_id: str):
    """Show details for a specific dataset."""
    dataset = api.get_dataset(dataset_id)
    if not dataset:
        console.print(f"[red]Dataset '{dataset_id}' not found.[/red]")
        raise typer.Exit(code=1)
    
    console.print(f"[bold cyan]Dataset ID:[/bold cyan] {dataset.id}")
    console.print(f"[bold]Provider:[/bold] {dataset.provider}")
    console.print(f"[bold]Tags:[/bold] {', '.join(dataset.topic_tags)}")
    console.print(f"[bold]Requires Credentials:[/bold] {'Yes' if dataset.requires_credentials else 'No'}")
    if dataset.license_notes:
        console.print(f"[bold]License:[/bold] {dataset.license_notes}")
    if dataset.description:
        console.print(f"[bold]Description:[/bold] {dataset.description}")

@app.command(name="get")
def get(
    dataset_id: str,
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o", help="Custom output directory"),
    cache: bool = typer.Option(True, help="Use cached version if available (default: on)"),
    prepare: bool = typer.Option(False, "--prepare", "-p", help="Convert to Parquet after download"),
    sample: Optional[float] = typer.Option(None, "--sample", help="Fraction of data to sample (0.0 to 1.0)"),
    stratify: Optional[str] = typer.Option(None, "--stratify", help="Column name for stratified sampling"),
    split: Optional[float] = typer.Option(None, "--split", help="Fraction for train/test split (e.g. 0.8)")
):
    """Download a dataset."""
    try:
        api.download(
            dataset_id, 
            data_dir=output_dir, 
            cache=cache, 
            prepare=prepare,
            sample_fraction=sample,
            stratify_col=stratify,
            split_fraction=split
        )
        console.print(f"[bold green]Successfully processed dataset '{dataset_id}'[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)

# Stub commands for future milestones
@app.command()
def inspect(
    dataset_id: str,
    prepared: bool = typer.Option(False, "--prepared", help="Inspect prepared Parquet data instead of raw files"),
    output: Optional[Path] = typer.Option(None, "--output", help="Custom data directory")
):
    """
    Explore a downloaded dataset: schema, stats, and validation.
    """
    dataset = Registry.get(dataset_id)
    if not dataset:
        rprint(f"[red]Dataset '{dataset_id}' not found in registry.[/red]")
        raise typer.Exit(1)

    base_dir = output or settings.final_data_dir
    subdir = "prepared" if prepared else "raw"
    data_path = base_dir / subdir / dataset_id
    
    if not data_path.exists():
        rprint(f"[red]Data for '{dataset_id}' not found at {data_path}[/red]")
        rprint(f"Try running: [bold]retaildata get {dataset_id}[/bold]")
        raise typer.Exit(1)

    import polars as pl
    from rich.table import Table

    rprint(f"\n[bold blue]Inspecting Dataset:[/bold blue] {dataset_id} ({subdir})")
    rprint(f"[dim]Path: {data_path}[/dim]\n")

    # Find files to inspect
    files = list(data_path.rglob("*"))
    files = [f for f in files if f.is_file() and f.suffix.lower() in [".csv", ".parquet", ".xlsx", ".xls"]]
    
    if not files:
        rprint("[yellow]No suitable data files found to inspect.[/yellow]")
        return

    for file_path in files:
        rprint(f"[bold underline]File: {file_path.name}[/bold underline]")
        try:
            # Read a sample for inspection
            if file_path.suffix.lower() == ".parquet":
                df = pl.read_parquet(file_path) # Parquet is fast enough
            elif file_path.suffix.lower() == ".csv":
                df = pl.read_csv(file_path, n_rows=1000) # Only first 1000 for raw
            else:
                df = pl.read_excel(file_path)
            
            # Schema Table
            schema_table = Table(title="Schema")
            schema_table.add_column("Column")
            schema_table.add_column("DType")
            schema_table.add_column("Status")
            
            actual_schema = df.collect_schema()
            expected = dataset.expected_schema or {}
            
            for col, dtype in actual_schema.items():
                status = "[green]OK[/green]"
                if col in expected:
                    if str(dtype) != expected[col]:
                        status = f"[yellow]Expected {expected[col]}[/yellow]"
                schema_table.add_row(col, str(dtype), status)
            
            rprint(schema_table)
            
            # Basic Stats
            rprint(f"[bold]Shape:[/bold] {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Descriptive stats for numeric columns
            numeric_cols = [c for c, t in actual_schema.items() if t in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]]
            if numeric_cols:
                rprint("\n[bold]Numerical Summary:[/bold]")
                print(df.select(numeric_cols).describe())
            
            rprint("\n" + "-"*40 + "\n")

        except Exception as e:
            rprint(f"[red]Error inspecting {file_path.name}: {e}[/red]")

@app.command()
def auth(
    action: str = typer.Argument(..., help="Action: set, status, rm"),
    provider: str = typer.Argument(..., help="Provider name (e.g., kaggle)"),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="Path to credential file (e.g. kaggle.json)"),
):
    """Manage credentials for providers."""
    from retaildata.credentials.manager import manager
    import getpass
    import json

    if action == "set":
        console.print(f"[bold]Setting credentials for {provider}...[/bold]")
        if provider.lower() == "kaggle":
            username = None
            key = None
            
            # 1. Try provided file
            if file:
                if not file.exists():
                    console.print(f"[red]File not found: {file}[/red]")
                    raise typer.Exit(code=1)
                try:
                    with open(file, "r") as f:
                        data = json.load(f)
                        username = data.get("username")
                        key = data.get("key")
                except Exception as e:
                    console.print(f"[red]Error reading file: {e}[/red]")
                    raise typer.Exit(code=1)
            
            # 2. Search in default location if not yet found
            if not username and not key:
                default_path = Path.home() / ".kaggle" / "kaggle.json"
                if default_path.exists():
                    if typer.confirm(f"Found existing credentials at {default_path}. Import them?"):
                        try:
                            with open(default_path, "r") as f:
                                data = json.load(f)
                                username = data.get("username")
                                key = data.get("key")
                        except Exception as e:
                            console.print(f"[red]Error reading default file: {e}[/red]")

            # 3. Interactive prompt if still missing
            if not username or not key:
                console.print("[yellow]Enter credentials manually:[/yellow]")
                username = typer.prompt("Kaggle Username")
                key = typer.prompt("Kaggle Key", hide_input=True)

            if username and key:
                manager.set_credential("kaggle", "username", username)
                manager.set_credential("kaggle", "key", key)
                console.print(f"[green]Credentials for {provider} set successfully (username: {username}).[/green]")
            else:
                 console.print(f"[red]Failed to obtain valid credentials.[/red]")
                 raise typer.Exit(code=1)

        elif provider.lower() == "hf":
            token = typer.prompt("Hugging Face Token", hide_input=True)
            if token:
                manager.set_credential("hf", "token", token)
                console.print(f"[green]Credentials for {provider} set successfully.[/green]")
            else:
                 console.print(f"[red]Failed to obtain valid token.[/red]")
                 raise typer.Exit(code=1)
        else:
            console.print(f"[red]Unknown provider: {provider}[/red]")
            raise typer.Exit(code=1)

    elif action == "status":
        if provider.lower() == "kaggle":
             # Check if we have them
             u = manager.get_credential("kaggle", "username")
             k = manager.get_credential("kaggle", "key")
             if u and k:
                 console.print(f"[green]Credentials for {provider} are present (username: {u}).[/green]")
             else:
                 console.print(f"[yellow]Credentials for {provider} are missing.[/yellow]")
        else:
             console.print(f"[red]Unknown provider: {provider}[/red]")

    elif action == "rm":
        if provider.lower() == "kaggle":
            manager.delete_credential("kaggle", "username")
            manager.delete_credential("kaggle", "key")
            console.print(f"[green]Credentials for {provider} removed.[/green]")
        elif provider.lower() == "hf":
            manager.delete_credential("hf", "token")
            console.print(f"[green]Credentials for {provider} removed.[/green]")
        else:
            console.print(f"[red]Unknown provider: {provider}[/red]")

    else:
        console.print(f"[red]Unknown action: {action}[/red]")
        raise typer.Exit(code=1)

@app.command()
def rm(dataset_id: str):
    """Remove a dataset from the local cache."""
    from retaildata.cache.manager import manager as cache_manager
    
    if not cache_manager.is_downloaded(dataset_id):
        console.print(f"[yellow]Dataset '{dataset_id}' is not downloaded.[/yellow]")
        return

    if typer.confirm(f"Are you sure you want to delete dataset '{dataset_id}'?"):
        if cache_manager.delete_dataset(dataset_id):
            console.print(f"[green]Dataset '{dataset_id}' removed successfully.[/green]")
        else:
            console.print(f"[red]Failed to remove some files for '{dataset_id}'.[/red]")

@app.command()
def purge(
    all: bool = typer.Option(False, "--all", help="Remove all datasets"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
):
    """Purge all data."""
    from retaildata.cache.manager import manager as cache_manager
    
    if not all:
        console.print("[yellow]Please specify --all to purge everything.[/yellow]")
        raise typer.Exit(code=1)

    if not yes:
        if not typer.confirm("Are you sure you want to delete ALL downloaded datasets? This cannot be undone."):
            raise typer.Exit()

    cache_manager.purge_all()
    console.print("[green]All datasets purged successfully.[/green]")

if __name__ == "__main__":
    app()
