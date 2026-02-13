import retaildata.api as rd
import polars as pl
from pathlib import Path
from rich import print as rprint

def main():
    rprint("[bold blue]RetailData Expanded Benchmark Demo[/bold blue]")
    
    # Path to our test data
    base_dir = Path("./test_data_hf")
    
    # 1. Inspect prepared data for store_sales (Favorita)
    prepared_dir = base_dir / "prepared" / "store_sales"
    rprint(f"\n--- 1. Inspecting 'store_sales' (Favorita) at {prepared_dir} ---")
    
    if not prepared_dir.exists():
        rprint("[red]Prepared data not found. Please run: retaildata get store_sales --prepare --output ./test_data_hf[/red]")
        return

    # Load transactions and stores
    try:
        transactions = pl.read_parquet(prepared_dir / "transactions.parquet")
        stores = pl.read_parquet(prepared_dir / "stores.parquet")
        
        rprint("\n[bold]Transactions Schema:[/bold]")
        print(transactions.collect_schema())
        
        rprint("\n[bold]Stores Schema:[/bold]")
        print(stores.collect_schema())
        
        # 2. Polars Join & Aggregation
        rprint("\n--- 2. Joining Transactions with Store Metadata ---")
        df_merged = transactions.join(stores, on="store_nbr")
        
        # Aggregate by city and type
        city_sales = df_merged.group_by(["city", "type"]).agg(
            pl.col("transactions").sum().alias("total_transactions")
        ).sort("total_transactions", descending=True)
        
        rprint("\n[bold]Top Cities by Transactions:[/bold]")
        print(city_sales.head(10))
        
        # 3. Simple Time Series Analysis
        rprint("\n--- 3. Monthly Transaction Trends ---")
        # Convert date to datetime if it's string
        transactions = transactions.with_columns(
            pl.col("date").str.to_date()
        )
        
        monthly_trends = transactions.group_by_dynamic("date", every="1mo").agg(
            pl.col("transactions").sum().alias("monthly_transactions")
        ).sort("date")
        
        rprint("\n[bold]Monthly Trends (First 5 months):[/bold]")
        print(monthly_trends.head(5))

    except Exception as e:
        rprint(f"[red]Error loading/processing data: {e}[/red]")

if __name__ == "__main__":
    main()
