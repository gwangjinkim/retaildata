# RetailData

A unified interface for fetching and preparing retail datasets for benchmarking and analysis.

## Features

- **Unified API**: Fetch datasets from various providers (HTTP, Kaggle, Hugging Face, UCI, OpenML) with a single command.
- **Secure Credentials**: Integrated support for Kaggle and Hugging Face API keys.
- **Data Benchmark Pack**: Curated retail datasets (Favorita, Rossmann, Instacart, M5, Olist, and more).
- **Processing Pipeline**: Automatic conversion to high-performance Parquet optimized for Polars.
- **Cache Management**: Programmatic disk usage tracking and clearing.

## Installation

```bash
pip install retaildata
```

Or using `uv` (recommended for development):
```bash
uv pip install -e .
```

## Quick Start

### CLI

1. **List available datasets**:
   ```bash
   retaildata list
   ```

2. **Download a dataset**:
   ```bash
   retaildata get test_http
   ```

3. **Download with Preparation (Parquet)**:
   ```bash
   retaildata get online_retail_ii --prepare
   ```

4. **Manage Credentials (e.g. Kaggle)**:
   ```bash
   retaildata auth set kaggle --file ~/.kaggle/kaggle.json
   ```

5. **Clean Up**:
   ```bash
   retaildata rm test_http
   retaildata purge --all
   ```

### Python API

```python
import retaildata.api as rd
import polars as pl
from pathlib import Path

# Download and prepare dataset
rd.api.download("online_retail_ii", prepare=True)

# Load efficiently with Polars
df = pl.scan_parquet("~/.local/share/retaildata/prepared/online_retail_ii/*.parquet").collect()
print(df.head())
```

## Supported Datasets

- `online_retail_ii`: UK-based online retail transactions.
- `olist`: Brazilian e-commerce dataset.
- `m5`: Walmart time-series forecasting.
- `store_sales`: Corporación Favorita (Ecuador) store sales.
- `rossmann`: Rossmann store sales benchmarks.
- `instacart`: Online grocery basket analysis.
- `online_retail_uci`: Classical transactions dataset (UCI).
- `credit_approval_openml`: Financial benchmarking (OpenML).

See `retaildata list` for the full registry.

## License

This package is licensed under the MIT License. Individual datasets may have their own licenses.
