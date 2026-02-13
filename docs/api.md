# API Reference

## `RetailDataAPI`

The main entry point for the library.

### `download(dataset_id, data_dir=None, prepare=False, lazy=False, ...)`
Downloads and optionally prepares a dataset. Returns `None` OR a dictionary of frames if `prepare=True`.

### `load(dataset_id, data_dir=None, lazy=False)`
Loads already prepared Parquet files for a dataset.

### `list_datasets()`
Returns a list of all registered `Dataset` objects.

## `MLIntegrator` (via `ml` singleton)

### `to_pytorch(data)`
### `to_tensorflow(data)`
### `to_jax(data)`
Each method accepts a Polars DataFrame or Series and returns the corresponding framework tensor.
