# Optimization

## Parallel Downloads

When a dataset has multiple files, `retaildata` automatically downloads them in parallel using a thread pool. This is currently supported for the HTTP provider via the `urls` registry field.

## Lazy Loading

To save memory when working with large datasets, use `lazy=True`:

```python
from retaildata.api import api

# Returns a dictionary of Polars LazyFrames
data = api.load("bank_marketing_uci", lazy=True)

# Data is only loaded/processed when you call .collect()
df = data["features"].filter(pl.col("age") > 30).collect()
```
