# ML Integrations

`retaildata` provides seamless conversion to major ML frameworks.

## PyTorch

```python
from retaildata.integrations.ml import ml
import polars as pl

series = pl.Series([1.0, 2.0, 3.0])
tensor = ml.to_pytorch(series)
```

## TensorFlow

```python
tensor = ml.to_tensorflow(series)
```

## JAX

```python
array = ml.to_jax(series)
```
