import pytest
import polars as pl
import numpy as np
from unittest.mock import MagicMock, patch
from retaildata.integrations.ml import ml

def test_to_pytorch_series_import_error():
    series = pl.Series("test", [1, 2, 3])
    # Ensure torch is NOT in sys.modules to trigger ImportError
    with patch.dict("sys.modules", {"torch": None}):
        with pytest.raises(ImportError):
            ml.to_pytorch(series)

def test_to_pytorch_with_mock():
    series = pl.Series("test", [1, 2, 3])
    # Mocking the torch module
    mock_torch = MagicMock()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        ml.to_pytorch(series)
        mock_torch.from_numpy.assert_called_once()

def test_to_tensorflow_with_mock():
    series = pl.Series("test", [1, 2, 3])
    mock_tf = MagicMock()
    with patch.dict("sys.modules", {"tensorflow": mock_tf}):
        ml.to_tensorflow(series)
        mock_tf.convert_to_tensor.assert_called_once()

def test_to_jax_with_mock():
    series = pl.Series("test", [1, 2, 3])
    mock_jax = MagicMock()
    # JAX uses jax.numpy
    with patch.dict("sys.modules", {"jax": mock_jax, "jax.numpy": mock_jax.numpy}):
        ml.to_jax(series)
        mock_jax.numpy.array.assert_called_once()
