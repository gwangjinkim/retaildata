import polars as pl
from typing import Any, Union, Optional

class MLIntegrator:
    """
    Utilities for converting Polars data structures to common ML framework tensors.
    """
    
    @staticmethod
    def to_pytorch(data: Union[pl.DataFrame, pl.Series]):
        """
        Convert to PyTorch Tensor.
        """
        try:
            import torch
            # Polars to numpy to torch is the most stable path
            if isinstance(data, pl.DataFrame):
                return torch.from_numpy(data.to_numpy())
            return torch.from_numpy(data.to_numpy())
        except ImportError:
            raise ImportError("PyTorch not installed. Install with 'pip install torch' or 'pip install retaildata[torch]'.")

    @staticmethod
    def to_tensorflow(data: Union[pl.DataFrame, pl.Series]):
        """
        Convert to TensorFlow Tensor.
        """
        try:
            import tensorflow as tf
            if isinstance(data, pl.DataFrame):
                return tf.convert_to_tensor(data.to_numpy())
            return tf.convert_to_tensor(data.to_numpy())
        except ImportError:
            raise ImportError("TensorFlow not installed. Install with 'pip install tensorflow' or 'pip install retaildata[tf]'.")

    @staticmethod
    def to_jax(data: Union[pl.DataFrame, pl.Series]):
        """
        Convert to JAX array.
        """
        try:
            import jax.numpy as jnp
            if isinstance(data, pl.DataFrame):
                return jnp.array(data.to_numpy())
            return jnp.array(data.to_numpy())
        except ImportError:
            raise ImportError("JAX not installed. Install with 'pip install jax jaxlib' or 'pip install retaildata[jax]'.")

# Singleton instance for easy access
ml = MLIntegrator()
