# Getting Started

## Installation

```bash
pip install retaildata
```

For ML integrations:

```bash
pip install "retaildata[all]"  # Installs PyTorch, TensorFlow, and JAX support
```

## Basic Usage

### CLI

```bash
# List available datasets
retaildata list

# Download and prepare a dataset
retaildata get bank_marketing_uci --prepare
```

### Python API

```python
from retaildata.api import api

# Download and load as DataFrames
data = api.download("bank_marketing_uci", prepare=True)
features = data["features"]
print(features.head())
```
