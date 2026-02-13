# Operational Data Connectors (dlt)

Milestone M7 introduces the `DLTProvider`, allowing `retaildata` to pull live operational data from external systems using [dlt (data load tool)](https://dlthub.com).

## Retail Express Integration

The primary operational source integrated is **Retail Express**, a professional SaaS ERP/POS system. This allows you to bridge your real inventory and order data directly into your ML training pipeline.

### Prerequisites

You must install the `dlt` optional dependency:
```bash
pip install "retaildata[dlt]"
```

### Usage

To download data from Retail Express, you need an API key and your instance URL.

```python
from retaildata.api import api

# Download and automatically prepare as Parquet
data = api.download(
    "retail_express",
    prepare=True,
    api_key="your_api_key",
    base_url="https://your-instance.retailexpress.com.au",
    topic_tags=["customers", "orders"] # Specific endpoints to pull
)

# Access tables
customers = data["customers"]
orders = data["orders"]

print(customers.head())
```

### Credential Management

You can store your Retail Express credentials securely:
```bash
retaildata credentials set --service retail_express --username your-instance --password your_api_key
```

### Unified Pipeline

Because `dlt` sources are integrated into the standard `prepare` workflow, you can use sampling, splitting, and ML tensor conversion just like any other dataset:

```python
# Pull operational data, sample it, and convert to PyTorch
prepared_data = api.download(
    "retail_express",
    prepare=True,
    sample_fraction=0.1,
    split_fraction=0.8
)

# Convert to PyTorch
from retaildata.integrations.ml import ml
torch_tensors = ml.to_pytorch(prepared_data["customers_train"])
```
