from typing import Any
from retaildata.providers.dlt import DLTProvider
from retaildata.datasets.registry import Dataset

class RetailExpressProvider(DLTProvider):
    """
    Provider for Retail Express (SaaS ERP) using dlt.
    """
    def get_source(self, dataset: Dataset, **kwargs) -> Any:
        try:
            from dlt.sources.rest_api import rest_api_source
        except ImportError:
            raise ImportError("dlt is not installed. Please install it with: pip install \"retaildata[dlt]\"")

        api_key = kwargs.get("api_key")
        base_url = kwargs.get("base_url")
        
        if not api_key:
            raise ValueError("api_key is required for RetailExpressProvider")
        if not base_url:
            raise ValueError("base_url is required for RetailExpressProvider (e.g. https://your-instance.retailexpress.com.au)")

        # Simplified configuration for dlt's REST API source
        config = {
            "client": {
                "base_url": base_url,
                "auth": {
                    "type": "api_key",
                    "name": "x-api-key",
                    "location": "header",
                    "api_key": api_key,
                },
            },
            "resources": dataset.topic_tags.copy() if dataset.topic_tags else ["customers", "orders", "products"],
        }
        
        return rest_api_source(config)
