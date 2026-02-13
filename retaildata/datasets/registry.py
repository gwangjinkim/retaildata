from typing import List, Optional, Dict, Any
from pydantic import BaseModel

class Dataset(BaseModel):
    id: str
    topic_tags: List[str]
    provider: str  # 'kaggle', 'http', 'hf', 'uci', 'openml'
    requires_credentials: bool = False
    license_notes: Optional[str] = None
    description: Optional[str] = None
    
    # Provider-specific config
    url: Optional[str] = None  # Single file URL
    urls: Optional[List[str]] = None # Multiple file URLs for parallel download
    hf_repo_id: Optional[str] = None # For HF provider
    kaggle_id: Optional[str] = None # For Kaggle provider
    uci_id: Optional[int] = None # For UCI provider
    openml_id: Optional[int] = None # For OpenML provider
    
    # M8: Bayesian Data Utilities
    standard_mapping: Optional[Dict[str, str]] = None # e.g. {"sales": "sales_train_eval", "calendar": "calendar"}
    hierarchies: Optional[List[List[str]]] = None # e.g. [["item_id", "dept_id"], ["dept_id", "cat_id"]]
    intervention_windows: Optional[Dict[str, Dict[str, str]]] = None # e.g. {"covid": {"start": "2020-03-01", "end": "2020-06-01"}}
    
    # Post-processing
    prepare_script: Optional[str] = None

class Registry:
    _datasets: Dict[str, Dataset] = {}

    @classmethod
    def register(cls, dataset: Dataset):
        cls._datasets[dataset.id] = dataset

    @classmethod
    def get(cls, dataset_id: str) -> Optional[Dataset]:
        return cls._datasets.get(dataset_id)

    @classmethod
    def list_all(cls) -> List[Dataset]:
        return list(cls._datasets.values())

# Initial seed of datasets (M0)
# Adding a test dataset for HTTP provider
Registry.register(Dataset(
    id="test_http",
    topic_tags=["test"],
    provider="http",
    requires_credentials=False,
    description="A simple test dataset downloaded via HTTP.",
    url="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv", # Example public CSV
    license_notes="Public Domain"
))

# M6: Multi-file test dataset for parallel downloads
Registry.register(Dataset(
    id="test_multi",
    topic_tags=["test"],
    provider="http",
    requires_credentials=False,
    description="A test dataset with multiple files for parallel download.",
    urls=[
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv",
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv",
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/planets.csv"
    ],
    license_notes="Public Domain"
))

Registry.register(Dataset(
    id="titanic",
    topic_tags=["test", "classification"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="heptapod/titanic", # Using a known small mirror or the official competition one
    description="Titanic dataset (test).",
    license_notes="Various"
))

# M3: Retail Benchmark Pack

Registry.register(Dataset(
    id="online_retail_ii",
    topic_tags=["retail", "transactions", "uk"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="mashlyn/online-retail-ii-uci",
    description="Online Retail II Data Set contains all the transactions occurring for a UK-based and registered, non-store online retail between 01/12/2009 and 09/12/2011.",
    license_notes="UCI Machine Learning Repository"
))

Registry.register(Dataset(
    id="olist",
    topic_tags=["ecommerce", "brazil", "marketplace"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="olistbr/brazilian-ecommerce",
    description="Brazilian E-Commerce Public Dataset by Olist. 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil.",
    license_notes="CC BY-NC-SA 4.0"
))

Registry.register(Dataset(
    id="m5",
    topic_tags=["forecasting", "walmart", "sales"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="c/m5-forecasting-accuracy",
    description="M5 Forecasting - Accuracy. Estimate the unit sales of Walmart retail goods.",
    license_notes="Kaggle Competition Rules",
    standard_mapping={
        "sales": "sales_train_evaluation",
        "calendar": "calendar",
        "prices": "sell_prices"
    },
    hierarchies=[
        ["item_id", "dept_id"],
        ["dept_id", "cat_id"],
        ["store_id", "state_id"]
    ]
))

Registry.register(Dataset(
    id="superstore",
    topic_tags=["retail", "sales", "tableau"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="vivek468/superstore-dataset-final",
    description="Superstore Dataset. Retail dataset of a global superstore for 4 years.",
    license_notes="Unknown/Public"
))

# Extended Benchmark Pack

# A) Hierarchical demand forecasting
# M5 is already registered

# B) Promo & price uplift
Registry.register(Dataset(
    id="store_sales",
    topic_tags=["forecasting", "retail", "promo", "ecuador"],
    provider="hf",
    requires_credentials=False, # Public mirror
    hf_repo_id="t4tiana/store-sales-time-series-forecasting",
    description="Corporación Favorita Store Sales forecasting. Includes promo flags and store/product metadata.",
    license_notes="Kaggle Competition",
    standard_mapping={
        "sales": "train",
        "calendar": "holidays_events",
        "prices": "oil" # Proxy for external economic factors in this dataset
    },
    hierarchies=[
        ["family", "class"], # class isn't in main train usually but in metadata
        ["store_nbr", "cluster"]
    ],
    intervention_windows={
        "earthquake": {"start": "2016-04-16", "end": "2016-05-16"}
    }
))

Registry.register(Dataset(
    id="rossmann",
    topic_tags=["forecasting", "retail", "promo", "germany"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="c/rossmann-store-sales",
    description="Rossmann Store Sales. Store-level daily sales with promo/holiday/store metadata.",
    license_notes="Kaggle Competition",
    standard_mapping={
        "sales": "train",
        "stores": "store"
    },
    intervention_windows={
        "refurbishment": {"start": "2014-07-01", "end": "2014-12-31"} # Example for specific stores
    }
))

# C) Scanner / price elasticity
# Note: Dominick's requires registration/academic access, skipping for auto-download for now.

# D) Customer journey / loyalty / baskets
Registry.register(Dataset(
    id="instacart",
    topic_tags=["basket", "ecommerce", "recommendation"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="c/instacart-market-basket-analysis",
    description="Instacart Online Grocery Shopping Dataset 2017. Huge basket dataset (orders + products).",
    license_notes="Instacart Open Source License"
))

Registry.register(Dataset(
    id="dunnhumby_journey",
    topic_tags=["transactions", "loyalty", "clv"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="mansy5/dunnhumby-the-complete-journey",
    description="dunnhumby - The Complete Journey. Customer-level transactions + products + store context.",
    license_notes="dunnhumby"
))

# E) Inventory demand
Registry.register(Dataset(
    id="grupo_bimbo",
    topic_tags=["supply-chain", "inventory", "forecasting"],
    provider="kaggle",
    requires_credentials=True,
    kaggle_id="c/grupo-bimbo-inventory-demand",
    description="Grupo Bimbo Inventory Demand. Weekly sales and inventory demand across stores.",
    license_notes="Kaggle Competition"
))

# M4: Extended Providers (UCI & OpenML)

# UCI Datasets
Registry.register(Dataset(
    id="online_retail_uci",
    topic_tags=["retail", "transactions", "uci"],
    provider="uci",
    uci_id=352,
    description="Online Retail Data Set (UCI). Transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.",
    license_notes="Public Domain / UCI"
))

Registry.register(Dataset(
    id="bank_marketing_uci",
    topic_tags=["marketing", "banking", "uci"],
    provider="uci",
    uci_id=222,
    description="Bank Marketing Data Set (UCI). Related with direct marketing campaigns of a Portuguese banking institution.",
    license_notes="Public Domain / UCI",
    expected_schema={
        "age": "Int64",
        "job": "String",
        "marital": "String",
        "education": "String",
        "default": "String"
    }
))

# OpenML Datasets
Registry.register(Dataset(
    id="credit_approval_openml",
    topic_tags=["finance", "classification", "openml"],
    provider="openml",
    openml_id=29, # Credit Approval
    description="Credit Approval dataset from OpenML.",
    license_notes="Public Domain / OpenML"
))

# M7: Retail Express (DLT Source)
Registry.register(Dataset(
    id="retail_express",
    topic_tags=["customers", "orders", "products", "outlets"],
    provider="dlt",
    requires_credentials=True,
    description="Operational retail data from Retail Express via dlt.",
    license_notes="Proprietary (API Key Required)"
))
