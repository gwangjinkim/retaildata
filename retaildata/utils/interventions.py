from typing import Dict, Optional
from retaildata.datasets.registry import Registry

def get_intervention_windows(dataset_id: str) -> Dict[str, Dict[str, str]]:
    """
    Returns pre-defined intervention windows for a given dataset.
    Useful for Bayesian Causal Impact or Synthetic Control analysis.
    """
    dataset = Registry.get(dataset_id)
    if not dataset or not dataset.intervention_windows:
        return {}
        
    return dataset.intervention_windows

def get_intervention_mask(df, date_col: str, window_name: str, dataset_id: str):
    """
    Returns a boolean mask for the specified intervention window.
    """
    windows = get_intervention_windows(dataset_id)
    if window_name not in windows:
        return None
        
    win = windows[window_name]
    start = win["start"]
    end = win["end"]
    
    import polars as pl
    return (pl.col(date_col) >= pl.to_datetime(start)) & (pl.col(date_col) <= pl.to_datetime(end))
