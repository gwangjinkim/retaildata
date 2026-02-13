import polars as pl
from typing import List, Tuple, Dict
import numpy as np

def build_hierarchy_matrix(df: pl.DataFrame, levels: List[List[str]]) -> Dict[str, np.ndarray]:
    """
    Builds aggregation matrices for hierarchical time series.
    
    Args:
        df: The dataframe containing the IDs (e.g. item_id, dept_id, cat_id).
        levels: A list of levels where each level is [child_col, parent_col].
                Example: [["item_id", "dept_id"], ["dept_id", "cat_id"]]
    
    Returns:
        A dictionary containing mapping matrices (S matrices) or grouping indices.
    """
    results = {}
    
    for child, parent in levels:
        if child not in df.columns or parent not in df.columns:
            continue
            
        # Get unique mappings
        mapping = df.select([child, parent]).unique().sort(child)
        
        # Create a sparse-like representation or simply the mapping
        results[f"{child}_to_{parent}"] = mapping
        
    return results

def get_hierarchy_groups(df: pl.DataFrame, child_col: str, parent_col: str):
    """
    Returns an array of parent indices for each unique child, 
    useful for hierarchical indexing in PyMC/Stan.
    """
    # 1. Get unique children and parents
    unique_children = df.select(child_col).unique().sort(child_col)
    unique_parents = df.select(parent_col).unique().sort(parent_col)
    
    # 2. Map parents to integers
    parent_map = {name: i for i, name in enumerate(unique_parents[parent_col].to_list())}
    
    # 3. Join back to find parent index for each child
    mapping = unique_children.join(
        df.select([child_col, parent_col]).unique(), 
        on=child_col
    ).sort(child_col)
    
    parent_indices = [parent_map[p] for p in mapping[parent_col].to_list()]
    
    return {
        "parent_indices": np.array(parent_indices),
        "child_names": unique_children[child_col].to_list(),
        "parent_names": unique_parents[parent_col].to_list()
    }
