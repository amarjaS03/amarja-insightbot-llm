"""
Common utilities for Acumatica data processing.
Shared functions for flattening and normalizing data across all subject areas.
"""
import json
import pandas as pd
from typing import Dict, Any, List, Union


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key for nested items
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Handle lists by converting to string or expanding
            if v and isinstance(v[0], dict):
                # If list contains dictionaries, create indexed entries
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", item))
            else:
                # Convert list to string
                items.append((new_key, json.dumps(v) if v else ''))
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_stock_items_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten stock items data for CSV export.
    
    Args:
        data: List of stock item dictionaries
        
    Returns:
        List of flattened dictionaries
    """
    flattened_data = []
    
    for item in data:
        flattened_item = flatten_dict(item)
        flattened_data.append(flattened_item)
    return flattened_data


def _to_list_of_dicts(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize API response into a list of dictionaries.
    
    Args:
        raw: Raw API response (can be list, dict, or dict with 'value' key)
        
    Returns:
        List of dictionaries
    """
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if isinstance(raw, dict):
        # OData style responses often use 'value'
        if isinstance(raw.get('value'), list):
            return [r for r in raw['value'] if isinstance(r, dict)]
        return [raw]
    return []


def _remove_system_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove system/internal columns that are automatically added by Acumatica API.
    These columns are not part of the selected fields and should be filtered out.
    
    Handles both original flattened patterns (id_BillToContact) and prefixed patterns
    after merge (BillToContact_id, Details_rowNumber, Details__links_files:put, etc.) for LLM clarity.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with system columns removed
    """
    if df.empty:
        return df
    
    # System/internal column names to remove
    system_column_names = ['id', 'note', 'rowNumber']
    
    # System column exact matches (main level)
    system_columns_exact = [
        '_links_files:put',
        'id',
        'note',
        'rowNumber'
    ]
    
    # System patterns to match anywhere in column name
    system_patterns = [
        '_links',  # Matches: _links_files:put, Details__links_files:put, etc.
    ]
    
    columns_to_remove = []
    for col in df.columns:
        col_str = str(col)
        
        # Exact match for system columns (main level)
        if col_str in system_columns_exact:
            columns_to_remove.append(col)
        # Columns starting with underscore (system fields like _links)
        elif col_str.startswith('_'):
            columns_to_remove.append(col)
        # Columns containing system patterns anywhere (e.g., Details__links_files:put)
        elif any(pattern in col_str for pattern in system_patterns):
            columns_to_remove.append(col)
        # Columns containing system column names with underscore (handles all patterns):
        # - Original flattening: id_BillToContact, rowNumber_Details
        # - Prefixed after merge: BillToContact_id, Details_rowNumber, ShipToAddress_id
        elif any(f'_{sys_col}' in col_str or f'{sys_col}_' in col_str for sys_col in system_column_names):
            columns_to_remove.append(col)
    
    # Remove the identified columns
    if columns_to_remove:
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
    
    return df


def generate_preview_data(df: pd.DataFrame, max_records: int = 10) -> dict:
    """
    Generate preview data with top_records from DataFrame.
    Copied from cloud function's dataProcessing.py to match v1 exactly.
    
    Args:
        df: pandas DataFrame to generate preview from
        max_records: Maximum number of records to include in preview (default: 10)
        
    Returns:
        Dictionary with 'top_records' key containing list of string-converted records
    """
    preview = {"top_records": []}
    if not df.empty:
        top_records = df.head(max_records).to_dict('records')
        for record in top_records:
            string_record = {}
            for key, value in record.items():
                if value is None or pd.isna(value):
                    string_record[str(key)] = "null"
                elif isinstance(value, (dict, list)):
                    string_record[str(key)] = str(value)
                else:
                    string_record[str(key)] = str(value)
            preview["top_records"].append(string_record)
    return preview