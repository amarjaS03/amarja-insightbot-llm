"""
Financial Data Processing for Acumatica
Handles processing of Financial subject area entities:
- Customer
- Vendor
- JournalTransaction
"""
import pandas as pd
from typing import Dict, Any, List, Union
from v2.modules.connector_framework.services.data_processing.acumatica.common_utils import (
    flatten_stock_items_data,
    _to_list_of_dicts,
    _remove_system_columns
)


def process_customer_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process Customer data from Acumatica API response.
    Simple entity with no expansions - returns a single DataFrame.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        pandas DataFrame with flattened Customer data
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    
    if not records:
        return pd.DataFrame()
    
    # Flatten each record
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df
    
    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)
    
    # Remove system/internal columns
    df = _remove_system_columns(df)
    
    return df


def process_vendor_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process Vendor data from Acumatica API response.
    Simple entity with no expansions - returns a single DataFrame.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        pandas DataFrame with flattened Vendor data
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    
    if not records:
        return pd.DataFrame()
    
    # Flatten each record
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df
    
    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)
    
    # Remove system/internal columns
    df = _remove_system_columns(df)
    
    return df


def process_journal_transaction_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process JournalTransaction data from Acumatica API response.
    Simple entity with no expansions - returns a single DataFrame.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        pandas DataFrame with flattened JournalTransaction data
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    
    if not records:
        return pd.DataFrame()
    
    # Flatten each record
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df
    
    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)
    
    # Remove system/internal columns
    df = _remove_system_columns(df)
    
    return df
