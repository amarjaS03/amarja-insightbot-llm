"""
Common utilities for Salesforce data processing.
Shared functions for normalizing and cleaning Salesforce data.
"""
import pandas as pd
from typing import Dict, Any, List, Union, Optional
from datetime import datetime


def normalize_salesforce_data(raw: Union[List[Dict[str, Any]], Dict[str, Any], pd.DataFrame]) -> pd.DataFrame:
    """
    Normalize Salesforce API response into a pandas DataFrame.
    
    Salesforce data is typically already flat, but this function handles:
    - List of dictionaries
    - Single dictionary
    - Already a DataFrame
    - CSV string (from Bulk API)
    
    Args:
        raw: Raw API response (can be list, dict, DataFrame, or CSV string)
        
    Returns:
        pandas DataFrame
    """
    if isinstance(raw, pd.DataFrame):
        return raw.copy()
    
    if isinstance(raw, str):
        # Handle CSV string from Bulk API
        import io
        return pd.read_csv(io.StringIO(raw))
    
    if isinstance(raw, dict):
        # Single record - convert to list
        raw = [raw]
    
    if isinstance(raw, list):
        if not raw:
            return pd.DataFrame()
        
        # Check if it's a list of dictionaries
        if isinstance(raw[0], dict):
            return pd.DataFrame(raw)
        else:
            return pd.DataFrame()
    
    return pd.DataFrame()


def convert_date_fields(df: pd.DataFrame, date_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Convert Salesforce date/datetime fields to proper pandas datetime types.
    
    Salesforce returns dates in ISO format strings. This function converts them
    to pandas datetime for better analysis.
    
    Args:
        df: DataFrame to process
        date_columns: Optional list of column names to convert. If None, auto-detect common Salesforce date fields.
        
    Returns:
        DataFrame with converted date columns
    """
    if df.empty:
        return df
    
    # Common Salesforce date/datetime fields
    common_date_fields = [
        'CreatedDate', 'LastModifiedDate', 'CloseDate', 'LastActivityDate',
        'SystemModstamp', 'LastViewedDate', 'LastReferencedDate',
        'CreatedById', 'LastModifiedById'  # These are IDs, not dates, but included for reference
    ]
    
    # Auto-detect date columns if not provided
    if date_columns is None:
        date_columns = [col for col in common_date_fields if col in df.columns]
    
    # Convert each date column
    for col in date_columns:
        if col in df.columns:
            try:
                # Try to convert to datetime
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                # If it's a date-only field (like CloseDate), convert to date
                if col in ['CloseDate', 'LastActivityDate']:
                    df[col] = df[col].dt.date
                    df[col] = pd.to_datetime(df[col])
            except Exception as e:
                # If conversion fails, leave as is
                pass
    
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Salesforce DataFrame by removing system fields and handling nulls.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        return df
    
    # Remove system/internal columns that are not useful for analysis
    system_columns = [
        'attributes',  # Salesforce metadata
        'Id',  # Keep Id for reference, but can be removed if needed
    ]
    
    # Remove system columns if they exist
    columns_to_remove = [col for col in system_columns if col in df.columns and col != 'Id']
    if columns_to_remove:
        df = df.drop(columns=columns_to_remove, errors='ignore')
    
    # Replace empty strings with None for consistency
    df = df.replace('', None)
    
    # Remove completely empty columns
    df = df.dropna(axis=1, how='all')
    
    return df
