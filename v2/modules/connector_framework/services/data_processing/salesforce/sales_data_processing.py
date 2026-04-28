"""
Sales Data Processing for Salesforce
Handles processing of Sales subject area objects:
- Account
- Opportunity
"""
import pandas as pd
from typing import Dict, Any, List, Union, Optional
from v2.modules.connector_framework.salesforce.common_utils import (
    normalize_salesforce_data,
    convert_date_fields,
    clean_dataframe
)


def process_account_data(
    raw: Union[List[Dict[str, Any]], Dict[str, Any], pd.DataFrame, str],
    date_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Process Account data from Salesforce API response.
    
    Account is a simple object with no nested structures.
    Main processing includes:
    - Normalizing data format
    - Converting date fields
    - Cleaning system columns
    
    Args:
        raw: Raw API response (can be list, dict, DataFrame, or CSV string from Bulk API)
        date_columns: Optional list of date column names to convert. If None, uses defaults.
        
    Returns:
        pandas DataFrame with processed Account data
    """
    # Normalize to DataFrame
    df = normalize_salesforce_data(raw)
    
    if df.empty:
        return df
    
    # Convert date fields (Account has CreatedDate, LastModifiedDate, etc.)
    if date_columns is None:
        date_columns = ['CreatedDate', 'LastModifiedDate', 'SystemModstamp']
    
    df = convert_date_fields(df, date_columns)
    
    # Clean the DataFrame
    df = clean_dataframe(df)
    
    return df


def process_opportunity_data(
    raw: Union[List[Dict[str, Any]], Dict[str, Any], pd.DataFrame, str],
    date_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Process Opportunity data from Salesforce API response.
    
    Opportunity is a simple object with no nested structures.
    Main processing includes:
    - Normalizing data format
    - Converting date fields (especially CloseDate)
    - Cleaning system columns
    
    Args:
        raw: Raw API response (can be list, dict, DataFrame, or CSV string from Bulk API)
        date_columns: Optional list of date column names to convert. If None, uses defaults.
        
    Returns:
        pandas DataFrame with processed Opportunity data
    """
    # Normalize to DataFrame
    df = normalize_salesforce_data(raw)
    
    if df.empty:
        return df
    
    # Convert date fields (Opportunity has CreatedDate, CloseDate, etc.)
    if date_columns is None:
        date_columns = ['CreatedDate', 'CloseDate', 'LastModifiedDate', 'SystemModstamp']
    
    df = convert_date_fields(df, date_columns)
    
    # Clean the DataFrame
    df = clean_dataframe(df)
    
    return df
