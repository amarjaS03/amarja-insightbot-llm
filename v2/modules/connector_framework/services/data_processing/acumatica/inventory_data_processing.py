"""
Inventory Data Processing for Acumatica
Handles processing of Inventory subject area entities:
- StockItem
- ItemWarehouse
- InventoryReceipt
"""
import pandas as pd
from typing import Dict, Any, List, Union, Tuple
from v2.modules.connector_framework.services.data_processing.acumatica.common_utils import (
    flatten_stock_items_data,
    _to_list_of_dicts,
    _remove_system_columns
)


def process_stock_item_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process StockItem data from Acumatica API response.
    Returns separate DataFrames for stock items and warehouse details (matching cloud function behavior).
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Tuple of (stock_items_df, warehouse_df) - both pandas DataFrames
        - stock_items_df: Main stock item data
        - warehouse_df: Warehouse details (can be empty if no warehouse data)
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df, df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # Stock items subset (only columns that exist)
    desired_stock_cols = [
        'InventoryID', 'Description', 'ItemClass', 'BaseUOM', 'DefaultPrice',
        'DefaultWarehouseID', 'ItemType', 'ItemStatus', 'IsAKit'
    ]
    stock_cols = [c for c in desired_stock_cols if c in df.columns]
    stock_items_df = df[stock_cols].copy() if stock_cols else pd.DataFrame()
    
    # Remove system columns from stock items
    stock_items_df = _remove_system_columns(stock_items_df)

    # Warehouse details: detect any WarehouseDetails_<index>_<Field> columns
    warehouse_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('WarehouseDetails_')]
    warehouse_df = pd.DataFrame()
    
    if warehouse_cols and 'InventoryID' in df.columns:
        base = df[['InventoryID'] + warehouse_cols].copy()
        melted = base.melt(id_vars=['InventoryID'], value_vars=warehouse_cols, var_name='WarehouseInfo', value_name='Value')
        extracted = melted['WarehouseInfo'].str.extract(r'WarehouseDetails_(\d+)_(.+)')
        melted['WarehouseIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(
            index=['InventoryID', 'WarehouseIndex'],
            columns='Field',
            values='Value',
            aggfunc='first'
        ).reset_index()
        
        rename_map = {
            'QtyOnHand': 'QtyOnHand',
            'WarehouseID': 'WarehouseID',
            'ReplenishmentWarehouse': 'ReplenishmentWarehouse'
        }
        pivoted = pivoted.rename(columns=rename_map)
        final_cols = ['InventoryID'] + [c for c in ['QtyOnHand', 'WarehouseID', 'ReplenishmentWarehouse'] if c in pivoted.columns]
        warehouse_df = pivoted[final_cols].copy()
        warehouse_df = _remove_system_columns(warehouse_df)
    
    return stock_items_df, warehouse_df


def process_item_warehouse_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process ItemWarehouse data from Acumatica API response.
    Simple entity with no expansions - returns a single DataFrame.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        pandas DataFrame with flattened ItemWarehouse data
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
    
    # Extract relevant fields
    desired_cols = ['InventoryID', 'WarehouseID', 'QtyOnHand', 'QtyAvailable', 'ReplenishmentSource']
    cols = [c for c in desired_cols if c in df.columns]
    item_warehouse_df = df[cols].copy() if cols else df.copy()
    
    # Remove system/internal columns
    item_warehouse_df = _remove_system_columns(item_warehouse_df)
    
    return item_warehouse_df


def process_inventory_receipt_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process InventoryReceipt data from Acumatica API response.
    Simple entity with no expansions - returns a single DataFrame.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        pandas DataFrame with flattened InventoryReceipt data
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
    
    # Extract relevant fields
    desired_cols = ['ReferenceNbr', 'Date', 'Status', 'TotalQty', 'TotalCost']
    cols = [c for c in desired_cols if c in df.columns]
    inventory_receipt_df = df[cols].copy() if cols else df.copy()
    
    # Remove system/internal columns
    inventory_receipt_df = _remove_system_columns(inventory_receipt_df)
    
    return inventory_receipt_df
