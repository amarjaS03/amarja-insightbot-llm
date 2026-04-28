"""
Data flattening utilities for Acumatica stock items data.
Handles nested JSON structures and converts them to flat CSV format.
"""

import pandas as pd
import json
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


def flatten_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten a pandas DataFrame with nested structures.
    
    Args:
        df: DataFrame to flatten
        
    Returns:
        Flattened DataFrame
    """
    # Convert DataFrame to list of dictionaries
    data = df.to_dict('records')
    
    # Flatten the data
    flattened_data = flatten_stock_items_data(data)
    
    # Convert back to DataFrame
    flattened_df = pd.DataFrame(flattened_data)
    return flattened_df

def _to_list_of_dicts(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize API response into a list of dictionaries."""
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


def processed_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
    # Normalize and flatten records
    
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df, df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # Stock items subset (only columns that exist)
    desired_stock_cols = ['InventoryID', 'Description', 'ItemClass', 'BaseUOM', 'DefaultPrice', 'DefaultWarehouseID', 'ItemType', 'ItemStatus', 'IsAKit']
    stock_cols = [c for c in desired_stock_cols if c in df.columns]
    stock_items_df = df[stock_cols].copy() if stock_cols else pd.DataFrame()

    # Warehouse details: detect any WarehouseDetails_<index>_<Field> columns
    warehouse_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('WarehouseDetails_')]
    if not warehouse_cols or 'InventoryID' not in df.columns:
        return stock_items_df, pd.DataFrame()

    base = df[['InventoryID'] + warehouse_cols].copy()
    melted = base.melt(id_vars=['InventoryID'], value_vars=warehouse_cols, var_name='WarehouseInfo', value_name='Value')
    extracted = melted['WarehouseInfo'].str.extract(r'WarehouseDetails_(\d+)_(.+)')
    melted['WarehouseIndex'] = extracted[0]
    melted['Field'] = extracted[1]
    pivoted = melted.pivot_table(index=['InventoryID', 'WarehouseIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
    rename_map = {
        'QtyOnHand': 'QtyOnHand',
        'WarehouseID': 'WarehouseID',
        'ReplenishmentWarehouse': 'ReplenishmentWarehouse'
    }
    pivoted = pivoted.rename(columns=rename_map)
    final_cols = ['InventoryID'] + [c for c in ['QtyOnHand', 'WarehouseID', 'ReplenishmentWarehouse'] if c in pivoted.columns]
    warehouse_df = pivoted[final_cols].copy()
    

    return stock_items_df, warehouse_df


def process_sales_order_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
    """
    Process SalesOrder data from Acumatica API response.
    Merges SalesOrder main data (including ShipToContact, BillToContact, ShipToAddress, BillToAddress, Shipments)
    with Details and TaxDetails expansions into a single DataFrame using OrderNbr as the merge key.
    Follows the same pattern as processed_data for StockItem.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all SalesOrder data
    """
    # Normalize and flatten records (same as processed_data)
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # SalesOrder main fields subset (only columns that exist)
    desired_sales_order_cols = [
        'OrderType', 'CustomerID', 'CustomerOrder', 'OrderNbr', 
        'LocationID', 'Status', 'Date', 'RequestedOn', 'Description', 
        'OrderedQty', 'OrderTotal', 'ShipVia'
    ]
    sales_order_cols = [c for c in desired_sales_order_cols if c in df.columns]
    sales_order_df = df[sales_order_cols].copy() if sales_order_cols else pd.DataFrame()
    
    # Process indexed expansions using same pattern as WarehouseDetails
    # Use OrderType and OrderNbr as composite key for merging
    merge_keys = ['OrderType', 'OrderNbr'] if 'OrderType' in df.columns and 'OrderNbr' in df.columns else (['OrderNbr'] if 'OrderNbr' in df.columns else [])
    
    # ShipToContact expansion: detect any ShipToContact_<index>_<Field> or ShipToContact_<Field> columns
    ship_to_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('ShipToContact_')]
    ship_to_contact_df = pd.DataFrame()
    if ship_to_contact_cols and merge_keys:
        base_stc = df[merge_keys + ship_to_contact_cols].copy()
        melted_stc = base_stc.melt(id_vars=merge_keys, value_vars=ship_to_contact_cols, var_name='ShipToContactInfo', value_name='Value')
        extracted_stc = melted_stc['ShipToContactInfo'].str.extract(r'ShipToContact_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_stc[0].isna().all():
            extracted_stc = melted_stc['ShipToContactInfo'].str.extract(r'ShipToContact_(.+)')
            extracted_stc.insert(0, 'ShipToContactIndex', '0')
            extracted_stc.columns = ['ShipToContactIndex', 'Field']
        else:
            extracted_stc.columns = ['ShipToContactIndex', 'Field']
        melted_stc['ShipToContactIndex'] = extracted_stc['ShipToContactIndex']
        melted_stc['Field'] = extracted_stc['Field']
        melted_stc = melted_stc[melted_stc['Field'].notna()]
        if not melted_stc.empty:
            pivoted_stc = melted_stc.pivot_table(index=merge_keys + ['ShipToContactIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            stc_final_cols = merge_keys + [c for c in pivoted_stc.columns if c not in merge_keys + ['ShipToContactIndex']]
            ship_to_contact_df = pivoted_stc[stc_final_cols].copy()
    
    # BillToContact expansion: detect any BillToContact_<index>_<Field> or BillToContact_<Field> columns
    bill_to_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillToContact_')]
    bill_to_contact_df = pd.DataFrame()
    if bill_to_contact_cols and merge_keys:
        base_btc = df[merge_keys + bill_to_contact_cols].copy()
        melted_btc = base_btc.melt(id_vars=merge_keys, value_vars=bill_to_contact_cols, var_name='BillToContactInfo', value_name='Value')
        extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillToContact_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_btc[0].isna().all():
            extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillToContact_(.+)')
            extracted_btc.insert(0, 'BillToContactIndex', '0')
            extracted_btc.columns = ['BillToContactIndex', 'Field']
        else:
            extracted_btc.columns = ['BillToContactIndex', 'Field']
        melted_btc['BillToContactIndex'] = extracted_btc['BillToContactIndex']
        melted_btc['Field'] = extracted_btc['Field']
        melted_btc = melted_btc[melted_btc['Field'].notna()]
        if not melted_btc.empty:
            pivoted_btc = melted_btc.pivot_table(index=merge_keys + ['BillToContactIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            btc_final_cols = merge_keys + [c for c in pivoted_btc.columns if c not in merge_keys + ['BillToContactIndex']]
            bill_to_contact_df = pivoted_btc[btc_final_cols].copy()
    
    # ShipToAddress expansion: detect any ShipToAddress_<index>_<Field> or ShipToAddress_<Field> columns
    ship_to_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('ShipToAddress_')]
    ship_to_address_df = pd.DataFrame()
    if ship_to_address_cols and merge_keys:
        base_sta = df[merge_keys + ship_to_address_cols].copy()
        melted_sta = base_sta.melt(id_vars=merge_keys, value_vars=ship_to_address_cols, var_name='ShipToAddressInfo', value_name='Value')
        extracted_sta = melted_sta['ShipToAddressInfo'].str.extract(r'ShipToAddress_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_sta[0].isna().all():
            extracted_sta = melted_sta['ShipToAddressInfo'].str.extract(r'ShipToAddress_(.+)')
            extracted_sta.insert(0, 'ShipToAddressIndex', '0')
            extracted_sta.columns = ['ShipToAddressIndex', 'Field']
        else:
            extracted_sta.columns = ['ShipToAddressIndex', 'Field']
        melted_sta['ShipToAddressIndex'] = extracted_sta['ShipToAddressIndex']
        melted_sta['Field'] = extracted_sta['Field']
        melted_sta = melted_sta[melted_sta['Field'].notna()]
        if not melted_sta.empty:
            pivoted_sta = melted_sta.pivot_table(index=merge_keys + ['ShipToAddressIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            sta_final_cols = merge_keys + [c for c in pivoted_sta.columns if c not in merge_keys + ['ShipToAddressIndex']]
            ship_to_address_df = pivoted_sta[sta_final_cols].copy()
    
    # BillToAddress expansion: detect any BillToAddress_<index>_<Field> or BillToAddress_<Field> columns
    bill_to_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillToAddress_')]
    bill_to_address_df = pd.DataFrame()
    if bill_to_address_cols and merge_keys:
        base_bta = df[merge_keys + bill_to_address_cols].copy()
        melted_bta = base_bta.melt(id_vars=merge_keys, value_vars=bill_to_address_cols, var_name='BillToAddressInfo', value_name='Value')
        extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillToAddress_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_bta[0].isna().all():
            extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillToAddress_(.+)')
            extracted_bta.insert(0, 'BillToAddressIndex', '0')
            extracted_bta.columns = ['BillToAddressIndex', 'Field']
        else:
            extracted_bta.columns = ['BillToAddressIndex', 'Field']
        melted_bta['BillToAddressIndex'] = extracted_bta['BillToAddressIndex']
        melted_bta['Field'] = extracted_bta['Field']
        melted_bta = melted_bta[melted_bta['Field'].notna()]
        if not melted_bta.empty:
            pivoted_bta = melted_bta.pivot_table(index=merge_keys + ['BillToAddressIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            bta_final_cols = merge_keys + [c for c in pivoted_bta.columns if c not in merge_keys + ['BillToAddressIndex']]
            bill_to_address_df = pivoted_bta[bta_final_cols].copy()
    
    # Shipments expansion: detect any Shipments_<index>_<Field> or Shipments_<Field> columns
    shipments_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Shipments_')]
    shipments_df = pd.DataFrame()
    if shipments_cols and merge_keys:
        base_ship = df[merge_keys + shipments_cols].copy()
        melted_ship = base_ship.melt(id_vars=merge_keys, value_vars=shipments_cols, var_name='ShipmentsInfo', value_name='Value')
        extracted_ship = melted_ship['ShipmentsInfo'].str.extract(r'Shipments_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_ship[0].isna().all():
            extracted_ship = melted_ship['ShipmentsInfo'].str.extract(r'Shipments_(.+)')
            extracted_ship.insert(0, 'ShipmentsIndex', '0')
            extracted_ship.columns = ['ShipmentsIndex', 'Field']
        else:
            extracted_ship.columns = ['ShipmentsIndex', 'Field']
        melted_ship['ShipmentsIndex'] = extracted_ship['ShipmentsIndex']
        melted_ship['Field'] = extracted_ship['Field']
        melted_ship = melted_ship[melted_ship['Field'].notna()]
        if not melted_ship.empty:
            pivoted_ship = melted_ship.pivot_table(index=merge_keys + ['ShipmentsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            ship_final_cols = merge_keys + [c for c in pivoted_ship.columns if c not in merge_keys + ['ShipmentsIndex']]
            shipments_df = pivoted_ship[ship_final_cols].copy()

    # Details expansion: detect any Details_<index>_<Field> columns
    details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Details_')]
    details_df = pd.DataFrame()
    if details_cols and merge_keys:
        base = df[merge_keys + details_cols].copy()
        melted = base.melt(id_vars=merge_keys, value_vars=details_cols, var_name='DetailsInfo', value_name='Value')
        extracted = melted['DetailsInfo'].str.extract(r'Details_(\d+)_(.+)')
        melted['DetailsIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(index=merge_keys + ['DetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        details_final_cols = merge_keys + [c for c in pivoted.columns if c not in merge_keys + ['DetailsIndex']]
        details_df = pivoted[details_final_cols].copy()

    # TaxDetails expansion: detect any TaxDetails_<index>_<Field> columns
    tax_details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('TaxDetails_')]
    tax_details_df = pd.DataFrame()
    if tax_details_cols and merge_keys:
        base_tax = df[merge_keys + tax_details_cols].copy()
        melted_tax = base_tax.melt(id_vars=merge_keys, value_vars=tax_details_cols, var_name='TaxDetailsInfo', value_name='Value')
        extracted_tax = melted_tax['TaxDetailsInfo'].str.extract(r'TaxDetails_(\d+)_(.+)')
        melted_tax['TaxDetailsIndex'] = extracted_tax[0]
        melted_tax['Field'] = extracted_tax[1]
        pivoted_tax = melted_tax.pivot_table(index=merge_keys + ['TaxDetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        tax_details_final_cols = merge_keys + [c for c in pivoted_tax.columns if c not in merge_keys + ['TaxDetailsIndex']]
        tax_details_df = pivoted_tax[tax_details_final_cols].copy()

    # Merge all dataframes on OrderType and OrderNbr (following WarehouseDetails pattern)
    merged_df = sales_order_df.copy()
    
    # Merge with expansion dataframes if they exist and are not empty
    # Add prefixes to column names for LLM clarity
    if not ship_to_contact_df.empty and all(key in ship_to_contact_df.columns for key in merge_keys):
        # Rename columns to add ShipToContact_ prefix (excluding merge keys)
        rename_dict = {col: f'ShipToContact_{col}' for col in ship_to_contact_df.columns if col not in merge_keys}
        ship_to_contact_df_prefixed = ship_to_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(ship_to_contact_df_prefixed, on=merge_keys, how='left')
    
    if not bill_to_contact_df.empty and all(key in bill_to_contact_df.columns for key in merge_keys):
        # Rename columns to add BillToContact_ prefix (excluding merge keys)
        rename_dict = {col: f'BillToContact_{col}' for col in bill_to_contact_df.columns if col not in merge_keys}
        bill_to_contact_df_prefixed = bill_to_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(bill_to_contact_df_prefixed, on=merge_keys, how='left')
    
    if not ship_to_address_df.empty and all(key in ship_to_address_df.columns for key in merge_keys):
        # Rename columns to add ShipToAddress_ prefix (excluding merge keys)
        rename_dict = {col: f'ShipToAddress_{col}' for col in ship_to_address_df.columns if col not in merge_keys}
        ship_to_address_df_prefixed = ship_to_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(ship_to_address_df_prefixed, on=merge_keys, how='left')
    
    if not bill_to_address_df.empty and all(key in bill_to_address_df.columns for key in merge_keys):
        # Rename columns to add BillToAddress_ prefix (excluding merge keys)
        rename_dict = {col: f'BillToAddress_{col}' for col in bill_to_address_df.columns if col not in merge_keys}
        bill_to_address_df_prefixed = bill_to_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(bill_to_address_df_prefixed, on=merge_keys, how='left')
    
    if not shipments_df.empty and all(key in shipments_df.columns for key in merge_keys):
        # Rename columns to add Shipments_ prefix (excluding merge keys)
        rename_dict = {col: f'Shipments_{col}' for col in shipments_df.columns if col not in merge_keys}
        shipments_df_prefixed = shipments_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(shipments_df_prefixed, on=merge_keys, how='left')
    
    # Merge with details_df if it exists and is not empty
    if not details_df.empty and all(key in details_df.columns for key in merge_keys):
        # Rename columns to add Details_ prefix (excluding merge keys)
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col not in merge_keys}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(details_df_prefixed, on=merge_keys, how='left')
    
    # Merge with tax_details_df if it exists and is not empty
    if not tax_details_df.empty and all(key in tax_details_df.columns for key in merge_keys):
        # Rename columns to add TaxDetails_ prefix (excluding merge keys)
        rename_dict = {col: f'TaxDetails_{col}' for col in tax_details_df.columns if col not in merge_keys}
        tax_details_df_prefixed = tax_details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(tax_details_df_prefixed, on=merge_keys, how='left')
    
    # Remove system/internal columns that are automatically added by Acumatica API
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_shipment_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
    """
    Process Shipment data from Acumatica API response.
    Merges Shipment main data with Details and Packages expansions into a single DataFrame using ShipmentNbr as the merge key.
    Follows the same pattern as processed_data for StockItem.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all Shipment data
    """
    # Normalize and flatten records (same as processed_data)
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df
    
    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)
    
    # Shipment main fields subset (only columns that exist)
    desired_shipment_cols = [
        'ShipmentNbr', 'Type', 'Status', 'Operation', 'ShipmentDate', 'Description',
        'CustomerID', 'LocationID', 'WarehouseID', 'ShippedQty', 'ShippedWeight',
        'ShippedVolume', 'PackageWeight', 'ShipVia'
    ]
    shipment_cols = [c for c in desired_shipment_cols if c in df.columns]
    shipment_df = df[shipment_cols].copy() if shipment_cols else pd.DataFrame()
    
    # Process indexed expansions using same pattern as WarehouseDetails
    # ShippingSettings/ShipToContact expansion: detect any ShippingSettings_ShipToContact_<index>_<Field> columns
    shipping_ship_to_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('ShippingSettings_ShipToContact_')]
    shipping_ship_to_contact_df = pd.DataFrame()
    if shipping_ship_to_contact_cols and 'ShipmentNbr' in df.columns:
        base_stc = df[['ShipmentNbr'] + shipping_ship_to_contact_cols].copy()
        melted_stc = base_stc.melt(id_vars=['ShipmentNbr'], value_vars=shipping_ship_to_contact_cols, var_name='ShipToContactInfo', value_name='Value')
        # Extract pattern: ShippingSettings_ShipToContact_<index>_<Field> or ShippingSettings_ShipToContact_<Field>
        extracted_stc = melted_stc['ShipToContactInfo'].str.extract(r'ShippingSettings_ShipToContact_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_stc[0].isna().all():
            # Extract field name directly: ShippingSettings_ShipToContact_BusinessName -> BusinessName
            extracted_stc = melted_stc['ShipToContactInfo'].str.extract(r'ShippingSettings_ShipToContact_(.+)')
            # Add index column with default value '0'
            extracted_stc.insert(0, 'ShipToContactIndex', '0')
            extracted_stc.columns = ['ShipToContactIndex', 'Field']
        else:
            # Pattern with index found, rename columns
            extracted_stc.columns = ['ShipToContactIndex', 'Field']
        
        melted_stc['ShipToContactIndex'] = extracted_stc['ShipToContactIndex']
        melted_stc['Field'] = extracted_stc['Field']
        melted_stc = melted_stc[melted_stc['Field'].notna()]
        if not melted_stc.empty:
            pivoted_stc = melted_stc.pivot_table(index=['ShipmentNbr', 'ShipToContactIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            stc_final_cols = ['ShipmentNbr'] + [c for c in pivoted_stc.columns if c not in ['ShipmentNbr', 'ShipToContactIndex']]
            shipping_ship_to_contact_df = pivoted_stc[stc_final_cols].copy()
    
    # ShippingSettings/ShipToAddress expansion: detect any ShippingSettings_ShipToAddress_<index>_<Field> columns
    shipping_ship_to_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('ShippingSettings_ShipToAddress_')]
    shipping_ship_to_address_df = pd.DataFrame()
    if shipping_ship_to_address_cols and 'ShipmentNbr' in df.columns:
        base_sta = df[['ShipmentNbr'] + shipping_ship_to_address_cols].copy()
        melted_sta = base_sta.melt(id_vars=['ShipmentNbr'], value_vars=shipping_ship_to_address_cols, var_name='ShipToAddressInfo', value_name='Value')
        # Extract pattern: ShippingSettings_ShipToAddress_<index>_<Field> or ShippingSettings_ShipToAddress_<Field>
        extracted_sta = melted_sta['ShipToAddressInfo'].str.extract(r'ShippingSettings_ShipToAddress_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_sta[0].isna().all():
            # Extract field name directly: ShippingSettings_ShipToAddress_AddressLine1 -> AddressLine1
            extracted_sta = melted_sta['ShipToAddressInfo'].str.extract(r'ShippingSettings_ShipToAddress_(.+)')
            # Add index column with default value '0'
            extracted_sta.insert(0, 'ShipToAddressIndex', '0')
            extracted_sta.columns = ['ShipToAddressIndex', 'Field']
        else:
            # Pattern with index found, rename columns
            extracted_sta.columns = ['ShipToAddressIndex', 'Field']
        
        melted_sta['ShipToAddressIndex'] = extracted_sta['ShipToAddressIndex']
        melted_sta['Field'] = extracted_sta['Field']
        melted_sta = melted_sta[melted_sta['Field'].notna()]
        if not melted_sta.empty:
            pivoted_sta = melted_sta.pivot_table(index=['ShipmentNbr', 'ShipToAddressIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            sta_final_cols = ['ShipmentNbr'] + [c for c in pivoted_sta.columns if c not in ['ShipmentNbr', 'ShipToAddressIndex']]
            shipping_ship_to_address_df = pivoted_sta[sta_final_cols].copy()
    
    # Details expansion: detect any Details_<index>_<Field> columns
    details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Details_')]
    details_df = pd.DataFrame()
    if details_cols and 'ShipmentNbr' in df.columns:
        base = df[['ShipmentNbr'] + details_cols].copy()
        melted = base.melt(id_vars=['ShipmentNbr'], value_vars=details_cols, var_name='DetailsInfo', value_name='Value')
        extracted = melted['DetailsInfo'].str.extract(r'Details_(\d+)_(.+)')
        melted['DetailsIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(index=['ShipmentNbr', 'DetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        details_final_cols = ['ShipmentNbr'] + [c for c in pivoted.columns if c not in ['ShipmentNbr', 'DetailsIndex']]
        details_df = pivoted[details_final_cols].copy()
    
    # Packages expansion: detect any Packages_<index>_<Field> columns
    packages_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Packages_')]
    packages_df = pd.DataFrame()
    if packages_cols and 'ShipmentNbr' in df.columns:
        base_pkg = df[['ShipmentNbr'] + packages_cols].copy()
        melted_pkg = base_pkg.melt(id_vars=['ShipmentNbr'], value_vars=packages_cols, var_name='PackagesInfo', value_name='Value')
        extracted_pkg = melted_pkg['PackagesInfo'].str.extract(r'Packages_(\d+)_(.+)')
        melted_pkg['PackagesIndex'] = extracted_pkg[0]
        melted_pkg['Field'] = extracted_pkg[1]
        pivoted_pkg = melted_pkg.pivot_table(index=['ShipmentNbr', 'PackagesIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        packages_final_cols = ['ShipmentNbr'] + [c for c in pivoted_pkg.columns if c not in ['ShipmentNbr', 'PackagesIndex']]
        packages_df = pivoted_pkg[packages_final_cols].copy()
    
    # Merge all dataframes on ShipmentNbr (following WarehouseDetails pattern)
    merged_df = shipment_df.copy()
    
    # Merge with ShippingSettings expansion dataframes if they exist and are not empty
    # Add prefixes to column names for LLM clarity
    if not shipping_ship_to_contact_df.empty and 'ShipmentNbr' in shipping_ship_to_contact_df.columns:
        # Rename columns to add ShipToContact_ prefix (excluding merge key)
        rename_dict = {col: f'ShipToContact_{col}' for col in shipping_ship_to_contact_df.columns if col != 'ShipmentNbr'}
        shipping_ship_to_contact_df_prefixed = shipping_ship_to_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(shipping_ship_to_contact_df_prefixed, on='ShipmentNbr', how='left')
    
    if not shipping_ship_to_address_df.empty and 'ShipmentNbr' in shipping_ship_to_address_df.columns:
        # Rename columns to add ShipToAddress_ prefix (excluding merge key)
        rename_dict = {col: f'ShipToAddress_{col}' for col in shipping_ship_to_address_df.columns if col != 'ShipmentNbr'}
        shipping_ship_to_address_df_prefixed = shipping_ship_to_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(shipping_ship_to_address_df_prefixed, on='ShipmentNbr', how='left')
    
    # Merge with details_df if it exists and is not empty
    if not details_df.empty and 'ShipmentNbr' in details_df.columns:
        # Rename columns to add Details_ prefix (excluding merge key)
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col != 'ShipmentNbr'}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(details_df_prefixed, on='ShipmentNbr', how='left')
    
    # Merge with packages_df if it exists and is not empty
    if not packages_df.empty and 'ShipmentNbr' in packages_df.columns:
        # Rename columns to add Packages_ prefix (excluding merge key)
        rename_dict = {col: f'Packages_{col}' for col in packages_df.columns if col != 'ShipmentNbr'}
        packages_df_prefixed = packages_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(packages_df_prefixed, on='ShipmentNbr', how='left')
    
    # Remove system/internal columns that are automatically added by Acumatica API
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_sales_invoice_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
    """
    Process SalesInvoice data from Acumatica API response.
    Merges SalesInvoice main data (including BillingSettings) with Details, TaxDetails, and Commission expansions
    into a single DataFrame using ReferenceNbr as the merge key.
    Follows the same pattern as process_sales_order_data.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all SalesInvoice data
    """
    # Normalize and flatten records (same as processed_data)
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # SalesInvoice main fields subset (only columns that exist)
    desired_sales_invoice_cols = [
        'ReferenceNbr', 'Status', 'Date', 'Type', 'Description', 
        'CustomerID', 'DetailTotal', 'TaxTotal', 
        'Amount', 'Balance'
    ]
    sales_invoice_cols = [c for c in desired_sales_invoice_cols if c in df.columns]
    sales_invoice_df = df[sales_invoice_cols].copy() if sales_invoice_cols else pd.DataFrame()
    
    # Process indexed expansions using same pattern as WarehouseDetails
    # BillingSettings/BillToContact expansion: detect any BillingSettings_BillToContact_<index>_<Field> columns
    billing_billto_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillingSettings_BillToContact_')]
    billing_billto_contact_df = pd.DataFrame()
    if billing_billto_contact_cols and 'ReferenceNbr' in df.columns:
        base_btc = df[['ReferenceNbr'] + billing_billto_contact_cols].copy()
        melted_btc = base_btc.melt(id_vars=['ReferenceNbr'], value_vars=billing_billto_contact_cols, var_name='BillToContactInfo', value_name='Value')
        # Extract pattern: BillingSettings_BillToContact_<index>_<Field> or BillingSettings_BillToContact_<Field>
        extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillingSettings_BillToContact_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_btc[0].isna().all():
            # Extract field name directly: BillingSettings_BillToContact_BusinessName -> BusinessName
            extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillingSettings_BillToContact_(.+)')
            # Add index column with default value '0'
            extracted_btc.insert(0, 'BillToContactIndex', '0')
            extracted_btc.columns = ['BillToContactIndex', 'Field']
        else:
            # Pattern with index found, rename columns
            extracted_btc.columns = ['BillToContactIndex', 'Field']
        
        melted_btc['BillToContactIndex'] = extracted_btc['BillToContactIndex']
        melted_btc['Field'] = extracted_btc['Field']
        melted_btc = melted_btc[melted_btc['Field'].notna()]
        if not melted_btc.empty:
            pivoted_btc = melted_btc.pivot_table(index=['ReferenceNbr', 'BillToContactIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            btc_final_cols = ['ReferenceNbr'] + [c for c in pivoted_btc.columns if c not in ['ReferenceNbr', 'BillToContactIndex']]
            billing_billto_contact_df = pivoted_btc[btc_final_cols].copy()
    
    # BillingSettings/BillToAddress expansion: detect any BillingSettings_BillToAddress_<index>_<Field> columns
    billing_billto_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillingSettings_BillToAddress_')]
    billing_billto_address_df = pd.DataFrame()
    if billing_billto_address_cols and 'ReferenceNbr' in df.columns:
        base_bta = df[['ReferenceNbr'] + billing_billto_address_cols].copy()
        melted_bta = base_bta.melt(id_vars=['ReferenceNbr'], value_vars=billing_billto_address_cols, var_name='BillToAddressInfo', value_name='Value')
        # Extract pattern: BillingSettings_BillToAddress_<index>_<Field> or BillingSettings_BillToAddress_<Field>
        extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillingSettings_BillToAddress_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_bta[0].isna().all():
            # Extract field name directly: BillingSettings_BillToAddress_AddressLine1 -> AddressLine1
            extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillingSettings_BillToAddress_(.+)')
            # Add index column with default value '0'
            extracted_bta.insert(0, 'BillToAddressIndex', '0')
            extracted_bta.columns = ['BillToAddressIndex', 'Field']
        else:
            # Pattern with index found, rename columns
            extracted_bta.columns = ['BillToAddressIndex', 'Field']
        
        melted_bta['BillToAddressIndex'] = extracted_bta['BillToAddressIndex']
        melted_bta['Field'] = extracted_bta['Field']
        melted_bta = melted_bta[melted_bta['Field'].notna()]
        if not melted_bta.empty:
            pivoted_bta = melted_bta.pivot_table(index=['ReferenceNbr', 'BillToAddressIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            bta_final_cols = ['ReferenceNbr'] + [c for c in pivoted_bta.columns if c not in ['ReferenceNbr', 'BillToAddressIndex']]
            billing_billto_address_df = pivoted_bta[bta_final_cols].copy()

    # Details expansion: detect any Details_<index>_<Field> columns
    details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Details_')]
    details_df = pd.DataFrame()
    if details_cols and 'ReferenceNbr' in df.columns:
        base = df[['ReferenceNbr'] + details_cols].copy()
        melted = base.melt(id_vars=['ReferenceNbr'], value_vars=details_cols, var_name='DetailsInfo', value_name='Value')
        extracted = melted['DetailsInfo'].str.extract(r'Details_(\d+)_(.+)')
        melted['DetailsIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(index=['ReferenceNbr', 'DetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        # KEEP DetailsIndex in details_df for proper row-by-row merging
        details_final_cols = ['ReferenceNbr', 'DetailsIndex'] + [c for c in pivoted.columns if c not in ['ReferenceNbr', 'DetailsIndex']]
        details_df = pivoted[details_final_cols].copy()

    # TaxDetails expansion: detect any TaxDetails_<index>_<Field> columns
    # COMMENTED OUT FOR NOW
    # tax_details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('TaxDetails_')]
    tax_details_df = pd.DataFrame()
    # if tax_details_cols and 'ReferenceNbr' in df.columns:
    #     base_tax = df[['ReferenceNbr'] + tax_details_cols].copy()
    #     melted_tax = base_tax.melt(id_vars=['ReferenceNbr'], value_vars=tax_details_cols, var_name='TaxDetailsInfo', value_name='Value')
    #     extracted_tax = melted_tax['TaxDetailsInfo'].str.extract(r'TaxDetails_(\d+)_(.+)')
    #     melted_tax['TaxDetailsIndex'] = extracted_tax[0]
    #     melted_tax['Field'] = extracted_tax[1]
    #     pivoted_tax = melted_tax.pivot_table(index=['ReferenceNbr', 'TaxDetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
    #     tax_details_final_cols = ['ReferenceNbr'] + [c for c in pivoted_tax.columns if c not in ['ReferenceNbr', 'TaxDetailsIndex']]
    #     tax_details_df = pivoted_tax[tax_details_final_cols].copy()

    # Commission expansion: detect any Commissions_<index>_<Field> or Commissions_<Field> columns (note: plural "Commissions")
    commission_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Commissions_')]
    commission_df = pd.DataFrame()
    if commission_cols and 'ReferenceNbr' in df.columns:
        base_comm = df[['ReferenceNbr'] + commission_cols].copy()
        melted_comm = base_comm.melt(id_vars=['ReferenceNbr'], value_vars=commission_cols, var_name='CommissionInfo', value_name='Value')
        # Try to extract index and field - handle both patterns:
        # Pattern 1: Commissions_0_CommissionAmount (with index)
        # Pattern 2: Commissions_CommissionAmount (without index)
        extracted_comm = melted_comm['CommissionInfo'].str.extract(r'Commissions_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_comm[0].isna().all():
            # Extract field name directly: Commissions_CommissionAmount -> CommissionAmount
            extracted_comm = melted_comm['CommissionInfo'].str.extract(r'Commissions_(.+)')
            # Add index column with default value '0' for single commission per invoice
            extracted_comm.insert(0, 'CommissionIndex', '0')
            extracted_comm.columns = ['CommissionIndex', 'Field']
        else:
            # Pattern with index found, rename columns
            extracted_comm.columns = ['CommissionIndex', 'Field']
        
        melted_comm['CommissionIndex'] = extracted_comm['CommissionIndex']
        melted_comm['Field'] = extracted_comm['Field']
        # Filter out rows where extraction failed
        melted_comm = melted_comm[melted_comm['Field'].notna()]
        if not melted_comm.empty:
            pivoted_comm = melted_comm.pivot_table(index=['ReferenceNbr', 'CommissionIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            commission_final_cols = ['ReferenceNbr'] + [c for c in pivoted_comm.columns if c not in ['ReferenceNbr', 'CommissionIndex']]
            commission_df = pivoted_comm[commission_final_cols].copy()

    # DiscountDetails expansion: detect any DiscountDetails_<index>_<Field> or DiscountDetails_<Field> columns
    discount_details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('DiscountDetails_')]
    discount_details_df = pd.DataFrame()
    if discount_details_cols and 'ReferenceNbr' in df.columns:
        base_discount = df[['ReferenceNbr'] + discount_details_cols].copy()
        melted_discount = base_discount.melt(id_vars=['ReferenceNbr'], value_vars=discount_details_cols, var_name='DiscountDetailsInfo', value_name='Value')
        # Try to extract index and field - handle both patterns:
        # Pattern 1: DiscountDetails_0_DiscountCode (with index)
        # Pattern 2: DiscountDetails_DiscountCode (without index)
        extracted_discount = melted_discount['DiscountDetailsInfo'].str.extract(r'DiscountDetails_(\d+)_(.+)')
        # If extraction failed (no index found), try pattern without index
        if extracted_discount[0].isna().all():
            # Extract field name directly: DiscountDetails_DiscountCode -> DiscountCode
            extracted_discount = melted_discount['DiscountDetailsInfo'].str.extract(r'DiscountDetails_(.+)')
            # Add index column with default value '0'
            extracted_discount.insert(0, 'DiscountDetailsIndex', '0')
            extracted_discount.columns = ['DiscountDetailsIndex', 'Field']
        else:
            # Pattern with index found, rename columns
            extracted_discount.columns = ['DiscountDetailsIndex', 'Field']
        
        melted_discount['DiscountDetailsIndex'] = extracted_discount['DiscountDetailsIndex']
        melted_discount['Field'] = extracted_discount['Field']
        # Filter out rows where extraction failed
        melted_discount = melted_discount[melted_discount['Field'].notna()]
        if not melted_discount.empty:
            pivoted_discount = melted_discount.pivot_table(index=['ReferenceNbr', 'DiscountDetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            discount_final_cols = ['ReferenceNbr'] + [c for c in pivoted_discount.columns if c not in ['ReferenceNbr', 'DiscountDetailsIndex']]
            discount_details_df = pivoted_discount[discount_final_cols].copy()

    # Merge all dataframes on ReferenceNbr (following WarehouseDetails pattern)
    merged_df = sales_invoice_df.copy()
    
    # Merge with BillingSettings expansion dataframes if they exist and are not empty
    # Add prefixes to column names for LLM clarity
    if not billing_billto_contact_df.empty and 'ReferenceNbr' in billing_billto_contact_df.columns:
        # Rename columns to add BillToContact_ prefix (excluding merge key)
        rename_dict = {col: f'BillToContact_{col}' for col in billing_billto_contact_df.columns if col != 'ReferenceNbr'}
        billing_billto_contact_df_prefixed = billing_billto_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(billing_billto_contact_df_prefixed, on='ReferenceNbr', how='left')
    
    if not billing_billto_address_df.empty and 'ReferenceNbr' in billing_billto_address_df.columns:
        # Rename columns to add BillToAddress_ prefix (excluding merge key)
        rename_dict = {col: f'BillToAddress_{col}' for col in billing_billto_address_df.columns if col != 'ReferenceNbr'}
        billing_billto_address_df_prefixed = billing_billto_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(billing_billto_address_df_prefixed, on='ReferenceNbr', how='left')
    
    # Merge with details_df if it exists and is not empty
    if not details_df.empty and 'ReferenceNbr' in details_df.columns:
        # Expand main dataframe to have one row per (ReferenceNbr, DetailsIndex) combination
        # This prevents cartesian product when merging Details
        details_index_map = details_df[['ReferenceNbr', 'DetailsIndex']].drop_duplicates()
        merged_df_expanded = merged_df.merge(details_index_map, on='ReferenceNbr', how='inner')
        
        # Rename columns to add Details_ prefix (excluding merge keys)
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col not in ['ReferenceNbr', 'DetailsIndex']}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        
        # Merge Details on both ReferenceNbr and DetailsIndex for proper row-by-row matching
        merged_df = merged_df_expanded.merge(details_df_prefixed, on=['ReferenceNbr', 'DetailsIndex'], how='left')
        # Remove DetailsIndex after merge (optional, for cleaner output)
        merged_df.drop(columns=['DetailsIndex'], inplace=True)
    
    # Merge with tax_details_df if it exists and is not empty
    if not tax_details_df.empty and 'ReferenceNbr' in tax_details_df.columns:
        # Rename columns to add TaxDetails_ prefix (excluding merge key)
        rename_dict = {col: f'TaxDetails_{col}' for col in tax_details_df.columns if col != 'ReferenceNbr'}
        tax_details_df_prefixed = tax_details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(tax_details_df_prefixed, on='ReferenceNbr', how='left')
    
    # Merge with commission_df if it exists and is not empty
    if not commission_df.empty and 'ReferenceNbr' in commission_df.columns:
        # Rename columns to add Commissions_ prefix (excluding merge key)
        rename_dict = {col: f'Commissions_{col}' for col in commission_df.columns if col != 'ReferenceNbr'}
        commission_df_prefixed = commission_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(commission_df_prefixed, on='ReferenceNbr', how='left')
    
    # Merge with discount_details_df if it exists and is not empty
    if not discount_details_df.empty and 'ReferenceNbr' in discount_details_df.columns:
        # Rename columns to add DiscountDetails_ prefix (excluding merge key)
        rename_dict = {col: f'DiscountDetails_{col}' for col in discount_details_df.columns if col != 'ReferenceNbr'}
        discount_details_df_prefixed = discount_details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(discount_details_df_prefixed, on='ReferenceNbr', how='left')
    
    # Remove system/internal columns that are automatically added by Acumatica API
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_invoice_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
    """
    Process Invoice data from Acumatica API response.
    Merges Invoice main data with Details expansion into a single DataFrame using ReferenceNbr as the merge key.
    Follows the same pattern as process_sales_invoice_data but simpler (only Details expansion).
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all Invoice data
    """
    # Normalize and flatten records (same as processed_data)
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # Invoice main fields subset (only columns that exist)
    desired_invoice_cols = [
        'ReferenceNbr',
        'LocationID',
        'PostPeriod'
    ]
    invoice_cols = [c for c in desired_invoice_cols if c in df.columns]
    invoice_df = df[invoice_cols].copy() if invoice_cols else pd.DataFrame()
    
    # Details expansion: detect any Details_<index>_<Field> columns
    details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Details_')]
    details_df = pd.DataFrame()
    if details_cols and 'ReferenceNbr' in df.columns:
        base = df[['ReferenceNbr'] + details_cols].copy()
        melted = base.melt(id_vars=['ReferenceNbr'], value_vars=details_cols, var_name='DetailsInfo', value_name='Value')
        extracted = melted['DetailsInfo'].str.extract(r'Details_(\d+)_(.+)')
        melted['DetailsIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(index=['ReferenceNbr', 'DetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        # KEEP DetailsIndex in details_df for proper row-by-row merging
        details_final_cols = ['ReferenceNbr', 'DetailsIndex'] + [c for c in pivoted.columns if c not in ['ReferenceNbr', 'DetailsIndex']]
        details_df = pivoted[details_final_cols].copy()

    # Merge all dataframes on ReferenceNbr (following WarehouseDetails pattern)
    merged_df = invoice_df.copy()
    
    # Merge with details_df if it exists and is not empty
    if not details_df.empty and 'ReferenceNbr' in details_df.columns:
        # Expand main dataframe to have one row per (ReferenceNbr, DetailsIndex) combination
        # This prevents cartesian product when merging Details
        details_index_map = details_df[['ReferenceNbr', 'DetailsIndex']].drop_duplicates()
        merged_df_expanded = merged_df.merge(details_index_map, on='ReferenceNbr', how='inner')
        
        # Rename columns to add Details_ prefix (excluding merge keys)
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col not in ['ReferenceNbr', 'DetailsIndex']}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        
        # Merge Details on both ReferenceNbr and DetailsIndex for proper row-by-row matching
        merged_df = merged_df_expanded.merge(details_df_prefixed, on=['ReferenceNbr', 'DetailsIndex'], how='left')
        # Remove DetailsIndex after merge (optional, for cleaner output)
        merged_df.drop(columns=['DetailsIndex'], inplace=True)
    
    # Remove system/internal columns that are automatically added by Acumatica API
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_item_warehouse_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
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
    
    return item_warehouse_df


def process_inventory_receipt_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]):
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
    
    return inventory_receipt_df


def generate_preview_data(df: pd.DataFrame, max_records: int = 10) -> dict:
    """
    Generate preview data with top_records from DataFrame.
    
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


# if __name__ == "__main__":
#     import csv
    
#     # Flatten existing JSON data
#     create_flattened_csv('stock_items.json', 'stock_items_flat.csv')
