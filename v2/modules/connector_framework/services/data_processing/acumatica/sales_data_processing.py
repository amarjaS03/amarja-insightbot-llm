"""
Sales Data Processing for Acumatica
Handles processing of Sales subject area entities:
- SalesOrder
- SalesInvoice
- Shipment
- Invoice
"""
import pandas as pd
from typing import Dict, Any, List, Union
from v2.modules.connector_framework.services.data_processing.acumatica.common_utils import (
    flatten_stock_items_data,
    _to_list_of_dicts,
    _remove_system_columns
)


def process_sales_order_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process SalesOrder data from Acumatica API response.
    Merges SalesOrder main data (including ShipToContact, BillToContact, ShipToAddress, BillToAddress, Shipments)
    with Details and TaxDetails expansions into a single DataFrame using OrderNbr as the merge key.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all SalesOrder data
    """
    # Normalize and flatten records
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
    
    # ShipToContact expansion
    ship_to_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('ShipToContact_')]
    ship_to_contact_df = pd.DataFrame()
    if ship_to_contact_cols and merge_keys:
        base_stc = df[merge_keys + ship_to_contact_cols].copy()
        melted_stc = base_stc.melt(id_vars=merge_keys, value_vars=ship_to_contact_cols, var_name='ShipToContactInfo', value_name='Value')
        extracted_stc = melted_stc['ShipToContactInfo'].str.extract(r'ShipToContact_(\d+)_(.+)')
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
    
    # BillToContact expansion
    bill_to_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillToContact_')]
    bill_to_contact_df = pd.DataFrame()
    if bill_to_contact_cols and merge_keys:
        base_btc = df[merge_keys + bill_to_contact_cols].copy()
        melted_btc = base_btc.melt(id_vars=merge_keys, value_vars=bill_to_contact_cols, var_name='BillToContactInfo', value_name='Value')
        extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillToContact_(\d+)_(.+)')
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
    
    # ShipToAddress expansion
    ship_to_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('ShipToAddress_')]
    ship_to_address_df = pd.DataFrame()
    if ship_to_address_cols and merge_keys:
        base_sta = df[merge_keys + ship_to_address_cols].copy()
        melted_sta = base_sta.melt(id_vars=merge_keys, value_vars=ship_to_address_cols, var_name='ShipToAddressInfo', value_name='Value')
        extracted_sta = melted_sta['ShipToAddressInfo'].str.extract(r'ShipToAddress_(\d+)_(.+)')
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
    
    # BillToAddress expansion
    bill_to_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillToAddress_')]
    bill_to_address_df = pd.DataFrame()
    if bill_to_address_cols and merge_keys:
        base_bta = df[merge_keys + bill_to_address_cols].copy()
        melted_bta = base_bta.melt(id_vars=merge_keys, value_vars=bill_to_address_cols, var_name='BillToAddressInfo', value_name='Value')
        extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillToAddress_(\d+)_(.+)')
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
    
    # Shipments expansion
    shipments_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Shipments_')]
    shipments_df = pd.DataFrame()
    if shipments_cols and merge_keys:
        base_ship = df[merge_keys + shipments_cols].copy()
        melted_ship = base_ship.melt(id_vars=merge_keys, value_vars=shipments_cols, var_name='ShipmentsInfo', value_name='Value')
        extracted_ship = melted_ship['ShipmentsInfo'].str.extract(r'Shipments_(\d+)_(.+)')
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

    # Details expansion
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

    # TaxDetails expansion
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

    # Merge all dataframes
    merged_df = sales_order_df.copy()
    
    if not ship_to_contact_df.empty and all(key in ship_to_contact_df.columns for key in merge_keys):
        rename_dict = {col: f'ShipToContact_{col}' for col in ship_to_contact_df.columns if col not in merge_keys}
        ship_to_contact_df_prefixed = ship_to_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(ship_to_contact_df_prefixed, on=merge_keys, how='left')
    
    if not bill_to_contact_df.empty and all(key in bill_to_contact_df.columns for key in merge_keys):
        rename_dict = {col: f'BillToContact_{col}' for col in bill_to_contact_df.columns if col not in merge_keys}
        bill_to_contact_df_prefixed = bill_to_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(bill_to_contact_df_prefixed, on=merge_keys, how='left')
    
    if not ship_to_address_df.empty and all(key in ship_to_address_df.columns for key in merge_keys):
        rename_dict = {col: f'ShipToAddress_{col}' for col in ship_to_address_df.columns if col not in merge_keys}
        ship_to_address_df_prefixed = ship_to_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(ship_to_address_df_prefixed, on=merge_keys, how='left')
    
    if not bill_to_address_df.empty and all(key in bill_to_address_df.columns for key in merge_keys):
        rename_dict = {col: f'BillToAddress_{col}' for col in bill_to_address_df.columns if col not in merge_keys}
        bill_to_address_df_prefixed = bill_to_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(bill_to_address_df_prefixed, on=merge_keys, how='left')
    
    if not shipments_df.empty and all(key in shipments_df.columns for key in merge_keys):
        rename_dict = {col: f'Shipments_{col}' for col in shipments_df.columns if col not in merge_keys}
        shipments_df_prefixed = shipments_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(shipments_df_prefixed, on=merge_keys, how='left')
    
    if not details_df.empty and all(key in details_df.columns for key in merge_keys):
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col not in merge_keys}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(details_df_prefixed, on=merge_keys, how='left')
    
    if not tax_details_df.empty and all(key in tax_details_df.columns for key in merge_keys):
        rename_dict = {col: f'TaxDetails_{col}' for col in tax_details_df.columns if col not in merge_keys}
        tax_details_df_prefixed = tax_details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(tax_details_df_prefixed, on=merge_keys, how='left')
    
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_sales_invoice_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process SalesInvoice data from Acumatica API response.
    Merges SalesInvoice main data (including BillingSettings) with Details, TaxDetails, and Commission expansions
    into a single DataFrame using ReferenceNbr as the merge key.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all SalesInvoice data
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # SalesInvoice main fields subset
    desired_sales_invoice_cols = [
        'ReferenceNbr', 'Status', 'Date', 'Type', 'Description', 
        'CustomerID', 'DetailTotal', 'TaxTotal', 
        'Amount', 'Balance'
    ]
    sales_invoice_cols = [c for c in desired_sales_invoice_cols if c in df.columns]
    sales_invoice_df = df[sales_invoice_cols].copy() if sales_invoice_cols else pd.DataFrame()
    
    # BillingSettings/BillToContact expansion
    billing_billto_contact_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillingSettings_BillToContact_')]
    billing_billto_contact_df = pd.DataFrame()
    if billing_billto_contact_cols and 'ReferenceNbr' in df.columns:
        base_btc = df[['ReferenceNbr'] + billing_billto_contact_cols].copy()
        melted_btc = base_btc.melt(id_vars=['ReferenceNbr'], value_vars=billing_billto_contact_cols, var_name='BillToContactInfo', value_name='Value')
        extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillingSettings_BillToContact_(\d+)_(.+)')
        if extracted_btc[0].isna().all():
            extracted_btc = melted_btc['BillToContactInfo'].str.extract(r'BillingSettings_BillToContact_(.+)')
            extracted_btc.insert(0, 'BillToContactIndex', '0')
            extracted_btc.columns = ['BillToContactIndex', 'Field']
        else:
            extracted_btc.columns = ['BillToContactIndex', 'Field']
        melted_btc['BillToContactIndex'] = extracted_btc['BillToContactIndex']
        melted_btc['Field'] = extracted_btc['Field']
        melted_btc = melted_btc[melted_btc['Field'].notna()]
        if not melted_btc.empty:
            pivoted_btc = melted_btc.pivot_table(index=['ReferenceNbr', 'BillToContactIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            btc_final_cols = ['ReferenceNbr'] + [c for c in pivoted_btc.columns if c not in ['ReferenceNbr', 'BillToContactIndex']]
            billing_billto_contact_df = pivoted_btc[btc_final_cols].copy()
    
    # BillingSettings/BillToAddress expansion
    billing_billto_address_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('BillingSettings_BillToAddress_')]
    billing_billto_address_df = pd.DataFrame()
    if billing_billto_address_cols and 'ReferenceNbr' in df.columns:
        base_bta = df[['ReferenceNbr'] + billing_billto_address_cols].copy()
        melted_bta = base_bta.melt(id_vars=['ReferenceNbr'], value_vars=billing_billto_address_cols, var_name='BillToAddressInfo', value_name='Value')
        extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillingSettings_BillToAddress_(\d+)_(.+)')
        if extracted_bta[0].isna().all():
            extracted_bta = melted_bta['BillToAddressInfo'].str.extract(r'BillingSettings_BillToAddress_(.+)')
            extracted_bta.insert(0, 'BillToAddressIndex', '0')
            extracted_bta.columns = ['BillToAddressIndex', 'Field']
        else:
            extracted_bta.columns = ['BillToAddressIndex', 'Field']
        melted_bta['BillToAddressIndex'] = extracted_bta['BillToAddressIndex']
        melted_bta['Field'] = extracted_bta['Field']
        melted_bta = melted_bta[melted_bta['Field'].notna()]
        if not melted_bta.empty:
            pivoted_bta = melted_bta.pivot_table(index=['ReferenceNbr', 'BillToAddressIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            bta_final_cols = ['ReferenceNbr'] + [c for c in pivoted_bta.columns if c not in ['ReferenceNbr', 'BillToAddressIndex']]
            billing_billto_address_df = pivoted_bta[bta_final_cols].copy()

    # Details expansion
    details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Details_')]
    details_df = pd.DataFrame()
    if details_cols and 'ReferenceNbr' in df.columns:
        base = df[['ReferenceNbr'] + details_cols].copy()
        melted = base.melt(id_vars=['ReferenceNbr'], value_vars=details_cols, var_name='DetailsInfo', value_name='Value')
        extracted = melted['DetailsInfo'].str.extract(r'Details_(\d+)_(.+)')
        melted['DetailsIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(index=['ReferenceNbr', 'DetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        details_final_cols = ['ReferenceNbr', 'DetailsIndex'] + [c for c in pivoted.columns if c not in ['ReferenceNbr', 'DetailsIndex']]
        details_df = pivoted[details_final_cols].copy()

    # Commission expansion
    commission_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Commissions_')]
    commission_df = pd.DataFrame()
    if commission_cols and 'ReferenceNbr' in df.columns:
        base_comm = df[['ReferenceNbr'] + commission_cols].copy()
        melted_comm = base_comm.melt(id_vars=['ReferenceNbr'], value_vars=commission_cols, var_name='CommissionInfo', value_name='Value')
        extracted_comm = melted_comm['CommissionInfo'].str.extract(r'Commissions_(\d+)_(.+)')
        if extracted_comm[0].isna().all():
            extracted_comm = melted_comm['CommissionInfo'].str.extract(r'Commissions_(.+)')
            extracted_comm.insert(0, 'CommissionIndex', '0')
            extracted_comm.columns = ['CommissionIndex', 'Field']
        else:
            extracted_comm.columns = ['CommissionIndex', 'Field']
        melted_comm['CommissionIndex'] = extracted_comm['CommissionIndex']
        melted_comm['Field'] = extracted_comm['Field']
        melted_comm = melted_comm[melted_comm['Field'].notna()]
        if not melted_comm.empty:
            pivoted_comm = melted_comm.pivot_table(index=['ReferenceNbr', 'CommissionIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            commission_final_cols = ['ReferenceNbr'] + [c for c in pivoted_comm.columns if c not in ['ReferenceNbr', 'CommissionIndex']]
            commission_df = pivoted_comm[commission_final_cols].copy()

    # DiscountDetails expansion
    discount_details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('DiscountDetails_')]
    discount_details_df = pd.DataFrame()
    if discount_details_cols and 'ReferenceNbr' in df.columns:
        base_discount = df[['ReferenceNbr'] + discount_details_cols].copy()
        melted_discount = base_discount.melt(id_vars=['ReferenceNbr'], value_vars=discount_details_cols, var_name='DiscountDetailsInfo', value_name='Value')
        extracted_discount = melted_discount['DiscountDetailsInfo'].str.extract(r'DiscountDetails_(\d+)_(.+)')
        if extracted_discount[0].isna().all():
            extracted_discount = melted_discount['DiscountDetailsInfo'].str.extract(r'DiscountDetails_(.+)')
            extracted_discount.insert(0, 'DiscountDetailsIndex', '0')
            extracted_discount.columns = ['DiscountDetailsIndex', 'Field']
        else:
            extracted_discount.columns = ['DiscountDetailsIndex', 'Field']
        melted_discount['DiscountDetailsIndex'] = extracted_discount['DiscountDetailsIndex']
        melted_discount['Field'] = extracted_discount['Field']
        melted_discount = melted_discount[melted_discount['Field'].notna()]
        if not melted_discount.empty:
            pivoted_discount = melted_discount.pivot_table(index=['ReferenceNbr', 'DiscountDetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
            discount_final_cols = ['ReferenceNbr'] + [c for c in pivoted_discount.columns if c not in ['ReferenceNbr', 'DiscountDetailsIndex']]
            discount_details_df = pivoted_discount[discount_final_cols].copy()

    # Merge all dataframes
    merged_df = sales_invoice_df.copy()
    
    if not billing_billto_contact_df.empty and 'ReferenceNbr' in billing_billto_contact_df.columns:
        rename_dict = {col: f'BillToContact_{col}' for col in billing_billto_contact_df.columns if col != 'ReferenceNbr'}
        billing_billto_contact_df_prefixed = billing_billto_contact_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(billing_billto_contact_df_prefixed, on='ReferenceNbr', how='left')
    
    if not billing_billto_address_df.empty and 'ReferenceNbr' in billing_billto_address_df.columns:
        rename_dict = {col: f'BillToAddress_{col}' for col in billing_billto_address_df.columns if col != 'ReferenceNbr'}
        billing_billto_address_df_prefixed = billing_billto_address_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(billing_billto_address_df_prefixed, on='ReferenceNbr', how='left')
    
    if not details_df.empty and 'ReferenceNbr' in details_df.columns:
        details_index_map = details_df[['ReferenceNbr', 'DetailsIndex']].drop_duplicates()
        merged_df_expanded = merged_df.merge(details_index_map, on='ReferenceNbr', how='inner')
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col not in ['ReferenceNbr', 'DetailsIndex']}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        merged_df = merged_df_expanded.merge(details_df_prefixed, on=['ReferenceNbr', 'DetailsIndex'], how='left')
        merged_df.drop(columns=['DetailsIndex'], inplace=True, errors='ignore')
    
    if not commission_df.empty and 'ReferenceNbr' in commission_df.columns:
        rename_dict = {col: f'Commissions_{col}' for col in commission_df.columns if col != 'ReferenceNbr'}
        commission_df_prefixed = commission_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(commission_df_prefixed, on='ReferenceNbr', how='left')
    
    if not discount_details_df.empty and 'ReferenceNbr' in discount_details_df.columns:
        rename_dict = {col: f'DiscountDetails_{col}' for col in discount_details_df.columns if col != 'ReferenceNbr'}
        discount_details_df_prefixed = discount_details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(discount_details_df_prefixed, on='ReferenceNbr', how='left')
    
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_shipment_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process Shipment data from Acumatica API response.
    Merges Shipment main data with Details and Packages expansions into a single DataFrame using ShipmentNbr as the merge key.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all Shipment data
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df
    
    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)
    
    # Shipment main fields subset
    desired_shipment_cols = [
        'ShipmentNbr', 'Type', 'Status', 'Operation', 'ShipmentDate', 'Description',
        'CustomerID', 'LocationID', 'WarehouseID', 'ShippedQty', 'ShippedWeight',
        'ShippedVolume', 'PackageWeight', 'ShipVia'
    ]
    shipment_cols = [c for c in desired_shipment_cols if c in df.columns]
    shipment_df = df[shipment_cols].copy() if shipment_cols else pd.DataFrame()
    
    # Details expansion
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
    
    # Packages expansion
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
    
    # Merge all dataframes
    merged_df = shipment_df.copy()
    
    if not details_df.empty and 'ShipmentNbr' in details_df.columns:
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col != 'ShipmentNbr'}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(details_df_prefixed, on='ShipmentNbr', how='left')
    
    if not packages_df.empty and 'ShipmentNbr' in packages_df.columns:
        rename_dict = {col: f'Packages_{col}' for col in packages_df.columns if col != 'ShipmentNbr'}
        packages_df_prefixed = packages_df.rename(columns=rename_dict)
        merged_df = merged_df.merge(packages_df_prefixed, on='ShipmentNbr', how='left')
    
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df


def process_invoice_data(raw: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Process Invoice data from Acumatica API response.
    Merges Invoice main data with Details expansion into a single DataFrame using ReferenceNbr as the merge key.
    
    Args:
        raw: Raw API response (dict or list)
        
    Returns:
        Single merged pandas DataFrame with all Invoice data
    """
    # Normalize and flatten records
    records = _to_list_of_dicts(raw)
    flattened_data = flatten_stock_items_data(records)
    df = pd.DataFrame(flattened_data)
    
    if df.empty:
        return df

    # Remove '_value' suffix from Acumatica fields
    df = df.rename(columns=lambda c: c[:-6] if isinstance(c, str) and c.endswith('_value') else c)

    # Invoice main fields subset
    desired_invoice_cols = [
        'ReferenceNbr',
        'LocationID',
        'PostPeriod'
    ]
    invoice_cols = [c for c in desired_invoice_cols if c in df.columns]
    invoice_df = df[invoice_cols].copy() if invoice_cols else pd.DataFrame()
    
    # Details expansion
    details_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('Details_')]
    details_df = pd.DataFrame()
    if details_cols and 'ReferenceNbr' in df.columns:
        base = df[['ReferenceNbr'] + details_cols].copy()
        melted = base.melt(id_vars=['ReferenceNbr'], value_vars=details_cols, var_name='DetailsInfo', value_name='Value')
        extracted = melted['DetailsInfo'].str.extract(r'Details_(\d+)_(.+)')
        melted['DetailsIndex'] = extracted[0]
        melted['Field'] = extracted[1]
        pivoted = melted.pivot_table(index=['ReferenceNbr', 'DetailsIndex'], columns='Field', values='Value', aggfunc='first').reset_index()
        details_final_cols = ['ReferenceNbr', 'DetailsIndex'] + [c for c in pivoted.columns if c not in ['ReferenceNbr', 'DetailsIndex']]
        details_df = pivoted[details_final_cols].copy()

    # Merge all dataframes on ReferenceNbr
    merged_df = invoice_df.copy()
    
    if not details_df.empty and 'ReferenceNbr' in details_df.columns:
        details_index_map = details_df[['ReferenceNbr', 'DetailsIndex']].drop_duplicates()
        merged_df_expanded = merged_df.merge(details_index_map, on='ReferenceNbr', how='inner')
        rename_dict = {col: f'Details_{col}' for col in details_df.columns if col not in ['ReferenceNbr', 'DetailsIndex']}
        details_df_prefixed = details_df.rename(columns=rename_dict)
        merged_df = merged_df_expanded.merge(details_df_prefixed, on=['ReferenceNbr', 'DetailsIndex'], how='left')
        merged_df.drop(columns=['DetailsIndex'], inplace=True, errors='ignore')
    
    merged_df = _remove_system_columns(merged_df)
    
    return merged_df
