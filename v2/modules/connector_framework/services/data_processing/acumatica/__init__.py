"""
Acumatica Data Processing Module

Subject-wise data processing for Acumatica entities.
Organized by subject area: Inventory, Sales, Financial
"""
from v2.modules.connector_framework.services.data_processing.acumatica.inventory_data_processing import (
    process_stock_item_data,  # Returns tuple: (stock_items_df, warehouse_df)
    process_item_warehouse_data,
    process_inventory_receipt_data
)
from v2.modules.connector_framework.services.data_processing.acumatica.sales_data_processing import (
    process_sales_order_data,
    process_sales_invoice_data,
    process_shipment_data,
    process_invoice_data
)
from v2.modules.connector_framework.services.data_processing.acumatica.financial_data_processing import (
    process_customer_data,
    process_vendor_data,
    process_journal_transaction_data
)
from v2.modules.connector_framework.services.data_processing.acumatica.common_utils import (
    flatten_dict,
    flatten_stock_items_data,
    _to_list_of_dicts,
    _remove_system_columns,
    generate_preview_data
)

__all__ = [
    # Inventory
    'process_stock_item_data',
    'process_item_warehouse_data',
    'process_inventory_receipt_data',
    # Sales
    'process_sales_order_data',
    'process_sales_invoice_data',
    'process_shipment_data',
    'process_invoice_data',
    # Financial
    'process_customer_data',
    'process_vendor_data',
    'process_journal_transaction_data',
    # Common utilities
    'flatten_dict',
    'flatten_stock_items_data',
    '_to_list_of_dicts',
    '_remove_system_columns',
    'generate_preview_data'
]
