"""
Salesforce Data Processing Module

Subject-wise data processing for Salesforce objects.
Organized by subject area: Sales, etc.
"""
from v2.modules.connector_framework.salesforce.sales_data_processing import (
    process_account_data,
    process_opportunity_data
)
from v2.modules.connector_framework.salesforce.common_utils import (
    normalize_salesforce_data,
    convert_date_fields,
    clean_dataframe
)

__all__ = [
    # Sales subject area
    'process_account_data',
    'process_opportunity_data',
    # Common utilities
    'normalize_salesforce_data',
    'convert_date_fields',
    'clean_dataframe'
]
