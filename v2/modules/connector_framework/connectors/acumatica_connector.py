"""
Acumatica Connector - Real implementation using OAuth and REST API
"""
import asyncio
import requests
from typing import Dict, Any, Optional, List
import pandas as pd

from v2.modules.connector_framework.core.base_connector import (
    BaseConnector, ConnectionError, SchemaError, DataFetchError
)
from v2.common.logger import add_log
import traceback


class AcumaticaConnector(BaseConnector):
    """
    Acumatica ERP Connector using OAuth and REST API.
    
    Required credentials:
        - tenant_url: Acumatica tenant URL (e.g., https://company.acumatica.com)
        - access_token: OAuth access token
        - refresh_token: Optional OAuth refresh token
        - company: Optional company/branch name
    """
    
    # Common Acumatica endpoints/entities
    COMMON_ENTITIES = [
        "Customer", "Vendor", "StockItem", "NonStockItem",
        "SalesOrder", "PurchaseOrder", "Invoice", "Bill",
        "Payment", "Employee", "Project", "Task",
        "Lead", "Opportunity", "Case", "Contact"
    ]
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize Acumatica connector.
        
        Args:
            credentials: Dict containing tenant_url, access_token, company
        """
        super().__init__(credentials)
        self._tenant_url: Optional[str] = None
        self._access_token: Optional[str] = None
        self._company: Optional[str] = None
        self._api_version = "20.200.001"
        self._session = requests.Session()
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests"""
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def _get_api_url(self, endpoint: str) -> str:
        """Build API URL"""
        base = f"{self._tenant_url}/entity/Default/{self._api_version}"
        if self._company:
            return f"{base}/{endpoint}?$company={self._company}"
        return f"{base}/{endpoint}"
    
    async def connect(self) -> bool:
        """
        Establish connection to Acumatica using OAuth tokens.
        
        Returns:
            bool: True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        await asyncio.sleep(0)
        try:
            self._tenant_url = self._credentials.get('tenant_url', '').rstrip('/')
            self._access_token = self._credentials.get('access_token', '')
            self._company = self._credentials.get('company')
            
            if not self._tenant_url or not self._access_token:
                raise ConnectionError("Missing tenant_url or access_token in credentials")
            
            # Validate connection by making a simple API call
            url = f"{self._tenant_url}/entity/Default/{self._api_version}/$metadata"
            response = self._session.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code in [200, 204]:
                self._connected = True
                add_log(f"AcumaticaConnector: Connected to {self._tenant_url}")
                return True
            elif response.status_code == 401:
                raise ConnectionError("Invalid or expired access token")
            else:
                raise ConnectionError(f"Acumatica API returned status {response.status_code}")
                
        except requests.RequestException as e:
            self._connected = False
            add_log(f"AcumaticaConnector: Connection failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to Acumatica: {str(e)}")
        except Exception as e:
            self._connected = False
            add_log(f"AcumaticaConnector: Connection failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to Acumatica: {str(e)}")
    
    async def test_connection(self) -> bool:
        """
        Test if the connection is healthy.
        
        Returns:
            bool: True if connection is healthy
        """
        await asyncio.sleep(0)
        try:
            if not self._tenant_url or not self._access_token:
                return False
            
            url = f"{self._tenant_url}/entity/Default/{self._api_version}/$metadata"
            response = self._session.get(url, headers=self._get_headers(), timeout=10)
            return response.status_code in [200, 204]
        except Exception as e:
            add_log(f"AcumaticaConnector: Connection test failed: {str(e)}")
            self._connected = False
            return False
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get available Acumatica entities.
        
        Returns:
            Dict containing 'objects' list
            
        Raises:
            SchemaError: If schema retrieval fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected:
                raise SchemaError("Not connected to Acumatica")
            
            # Try to get available endpoints from metadata
            available_entities = []
            
            # Test each common entity to see if it's available
            for entity in self.COMMON_ENTITIES:
                try:
                    url = self._get_api_url(entity)
                    response = self._session.get(
                        url, 
                        headers=self._get_headers(), 
                        params={'$top': 1},
                        timeout=10
                    )
                    if response.status_code == 200:
                        available_entities.append(entity)
                except Exception:
                    continue
            
            add_log(f"AcumaticaConnector: Found {len(available_entities)} available entities")
            return {
                "objects": available_entities,
                "tables": [],
                "views": []
            }
        except Exception as e:
            add_log(f"AcumaticaConnector: Schema retrieval failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Failed to get schema: {str(e)}")
    
    async def get_data(
        self, 
        table_name: str, 
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch data from an Acumatica entity.
        
        Args:
            table_name: Name of the Acumatica entity
            limit: Optional row limit
            
        Returns:
            pd.DataFrame: The fetched data
            
        Raises:
            DataFetchError: If data fetching fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected:
                raise DataFetchError("Not connected to Acumatica")
            
            url = self._get_api_url(table_name)
            params = {}
            
            if limit:
                params['$top'] = limit
            else:
                params['$top'] = 10000  # Default limit
            
            # Add expand parameter if provided
            expand = kwargs.get('expand')
            if expand:
                params['$expand'] = expand
            
            response = self._session.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=60
            )
            
            if response.status_code != 200:
                raise DataFetchError(f"API request failed: {response.text}")
            
            records = response.json()
            
            if not isinstance(records, list):
                records = [records] if records else []
            
            # Flatten nested objects for DataFrame compatibility
            flattened_records = []
            for record in records:
                flat_record = {}
                for key, value in record.items():
                    if isinstance(value, dict) and 'value' in value:
                        flat_record[key] = value['value']
                    elif isinstance(value, dict):
                        # Skip complex nested objects
                        continue
                    elif isinstance(value, list):
                        # Skip array fields
                        continue
                    else:
                        flat_record[key] = value
                flattened_records.append(flat_record)
            
            df = pd.DataFrame(flattened_records)
            
            add_log(f"AcumaticaConnector: Fetched {len(df)} records from {table_name}")
            return df
        except Exception as e:
            add_log(f"AcumaticaConnector: Data fetch failed for {table_name}: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to fetch data from {table_name}: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close the Acumatica connection"""
        await asyncio.sleep(0)
        try:
            self._session.close()
            self._access_token = None
            self._tenant_url = None
            self._connected = False
            add_log("AcumaticaConnector: Disconnected")
        except Exception as e:
            add_log(f"AcumaticaConnector: Error during disconnect: {str(e)}")

