"""
Salesforce Connector - Real implementation using OAuth and REST API
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


class SalesforceConnector(BaseConnector):
    """
    Salesforce Connector using OAuth and REST API.
    
    Required credentials:
        - instance_url: Salesforce instance URL (e.g., https://na1.salesforce.com)
        - access_token: OAuth access token
        - refresh_token: Optional OAuth refresh token
    """
    
    # Common Salesforce objects for schema
    COMMON_OBJECTS = [
        "Account", "Contact", "Lead", "Opportunity", "Case",
        "Campaign", "Task", "Event", "User", "Product2",
        "Pricebook2", "PricebookEntry", "Order", "OrderItem",
        "Contract", "Asset", "Quote", "QuoteLineItem"
    ]
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize Salesforce connector.
        
        Args:
            credentials: Dict containing instance_url, access_token, and optionally refresh_token
        """
        super().__init__(credentials)
        self._instance_url: Optional[str] = None
        self._access_token: Optional[str] = None
        self._api_version = "v59.0"
        self._session = requests.Session()
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests"""
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }
    
    def _get_api_url(self, endpoint: str) -> str:
        """Build API URL"""
        return f"{self._instance_url}/services/data/{self._api_version}/{endpoint}"
    
    async def connect(self) -> bool:
        """
        Establish connection to Salesforce using OAuth tokens.
        
        Returns:
            bool: True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        await asyncio.sleep(0)
        try:
            self._instance_url = self._credentials.get('instance_url', '').rstrip('/')
            self._access_token = self._credentials.get('access_token', '')
            
            if not self._instance_url or not self._access_token:
                raise ConnectionError("Missing instance_url or access_token in credentials")
            
            # Validate connection by fetching user info
            url = f"{self._instance_url}/services/oauth2/userinfo"
            response = self._session.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code == 200:
                self._connected = True
                user_info = response.json()
                add_log(f"SalesforceConnector: Connected as {user_info.get('preferred_username', 'unknown')}")
                return True
            else:
                raise ConnectionError(f"Salesforce API returned status {response.status_code}: {response.text}")
                
        except requests.RequestException as e:
            self._connected = False
            add_log(f"SalesforceConnector: Connection failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to Salesforce: {str(e)}")
        except Exception as e:
            self._connected = False
            add_log(f"SalesforceConnector: Connection failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to Salesforce: {str(e)}")
    
    async def test_connection(self) -> bool:
        """
        Test if the connection is healthy.
        
        Returns:
            bool: True if connection is healthy
        """
        await asyncio.sleep(0)
        try:
            if not self._instance_url or not self._access_token:
                return False
            
            url = self._get_api_url("limits")
            response = self._session.get(url, headers=self._get_headers(), timeout=10)
            return response.status_code == 200
        except Exception as e:
            add_log(f"SalesforceConnector: Connection test failed: {str(e)}")
            self._connected = False
            return False
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get available Salesforce objects.
        
        Returns:
            Dict containing 'objects' list
            
        Raises:
            SchemaError: If schema retrieval fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected:
                raise SchemaError("Not connected to Salesforce")
            
            url = self._get_api_url("sobjects")
            response = self._session.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code != 200:
                raise SchemaError(f"Failed to get schema: {response.text}")
            
            data = response.json()
            sobjects = data.get('sobjects', [])
            
            # Filter to queryable objects
            available_objects = [
                obj['name'] for obj in sobjects 
                if obj.get('queryable', False) and not obj.get('deprecatedAndHidden', False)
            ]
            
            add_log(f"SalesforceConnector: Retrieved {len(available_objects)} queryable objects")
            return {
                "objects": available_objects,
                "tables": [],  # For compatibility
                "views": []
            }
        except Exception as e:
            add_log(f"SalesforceConnector: Schema retrieval failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Failed to get schema: {str(e)}")
    
    async def get_object_fields(self, object_name: str) -> List[Dict[str, Any]]:
        """
        Get fields for a specific Salesforce object.
        
        Args:
            object_name: Name of the Salesforce object
            
        Returns:
            List of field information dictionaries
        """
        await asyncio.sleep(0)
        try:
            url = self._get_api_url(f"sobjects/{object_name}/describe")
            response = self._session.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code != 200:
                return []
            
            data = response.json()
            fields = data.get('fields', [])
            
            return [
                {
                    'name': f['name'],
                    'type': f['type'],
                    'label': f['label']
                }
                for f in fields
            ]
        except Exception as e:
            add_log(f"SalesforceConnector: Failed to get fields for {object_name}: {str(e)}")
            return []
    
    async def get_data(
        self, 
        table_name: str, 
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch data from a Salesforce object using SOQL.
        
        Args:
            table_name: Name of the Salesforce object
            limit: Optional row limit
            fields: Optional list of fields to fetch (defaults to all queryable fields)
            
        Returns:
            pd.DataFrame: The fetched data
            
        Raises:
            DataFetchError: If data fetching fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected:
                raise DataFetchError("Not connected to Salesforce")
            
            # Get fields for the object
            fields_to_query = kwargs.get('fields')
            if not fields_to_query:
                object_fields = await self.get_object_fields(table_name)
                fields_to_query = [f['name'] for f in object_fields if f['type'] not in ['address', 'location']]
            
            if not fields_to_query:
                fields_to_query = ['Id', 'Name']
            
            # Build SOQL query
            fields_str = ', '.join(fields_to_query[:100])  # Limit fields to avoid query length issues
            query = f"SELECT {fields_str} FROM {table_name}"
            
            if limit:
                query += f" LIMIT {limit}"
            else:
                query += " LIMIT 10000"  # Default limit
            
            # Execute query
            url = self._get_api_url("query")
            response = self._session.get(
                url, 
                headers=self._get_headers(),
                params={'q': query},
                timeout=60
            )
            
            if response.status_code != 200:
                raise DataFetchError(f"Query failed: {response.text}")
            
            data = response.json()
            records = data.get('records', [])
            
            # Remove Salesforce metadata from records
            cleaned_records = []
            for record in records:
                clean_record = {k: v for k, v in record.items() if k != 'attributes'}
                cleaned_records.append(clean_record)
            
            df = pd.DataFrame(cleaned_records)
            
            add_log(f"SalesforceConnector: Fetched {len(df)} records from {table_name}")
            return df
        except Exception as e:
            add_log(f"SalesforceConnector: Data fetch failed for {table_name}: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to fetch data from {table_name}: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close the Salesforce connection"""
        await asyncio.sleep(0)
        try:
            self._session.close()
            self._access_token = None
            self._instance_url = None
            self._connected = False
            add_log("SalesforceConnector: Disconnected")
        except Exception as e:
            add_log(f"SalesforceConnector: Error during disconnect: {str(e)}")
