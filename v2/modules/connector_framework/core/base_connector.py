"""
Base connector interface for all data source connectors
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    Defines the interface that all connectors must implement.
    """
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize connector with credentials.
        
        Args:
            credentials: Dictionary containing connection credentials
        """
        self._credentials = credentials
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        """Check if connector is currently connected"""
        return self._connected
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """
        Test if the connection is healthy and working.
        
        Returns:
            bool: True if connection is healthy, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get the schema/structure of the data source.
        
        Returns:
            Dict containing:
                - tables: List of table names (for databases)
                - views: List of view names (for databases)
                - objects: List of object names (for APIs like Salesforce)
        """
        pass
    
    @abstractmethod
    async def get_data(
        self, 
        table_name: str, 
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch data from a specific table/object.
        
        Args:
            table_name: Name of the table/object to fetch
            limit: Optional row limit
            **kwargs: Additional parameters (filters, columns, etc.)
            
        Returns:
            pd.DataFrame: The fetched data
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close the connection and clean up resources.
        """
        pass
    
    def get_preview(self, df: pd.DataFrame, num_rows: int = 10) -> Dict[str, Any]:
        """
        Generate a preview of the DataFrame for API responses.
        
        Args:
            df: DataFrame to preview
            num_rows: Number of rows to include in preview
            
        Returns:
            Dict containing preview data
        """
        try:
            top_records = df.head(num_rows).to_dict('records')
            preview_rows = []
            for record in top_records:
                string_record = {}
                for key, value in record.items():
                    if value is None or pd.isna(value):
                        string_record[str(key)] = "null"
                    elif isinstance(value, (dict, list)):
                        string_record[str(key)] = str(value)
                    else:
                        string_record[str(key)] = str(value)
                preview_rows.append(string_record)
            return {"top_records": preview_rows}
        except Exception:
            return {"top_records": []}


class ConnectorError(Exception):
    """Base exception for connector errors"""
    pass


class ConnectionError(ConnectorError):
    """Raised when connection fails"""
    pass


class SchemaError(ConnectorError):
    """Raised when schema retrieval fails"""
    pass


class DataFetchError(ConnectorError):
    """Raised when data fetching fails"""
    pass
