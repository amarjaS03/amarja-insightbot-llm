"""
MySQL Connector - Real implementation using SQLAlchemy
"""
import asyncio
import urllib.parse
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from v2.modules.connector_framework.core.base_connector import (
    BaseConnector, ConnectionError, SchemaError, DataFetchError
)
from v2.common.logger import add_log
import traceback


class MySQLConnector(BaseConnector):
    """
    MySQL Connector using SQLAlchemy with pymysql driver.
    
    Required credentials:
        - host: Database server hostname
        - port: Database server port (default: 3306)
        - username: Database username
        - password: Database password
        - database: Database name
    """
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize MySQL connector.
        
        Args:
            credentials: Dict containing host, port, username, password, database
        """
        super().__init__(credentials)
        self._engine: Optional[Engine] = None
        self._connection_url: Optional[str] = None
    
    def _build_connection_url(self) -> str:
        """Build SQLAlchemy connection URL from credentials"""
        host = self._credentials.get('host', '')
        port = self._credentials.get('port', 3306)
        username = self._credentials.get('username', '')
        password = self._credentials.get('password', '')
        database = self._credentials.get('database', '')
        
        # URL encode password to handle special characters
        encoded_password = urllib.parse.quote_plus(password)
        
        # Use pymysql driver
        connection_url = f"mysql+pymysql://{username}:{encoded_password}@{host}:{port}/{database}"
        return connection_url
    
    async def connect(self) -> bool:
        """
        Establish connection to MySQL database.
        
        Returns:
            bool: True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        await asyncio.sleep(0)  # Maintain async interface
        try:
            self._connection_url = self._build_connection_url()
            self._engine = create_engine(self._connection_url)
            
            # Test the connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._connected = True
            add_log(f"MySQLConnector: Successfully connected to {self._credentials.get('host')}")
            return True
        except Exception as e:
            self._connected = False
            add_log(f"MySQLConnector: Connection failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to MySQL: {str(e)}")
    
    async def test_connection(self) -> bool:
        """
        Test if the connection is healthy.
        
        Returns:
            bool: True if connection is healthy
        """
        await asyncio.sleep(0)
        try:
            if not self._engine:
                return False
            
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            add_log(f"MySQLConnector: Connection test failed: {str(e)}")
            self._connected = False
            return False
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get database schema (tables and views).
        
        Returns:
            Dict containing 'tables' and 'views' lists
            
        Raises:
            SchemaError: If schema retrieval fails
        """
        await asyncio.sleep(0)
        try:
            if not self._engine:
                raise SchemaError("Not connected to database")
            
            inspector = inspect(self._engine)
            
            # Get tables
            try:
                tables = inspector.get_table_names() or []
            except Exception:
                tables = []
            
            # Get views
            try:
                views = inspector.get_view_names() or []
            except Exception:
                views = []
            
            add_log(f"MySQLConnector: Retrieved schema - {len(tables)} tables, {len(views)} views")
            return {
                "tables": tables,
                "views": views
            }
        except Exception as e:
            add_log(f"MySQLConnector: Schema retrieval failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Failed to get schema: {str(e)}")
    
    async def get_data(
        self, 
        table_name: str, 
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch data from a table.
        
        Args:
            table_name: Name of the table to fetch
            limit: Optional row limit
            
        Returns:
            pd.DataFrame: The fetched data
            
        Raises:
            DataFetchError: If data fetching fails
        """
        await asyncio.sleep(0)
        try:
            if not self._engine:
                raise DataFetchError("Not connected to database")
            
            # Get the identifier preparer for safe quoting
            preparer = self._engine.dialect.identifier_preparer
            quoted_table = preparer.quote(table_name)
            
            # Build query
            if limit:
                query = f"SELECT * FROM {quoted_table} LIMIT {limit}"
            else:
                query = f"SELECT * FROM {quoted_table}"
            
            # Execute query
            df = pd.read_sql_query(query, con=self._engine)
            
            add_log(f"MySQLConnector: Fetched {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            add_log(f"MySQLConnector: Data fetch failed for {table_name}: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to fetch data from {table_name}: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close the database connection"""
        await asyncio.sleep(0)
        try:
            if self._engine:
                self._engine.dispose()
                self._engine = None
            self._connected = False
            add_log("MySQLConnector: Disconnected")
        except Exception as e:
            add_log(f"MySQLConnector: Error during disconnect: {str(e)}")

