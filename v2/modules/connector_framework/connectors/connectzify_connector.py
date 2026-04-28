import traceback
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import inspect

from Connectzify.api.client import ConnectorClient
from Connectzify.core.state import ConnectionState
from Connectzify.core.exceptions import (
    AuthenticationError,
    ConnectionError as ConnectzifyConnectionError,
    FetchError,
    ConfigurationError,
    ConnectorError as ConnectzifyConnectorError,
)

from v2.common.logger import add_log
from v2.modules.connector_framework.core.base_connector import (
    BaseConnector,
    ConnectionError,
    SchemaError,
    DataFetchError,
)


class ConnectzifyConnector(BaseConnector):
    """
    Connector wrapper around the Connectzify library.
    """

    def __init__(self, connector_type: str, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self._client = ConnectorClient()
        self._connection_id: Optional[str] = None
        self._session = None
        self._connector_type = connector_type
        self._schema_resource = credentials.get("schema_resource") or credentials.get("_schema_resource")

    async def connect(self) -> bool:
        if not self._connector_type:
            raise ConnectionError("Connectzify requires connector_type in credentials")
        try:
            result = await self._client.connect(self._connector_type, self._credentials)
            
            self._connection_id = getattr(result, "connection_id", None)
            self._session = getattr(result, "session", None)
            print("ConnectzifyConnector:",result.to_dict())

            # Mark connected; readiness is checked in test_connection
            # self._connected = True
            add_log(f"ConnectzifyConnector: Connected type={self._connector_type}, status={result.status}")
            
            return result.to_dict()
        except Exception as e:
            self._connected = False
            add_log(f"ConnectzifyConnector: Connection failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Connectzify connection failed: {str(e)}")

    async def test_connection(self) -> bool:
        """
        Test if the connection is ready.
        Returns True if session exists and connection appears ready.
        """
        try:
            if not self._session:
                return False

            # Prefer explicit state checks if available
            if hasattr(self._session, "state"):
                try:
                    state = self._session.state
                    # Check if state is READY or if it's a valid state that allows fetching
                    if state == ConnectionState.READY:
                        return True
                    # For some connectors, other states might still allow fetching
                    # Log the state for debugging
                    add_log(f"ConnectzifyConnector: Session state is {state}, not READY")
                except Exception as e:
                    add_log(f"ConnectzifyConnector: Error checking session state: {str(e)}")

            # Try test_connection method if available
            if hasattr(self._session, "test_connection"):
                try:
                    return await self._session.test_connection()
                except Exception as e:
                    add_log(f"ConnectzifyConnector: Error in session.test_connection(): {str(e)}")

            # Fall back to _connected flag
            return self._connected
        except Exception as e:
            add_log(f"ConnectzifyConnector: Error in test_connection(): {str(e)}")
            return False

    async def get_schema(self) -> Dict[str, Any]:
        try:
            if not self._session:
                raise SchemaError("Not connected")

            # For SQL connectors (mssql/mysql), schema is stored in session's connector config
            # during connect(), not via fetch("schema"). Calling fetch("schema") would try to
            # execute "schema" as SQL, which causes a syntax error.
            if self._connector_type in ["mssql", "mysql"]:
                # Get schema from session's connector config
                session_dict = self._session.to_dict() if hasattr(self._session, 'to_dict') else {}
                
                # Extract tables from session dict
                tables_metadata = session_dict.get("tables", [])
                
                # Transform tables to simple string arrays if needed
                tables = []
                if tables_metadata and isinstance(tables_metadata, list):
                    if len(tables_metadata) > 0 and isinstance(tables_metadata[0], dict):
                        # Extract table_name from objects
                        tables = [item.get("table_name", item) if isinstance(item, dict) else item for item in tables_metadata]
                    else:
                        # Already strings
                        tables = tables_metadata
                
                # Connectzify's SQL connector only stores tables, not views
                # Query views separately using SQLAlchemy inspector (like v1 does)
                views = []
                try:
                    # Access the underlying SQL connector from the session
                    if hasattr(self._session, 'connector') and self._session.connector:
                        connector = self._session.connector
                        # Check if connector has an engine attribute (SQLAlchemy engine)
                        if hasattr(connector, 'engine') and connector.engine:
                            inspector = inspect(connector.engine)
                            try:
                                views = inspector.get_view_names() or []
                            except Exception as view_err:
                                add_log(f"ConnectzifyConnector: Warning - Failed to get views: {str(view_err)}")
                                views = []
                except Exception as e:
                    add_log(f"ConnectzifyConnector: Warning - Failed to access connector engine for views: {str(e)}")
                    views = []
                
                add_log(f"ConnectzifyConnector: Retrieved schema for {self._connector_type} - {len(tables)} tables, {len(views)} views")
                
                return {
                    "tables": tables if isinstance(tables, list) else [],
                    "views": views if isinstance(views, list) else []
                }
            
            # For non-SQL connectors (Salesforce, Acumatica), use fetch("schema")
            resource = self._schema_resource or "schema"
            data = await self._client.fetch(resource)

            if isinstance(data, dict):
                return data
            if isinstance(data, list):
                return {"tables": data}
            return {"objects": [data]}
        except Exception as e:
            add_log(f"ConnectzifyConnector: Schema fetch failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Connectzify schema fetch failed: {str(e)}")

    async def get_data(self, resource: str, connection_id: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Fetch data from the connected data source.
        
        Args:
            resource: Resource identifier (table name, object name, query/URL)
            connection_id: Optional connection_id (for tracking/logging)
            **kwargs: Additional parameters including:
                - subject_area: Subject area for Salesforce/Acumatica
                - limit: Optional limit for number of records
                - where_clause: Optional WHERE clause for Salesforce queries
                - api_version: Optional API version for Acumatica
                - company: Optional company name for Acumatica
            
        Returns:
            DataFrame with fetched data
        """
        try:
            if not self._session:
                raise DataFetchError("Not connected. Please connect first.")
            
            # Check if connection is ready - but don't fail if test_connection returns False
            # The session might still be valid even if test_connection fails
            # We'll let the actual fetch operation determine if the connection is valid
            # Removed test_connection() check - it can be unreliable after first fetch
            # The actual fetch operation will fail with proper error if connection is invalid
            add_log(f"ConnectzifyConnector: Attempting to fetch data for resource '{resource[:100]}...'")
            
            # Extract parameters - remove non-fetch parameters from kwargs
            params = dict(kwargs)
            # Remove parameters that are not passed to fetch
            params.pop("subject_area", None)
            params.pop("where_clause", None)
            params.pop("api_version", None)
            params.pop("company", None)
            # Keep limit and other fetch parameters
            # limit is already in params if provided

            # Fetch data using the resource identifier
            result = await self._client.fetch(resource, **params)
            
            add_log(f"ConnectzifyConnector: Fetched data for resource '{resource[:100]}...'")
            return self._result_to_dataframe(result)
        except FetchError as e:
            add_log(f"ConnectzifyConnector: Fetch error for {resource[:100]}...: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Connectzify data fetch failed for {resource[:100]}...: {str(e)}")
        except Exception as e:
            add_log(f"ConnectzifyConnector: Data fetch failed for {resource[:100]}...: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Connectzify data fetch failed for {resource[:100]}...: {str(e)}")

    async def exchange_code(self, authorization_code: str, state: str) -> Dict[str, Any]:
        """
        Exchange OAuth authorization code for access tokens.
        
        Args:
            authorization_code: Authorization code from OAuth provider
            state: OAuth state parameter for CSRF protection
        
        Returns:
            Dict containing connection result with READY status
        
        Raises:
            ConnectionError: If connection not found or not in active state
            AuthenticationError: If authentication fails
        """
        try:
            if not self._client:
                raise ConnectionError("Client not initialized")
            
            if not authorization_code:
                raise ConnectionError("Authorization code is required")
            
            if not state:
                raise ConnectionError("State parameter is required")
            
            # Exchange authorization code for tokens
            result = await self._client.exchange_code(authorization_code, state)
            
            # Update stored session
            self._session = getattr(result, "session", None)
            self._connection_id = getattr(result, "connection_id", None) or self._connection_id
            
            # Update connection status based on result
            if hasattr(result, "status") and result.status == "READY":
                self._connected = True
            
            add_log(f"ConnectzifyConnector: Code exchanged successfully, status={getattr(result, 'status', 'unknown')}")
            
            return result.to_dict()
            
        except AuthenticationError as e:
            self._connected = False
            add_log(f"ConnectzifyConnector: Authentication error during code exchange: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Connectzify authentication failed: {str(e)}")
        except Exception as e:
            self._connected = False
            add_log(f"ConnectzifyConnector: Error exchanging code: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Connectzify code exchange failed: {str(e)}")

    async def reconnect(self) -> Dict[str, Any]:
        """
        Reconnect a failed or closed connection.
        
        Returns:
            Dict containing connection result with new connection status
        
        Raises:
            ConnectionError: If no active session to reconnect or reconnection fails
        """
        try:
            if not self._session:
                raise ConnectionError("No active session to reconnect")
            
            # Reconnect using the session
            result = await self._session.reconnect()
            
            # Update stored session
            self._session = getattr(result, "session", None)
            self._connection_id = getattr(result, "connection_id", None) or self._connection_id
            
            # Update connection status based on result
            if hasattr(result, "status") and result.status == "READY":
                self._connected = True
            
            add_log(f"ConnectzifyConnector: Reconnected successfully, status={getattr(result, 'status', 'unknown')}")
            
            return result.to_dict()
            
        except Exception as e:
            self._connected = False
            add_log(f"ConnectzifyConnector: Error reconnecting: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Connectzify reconnection failed: {str(e)}")

    async def disconnect(self) -> None:
        try:
            await self._client.close()
        finally:
            self._session = None
            self._connected = False

    def _result_to_dataframe(self, result: Any) -> pd.DataFrame:
        if isinstance(result, pd.DataFrame):
            return result

        if isinstance(result, dict):
            data = result.get("data", result)
        elif isinstance(result, list):
            data = result
        elif result is None:
            data = []
        else:
            data = [result]

        return pd.DataFrame(data)
