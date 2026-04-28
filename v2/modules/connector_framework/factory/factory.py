"""
Connector Factory for V2 Framework
Creates connector instances based on type
"""
from typing import Dict, Any

from v2.modules.connector_framework.core.base_connector import BaseConnector
from v2.modules.connector_framework.connectors.connectzify_connector import ConnectzifyConnector
from v2.modules.connector_framework.connectors.sample_data_connector import SampleDataConnector
from v2.modules.connector_framework.connectors.file_upload_connector import FileUploadConnector
from v2.modules.connector_framework.connectors.n8n_connector import N8nConnector
from v2.modules.connector_framework.models.connector_models import ConnectorType


class ConnectorFactory:
    """
    Factory that creates connectors based on type.
    
    Supported types:
        - mssql: Microsoft SQL Server
        - mysql: MySQL database
        - salesforce: Salesforce CRM (uses saved credentials)
        - salesforce-oauth: Salesforce CRM OAuth flow initiation (handled before factory)
        - acumatica: Acumatica ERP
        - sample_data: Pre-loaded sample datasets
        - file_upload: User-uploaded CSV/Excel files
        - n8n: N8N connector for fetching CSV data from URLs
    
    Note: salesforce-oauth is handled in ConnectorManager._connect() before factory is called.
    """
    
    # Registry of connector classes
    _connectors: Dict[str, type] = {
        ConnectorType.MSSQL: ConnectzifyConnector,
        ConnectorType.MYSQL: ConnectzifyConnector,
        ConnectorType.SALESFORCE: ConnectzifyConnector,
        ConnectorType.ACUMATICA: ConnectzifyConnector,
        ConnectorType.SAMPLE_DATA: SampleDataConnector,
        ConnectorType.FILE_UPLOAD: FileUploadConnector,
        ConnectorType.N8N: N8nConnector,
    }

    @staticmethod
    def create_connector(connector_type: str, credentials: Dict[str, Any]) -> BaseConnector:
        """
        Create a connector instance based on type.
        
        Args:
            connector_type: Type of connector (mssql, mysql, salesforce, acumatica, sample_data, file_upload, n8n)
            credentials: Dictionary containing connection credentials
            
        Returns:
            BaseConnector: An instance of the appropriate connector
            
        Raises:
            ValueError: If connector_type is not supported
        """
        normalized_type = (connector_type or "").lower().strip()
        
        connector_class = ConnectorFactory._connectors.get(normalized_type)
        
        if connector_class is None:
            supported = ", ".join(ConnectorFactory._connectors.keys())
            raise ValueError(f"Unknown connector type: '{connector_type}'. Supported types: {supported}")
        
        # ConnectzifyConnector requires connector_type as first parameter
        if connector_class == ConnectzifyConnector:
            return connector_class(normalized_type, credentials)
        
        return connector_class(credentials)
    
    @staticmethod
    def get_supported_types() -> list:
        """
        Get list of supported connector types.
        
        Returns:
            list: List of supported connector type strings
        """
        return list(ConnectorFactory._connectors.keys())
    
    @staticmethod
    def register_connector(connector_type: str, connector_class: type) -> None:
        """
        Register a new connector type.
        
        Args:
            connector_type: Type identifier for the connector
            connector_class: The connector class to register
        """
        ConnectorFactory._connectors[connector_type.lower()] = connector_class
