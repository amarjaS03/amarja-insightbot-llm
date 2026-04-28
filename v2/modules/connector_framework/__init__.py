"""
Connector framework module for FastAPI v2

Usage:
    from v2.modules.connector_framework import ConnectorManager, ConnectorFactory
    
    manager = ConnectorManager()
    result = await manager._connect(ConnectorConnectRequest(...))
    schema = await manager._fetch_schema(ConnectorActionRequest(...))
    data = await manager._fetch_data(ConnectorActionRequest(...))
"""
from v2.modules.connector_framework.factory.factory import ConnectorFactory
from v2.modules.connector_framework.manager.connector.connector_manager import ConnectorManager
from v2.modules.connector_framework.core.base_connector import (
    BaseConnector, ConnectorError, ConnectionError, SchemaError, DataFetchError
)
from v2.modules.connector_framework.models.connector_models import (
    ConnectorConnectRequest,
    ConnectorActionRequest,
    ConnectorResponse,
    ConnectorType,
)

__all__ = [
    "ConnectorFactory",
    "ConnectorManager",
    "BaseConnector",
    "ConnectorError",
    "ConnectionError",
    "SchemaError",
    "DataFetchError",
    "ConnectorConnectRequest",
    "ConnectorActionRequest",
    "ConnectorResponse",
    "ConnectorType",
]
