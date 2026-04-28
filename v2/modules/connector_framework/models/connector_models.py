"""
Connector models for FastAPI v2
"""
from typing import Any, Dict, Optional, List
from pydantic import BaseModel


class ConnectorConnectRequest(BaseModel):
    """Request model for connecting to a data source"""
    type: str  # mssql, mysql, salesforce, salesforce-oauth, acumatica, sample_data, file_upload, n8n
    user_id: str
    session_id: str
    cred_id: Optional[str] = None  # Optional credential ID for direct lookup
    params: Optional[Dict[str, Any]] = None  # Additional connection parameters
    # For salesforce-oauth: pass redirect_uri (e.g. window.location.origin + "/salesforce/oauth/callback")
    # so OAuth callback lands where the user started (avoids "No session ID found" when constants use dev URL)


class ConnectorOAuthCallbackRequest(BaseModel):
    """Request model for OAuth callback (Salesforce and Acumatica only)"""
    session_id: str
    user_id: str
    connector_type: str  # Must be 'salesforce' or 'acumatica'
    code: str  # Authorization code from OAuth provider
    state: str  # OAuth state parameter for CSRF protection


class ConnectorActionRequest(BaseModel):
    """Request model for connector actions (fetch schema, fetch data, etc.)"""
    user_id: str
    session_id: str
    connector_type: str
    subject_area: Optional[str] = None
    tables: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None
    # params can contain:
    # - tables: List[str] - for fetch_data
    # - filename: str - for copy_sample_data
    # - url: str - for fetch_n8n_data
    # - limit: int - for limiting results
    # - fields: List[str] - for specifying columns

class N8nDownloadCsvRequest(BaseModel):
    """Request model for downloading a CSV file from the n8n"""
    user_id: str
    session_id: str
    url: str


class CopySampleDataRequest(BaseModel):
    """Request model for copying sample data"""
    user_id: str
    session_id: str
    params: Dict[str, Any]  # Should contain "filename": str


class FetchN8nDataRequest(BaseModel):
    """Request model for fetching n8n data"""
    user_id: str
    connector_type: str  # Should be "n8n"
    session_id: str
    params: Dict[str, Any]  # Should contain "url": str

class ConnectorResponse(BaseModel):
    """Generic response model for connector operations"""
    result: str  # success or fail
    message: str
    data: Optional[Any] = None


class SchemaResponse(BaseModel):
    """Response model for schema retrieval"""
    tables: Optional[List[str]] = None
    views: Optional[List[str]] = None
    objects: Optional[List[str]] = None  # For Salesforce/Acumatica
    datasets: Optional[List[Dict[str, Any]]] = None  # For sample_data


class DataFetchResponse(BaseModel):
    """Response model for data fetching"""
    session_id: str
    saved_files: List[str]
    preview_data: Dict[str, Any]


class SampleDataCopyResponse(BaseModel):
    """Response model for sample data copy operation"""
    data_file: str
    domain_file: str
    suggested_questions: List[str]
    suggested_questions_simple: List[str]


class ConnectorHealthResponse(BaseModel):
    """Response model for connector health check"""
    healthy: bool
    session_id: str
    connector_type: Optional[str] = None


# Connection type constants
class ConnectorType:
    """Supported connector types"""
    MSSQL = "mssql"
    MYSQL = "mysql"
    SALESFORCE = "salesforce"
    SALESFORCE_OAUTH = "salesforce-oauth"  # Direct OAuth flow initiation
    ACUMATICA = "acumatica"
    SAMPLE_DATA = "sample_data"
    FILE_UPLOAD = "file_upload"
    N8N = "n8n"
    
    @classmethod
    def all_types(cls) -> List[str]:
        return [cls.MSSQL, cls.MYSQL, cls.SALESFORCE, cls.SALESFORCE_OAUTH, cls.ACUMATICA, cls.SAMPLE_DATA, cls.FILE_UPLOAD, cls.N8N]
    
    @classmethod
    def database_types(cls) -> List[str]:
        return [cls.MSSQL, cls.MYSQL]
    
    @classmethod
    def api_types(cls) -> List[str]:
        return [cls.SALESFORCE, cls.SALESFORCE_OAUTH, cls.ACUMATICA]
    
    @classmethod
    def file_types(cls) -> List[str]:
        return [cls.FILE_UPLOAD, cls.SAMPLE_DATA, cls.N8N]
