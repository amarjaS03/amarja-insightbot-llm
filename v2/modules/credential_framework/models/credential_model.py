"""
Credential models for FastAPI v2
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime


class DatabaseCredentials(BaseModel):
    """Model for database connection credentials"""
    host: str
    port: int
    username: str
    password: str
    database: str


class SalesforceCredentials(BaseModel):
    """Model for Salesforce OAuth credentials"""
    instance_url: str
    access_token: str
    refresh_token: Optional[str] = None
    token_type: Optional[str] = "Bearer"


class AcumaticaCredentials(BaseModel):
    """Model for Acumatica OAuth credentials"""
    tenant_url: str
    access_token: str
    refresh_token: Optional[str] = None
    company: Optional[str] = None


class CredentialCreate(BaseModel):
    """Model for creating a credential"""
    user_id: str
    user_email: Optional[str] = None
    connection_type: str  # mssql, mysql, salesforce, acumatica, sample_data
    connection_name: str  # User-friendly name for this connection
    credentials: Dict[str, Any]  # The actual credential data (host, port, username, etc.)
    session_id: Optional[str] = None  # Optional session association
    

class CredentialUpdate(BaseModel):
    """Model for updating a credential"""
    connection_name: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    status: Optional[str] = None


class CredentialResponse(BaseModel):
    """Model for credential response"""
    credential_id: str
    user_id: str
    user_email: Optional[str] = None
    connection_type: str
    connection_name: str
    credentials: Optional[Dict[str, Any]] = None  # Excluded in some responses for security
    session_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    status: Optional[str] = "active"
    
    class Config:
        from_attributes = True


class CredentialListResponse(BaseModel):
    """Model for listing credentials (without sensitive data)"""
    credential_id: str
    user_id: str
    user_email: Optional[str] = None
    connection_type: str
    connection_name: str
    session_id: Optional[str] = None
    created_at: Optional[datetime] = None
    status: Optional[str] = "active"
    
    class Config:
        from_attributes = True

from typing import Generic, TypeVar, Optional
from pydantic import BaseModel
T = TypeVar('T')

class CredentialApiResponse(BaseModel, Generic[T]):
    """API Response model"""
    status: str
    statusCode: int
    message: str
    connector_type: Optional[str] = None
    data: T | None = None