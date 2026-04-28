"""
Session Request DTO for FastAPI v2
DTO (Data Transfer Object) for session requests with camelCase field names
"""
from pydantic import BaseModel, Field
from typing import Optional, List


class SessionRequestDTO(BaseModel):
    """Model for session request"""
    userId: str = Field(..., min_length=1, description="User document ID")
    dataSource: Optional[str] = None
    label: Optional[str] = None
    status: Optional[str] = None
    currentStep: Optional[str] = None
    nextStep: Optional[str] = None
    credentialId: Optional[str] = None
    jobIds: Optional[List[str]] = None
    
    class Config:
        from_attributes = True

