"""
Job Info Model for FastAPI v2
Simplified job information included in session responses
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class JobInfo(BaseModel):
    """Simplified job information for session responses"""
    job_id: str = Field(..., description="Job ID (UUID string)")
    job_type: str = Field(..., description="Type of job (e.g., 'file_upload', 'domain_dictionary', 'simple_qna_chat')")
    status: str = Field(..., description="Job status (e.g., 'pending', 'running', 'completed', 'failed', 'cancelled')")
    label: str = Field(..., description="Job label/name")
    createdOn: Optional[datetime] = Field(None, description="Job creation timestamp")
    
    class Config:
        from_attributes = True

