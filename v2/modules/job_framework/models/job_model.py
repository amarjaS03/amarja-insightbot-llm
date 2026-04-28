"""
Job models for FastAPI v2
Matches Firestore schema exactly
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class JobStatus(str, Enum):
    """Job status matching schema"""
    PENDING = "pending"
    INITIALIZED = "initialized"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"
    CANCELLED = "cancelled"


class JobType(str, Enum):
    """Job type matching schema and handler registry"""
    DATA_FETCH = "data_fetch"
    DOMAIN_DICTIONARY = "domain_dictionary"
    ANALYSIS = "analysis"
    QNA = "qna"
    # Legacy aliases
    FETCH_DATA = "data_fetch"
    CREATE_DOMAIN = "domain_dictionary"
    ANALYSIS_JOB = "analysis"
    QNA_JOB = "qna"


class JobCreate(BaseModel):
    """Model for creating a job"""
    user_id: str = Field(..., description="Owner of job (FK → USERS.uid) - UUID string")
    session_id: str = Field(..., description="Session this job belongs to (FK → SESSIONS.session_id) - UUID string")
    job_type: str = Field(..., min_length=1, description="Type of job (fetch_data/create_domain/analysis_job/qna_job)")
    label: str = Field(..., min_length=1, description="Job label/name")
    job_config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Job-specific configuration parameters")
    execution_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Execution details (container_port/input_dir/output_dir/model/query)")


class JobUpdate(BaseModel):
    """Model for updating a job"""
    status: Optional[JobStatus] = None
    label: Optional[str] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    execution_metadata: Optional[Dict[str, Any]] = None
    result_metadata: Optional[Dict[str, Any]] = None
    job_config: Optional[Dict[str, Any]] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobResponse(BaseModel):
    """Model for job response - matches Firestore schema exactly"""
    job_id: str
    user_id: str  # Changed from int to str (UUID string)
    session_id: str
    job_type: str
    status: JobStatus
    label: str
    created_on: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    execution_metadata: Optional[Dict[str, Any]] = None
    result_metadata: Optional[Dict[str, Any]] = None
    job_config: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True


class MilestoneCreate(BaseModel):
    """Model for creating a milestone (subcollection of JOBS)"""
    job_id: str = Field(..., description="ID of the job this milestone belongs to")
    name: str = Field(..., min_length=1, description="Name of the milestone")
    description: Optional[str] = Field(None, description="Optional description")
    data: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Optional data to store")
    job_status: Optional[str] = Field(None, description="Current job status at this milestone point (auto-set if not provided)")


class MilestoneUpdate(BaseModel):
    """Model for updating a milestone"""
    name: Optional[str] = None
    description: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    job_status: Optional[str] = Field(None, description="Job status at this milestone point")


class MilestoneResponse(BaseModel):
    """Model for milestone response"""
    milestone_id: str
    job_id: str
    name: str
    description: Optional[str] = None
    job_status: Optional[str] = Field(None, description="Job status at this milestone point")
    created_at: datetime
    updated_at: datetime
    data: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True
