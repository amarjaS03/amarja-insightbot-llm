"""
Admin models for FastAPI v2
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime


class UserModel(BaseModel):
    """User data model"""
    email: str
    name: Optional[str] = None
    role: str = "user"
    issued_token: int = 0
    used_token: int = 0
    report_count: int = 0
    registration_type: Optional[str] = None
    auth_provider: Optional[str] = None
    status: Optional[str] = "active"
    created_at: Optional[str] = None  # ISO format string
    updated_at: Optional[str] = None  # ISO format string
    created_via: Optional[str] = None
    
    class Config:
        from_attributes = True


class UserResponse(BaseModel):
    """Response model for user by email endpoint"""
    authorized: bool = False
    exists: bool = False
    email: Optional[str] = None
    role: Optional[str] = None
    status: Optional[str] = None
    registration_type: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class UserCreateRequest(BaseModel):
    """Request model for creating a user"""
    email: str
    name: str
    role: str
    issued_token: int
    registration_type: Optional[str] = "internal"
    auth_provider: Optional[str] = None
    created_via: Optional[str] = None


class UserUpdateRequest(BaseModel):
    """Request model for updating a user"""
    name: Optional[str] = None
    role: Optional[str] = None
    issued_token: Optional[int] = None
    used_token: Optional[int] = None
    report_count: Optional[int] = None
    registration_type: Optional[str] = None
    auth_provider: Optional[str] = None
    status: Optional[str] = None
    created_via: Optional[str] = None


class AddTokensRequest(BaseModel):
    """Request model for adding tokens to a user"""
    tokens_to_add: int
    reason: Optional[str] = None


class TokenHistoryModel(BaseModel):
    """Token history record model"""
    history_id: str
    user_email: str
    tokens_added: int
    previous_tokens: int
    new_total_tokens: int
    added_by: str
    reason: Optional[str] = None
    created_at: Optional[str] = None  # ISO format string


class AdminStatsResponse(BaseModel):
    """Admin dashboard statistics response"""
    total_users: int
    total_reports: int
    total_tokens_used: int
    active_users: int


class TokenHistoryResponse(BaseModel):
    """Token history response"""
    history: List[TokenHistoryModel]
    total_records: int
