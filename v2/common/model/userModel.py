"""
User Model for FastAPI v2
Model representing a user in the system
"""
from pydantic import BaseModel, Field, model_serializer
from typing import Optional, List
from datetime import datetime


class UserModel(BaseModel):
    """Model for user - matches Firestore structure"""
    uid: str = Field(..., description="User unique identifier (document ID)")
    email: Optional[str] = Field(None, description="User email address")
    name: Optional[str] = Field(None, description="User full name")
    created_at: Optional[datetime] = Field(None, description="User creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="User last update timestamp")
    role: Optional[str] = Field(None, description="User role")
    status: Optional[str] = Field(None, description="User status")
    report_count: Optional[int] = Field(default=0, description="Number of reports created by user")
    issued_token: Optional[int] = Field(default=0, description="Total tokens issued to user")
    used_token: Optional[int] = Field(default=0, description="Total tokens used by user")
    
    # Optional fields that may not exist in Firestore
    emailId: Optional[str] = Field(None, description="User email address (alias for email)")
    first_name: Optional[str] = Field(None, description="User first name (extracted from name)")
    last_name: Optional[str] = Field(None, description="User last name (extracted from name)")
    auth_provider: Optional[str] = Field(None, description="Authentication provider")
    created_by: Optional[int] = Field(None, description="UID of user who created this user")
    registration_type: Optional[str] = Field(None, description="Registration type")
    created_via: Optional[str] = Field(None, description="Method through which user was created")
    token_history: Optional[List[dict]] = Field(default_factory=list, description="History of token usage")
    session_id: Optional[List[str]] = Field(default_factory=list, description="List of session IDs referenced to session collection")
    
    @model_serializer(mode='wrap')
    def serialize_model(self, handler, info):
        """
        Custom serializer that includes only public fields in API responses.
        Fields included: uid, email, name
        """
        include_fields = {
            'uid',
            'email',
            'name'
        }
        
        # Call the default handler first, then filter to include only specified fields
        # This avoids recursion by using the handler instead of model_dump()
        result = handler(self)
        if isinstance(result, dict):
            return {k: v for k, v in result.items() if k in include_fields}
        return result
    
    def model_dump_include_only(self, include_fields: Optional[List[str]] = None) -> dict:
        """
        Return model as dict including only specified fields.
        By default includes: uid, email, name
        """
        default_include = [
            'uid',
            'email',
            'name'
        ]
        
        if include_fields:
            default_include = include_fields
        
        return self.model_dump(include=set(default_include))
    
    class Config:
        from_attributes = True
       
        
