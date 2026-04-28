"""
Simple QnA Models for FastAPI v2

Defines data models for chat-based QnA functionality:
- Chat = Long-running job
- Query = Individual question/answer pair within a chat
"""
from pydantic import BaseModel, Field, model_validator
from typing import Optional, Dict, Any, List
from datetime import datetime


class ChatCreate(BaseModel):
    """Model for creating a new chat"""
    user_id: str = Field(..., min_length=1, description="User ID (UUID string)")
    session_id: str = Field(..., min_length=1, description="Session ID (UUID string)")
    label: Optional[str] = Field(None, description="Optional chat label/name")
    model: Optional[str] = Field(default="gpt-5.4", description="Optional LLM model to use (defaults to gpt-5.4)")
    query: Optional[Dict[str, Any]] = Field(None, description="Optional nested object (legacy); user_id/session_id can be extracted from here")

    @model_validator(mode="before")
    @classmethod
    def flatten_query_if_needed(cls, data: Any) -> Any:
        """Support nested { query: { user_id, session_id } } from frontend (avoids 422 on first call)."""
        if not isinstance(data, dict):
            return data
        q = data.get("query")
        if isinstance(q, dict):
            if not data.get("user_id") and q.get("user_id"):
                data = {**data, "user_id": q["user_id"]}
            if not data.get("session_id") and q.get("session_id"):
                data = {**data, "session_id": q["session_id"]}
        return data


class ChatResponse(BaseModel):
    """Model for chat response"""
    chat_id: str = Field(..., description="Chat ID (same as job_id)")
    user_id: str = Field(..., description="User ID")
    session_id: str = Field(..., description="Session ID")
    status: str = Field(..., description="Chat status: pending, running, completed, cancelled, error")
    label: Optional[str] = Field(None, description="Chat label")
    total_queries: int = Field(default=0, description="Total number of queries in chat")
    created_on: datetime = Field(..., description="Chat creation timestamp")
    last_query_at: Optional[datetime] = Field(None, description="Last query timestamp")
    model: str = Field(default="gpt-5.4", description="LLM model used")
    
    class Config:
        from_attributes = True


class QueryCreate(BaseModel):
    """Model for creating a new query within a chat"""
    # Note: chat_id comes from the URL path parameter, model defaults to "gpt-5.4"
    query: str = Field(..., min_length=1, description="User question")


class QueryResponse(BaseModel):
    """Model for query response"""
    query_id: str = Field(..., description="Query ID (UUID string)")
    chat_id: str = Field(..., description="Chat ID")
    query: str = Field(..., description="User question")
    answer: Optional[str] = Field(None, description="HTML answer")
    query_number: int = Field(..., description="Sequential query number (1, 2, 3...)")
    status: str = Field(..., description="Query status: pending, running, completed, error")
    created_at: datetime = Field(..., description="Query creation timestamp")
    completed_at: Optional[datetime] = Field(None, description="Query completion timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Query metadata (metrics, errors, etc.)")
    
    class Config:
        from_attributes = True


class ChatListResponse(BaseModel):
    """Model for listing chats"""
    chats: List[ChatResponse] = Field(..., description="List of chats")
    total: int = Field(..., description="Total number of chats")


class QueryListResponse(BaseModel):
    """Model for listing queries in a chat"""
    queries: List[QueryResponse] = Field(..., description="List of queries")
    total: int = Field(..., description="Total number of queries")
    chat_id: str = Field(..., description="Chat ID")
