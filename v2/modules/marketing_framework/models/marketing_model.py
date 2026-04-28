"""
Marketing models for FastAPI v2
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any


class MarketingLogRequest(BaseModel):
    """Request model for marketing log data"""
    # Flexible model to accept any marketing log data structure
    # The actual structure depends on what the external API expects
    pass


class MarketingLogResponse(BaseModel):
    """Response model for marketing log submission"""
    success: Optional[bool] = None
    error: Optional[str] = None
    text: Optional[str] = None
    # Additional fields may be present from external API response
