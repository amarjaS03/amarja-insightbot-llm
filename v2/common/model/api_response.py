from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar('T')
class ApiResponse(BaseModel, Generic[T]):
    """API Response model"""
    status: str
    statusCode: int
    message: str
    data: T | None = None