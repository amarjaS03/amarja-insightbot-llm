"""
Authentication Middleware for FastAPI v2
"""
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from common.logger import add_log

# Paths that don't require API key validation (e.g., health checks, docs)
EXCLUDED_PATHS = ["/docs", "/redoc", "/openapi.json", "/"]


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Middleware to validate API key for all requests"""
    
    def __init__(self, app, api_key: str):
        super().__init__(app)
        self.api_key = api_key
        self.excluded_paths = EXCLUDED_PATHS
    
    async def dispatch(self, request: Request, call_next):
        # Skip API key validation for excluded paths
        request_path = request.url.path
        
        # Check for exact matches first
        if request_path in self.excluded_paths:
            return await call_next(request)
        
        # Check if path starts with any excluded path (for sub-paths like /docs/static)
        # But exclude "/" from this check to avoid matching all paths
        for excluded_path in self.excluded_paths:
            if excluded_path != "/" and (request_path == excluded_path or request_path.startswith(excluded_path + "/")):
                return await call_next(request)
        
        # Get API key from headers (check multiple header names)
        api_key_header = (
            request.headers.get("X-API-Key") or 
            request.headers.get("API-Key") or 
            request.headers.get("x-api-key") or 
            request.headers.get("api-key")
        )
        
        # Validate API key
        if not api_key_header:
            add_log(f"API key validation failed: No API key provided for path {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "result": "fail",
                    "status_code": 401,
                    "message": "API key is required",
                    "error": "Unauthorized"
                }
            )
        
        if api_key_header != self.api_key:
            add_log(f"API key validation failed: Invalid API key provided for path {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "result": "fail",
                    "status_code": 401,
                    "message": "Invalid API key",
                    "error": "Unauthorized"
                }
            )
        
        return await call_next(request)

