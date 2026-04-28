"""
Marketing Controller for FastAPI v2
Provides REST API endpoints for marketing campaign operations
"""
from typing import Dict, Any
from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from v2.modules.marketing_framework.services.marketing.marketing_service import MarketingService
from v2.common.logger import add_log
import traceback

router = APIRouter()


def get_marketing_service() -> MarketingService:
    """Dependency to get marketing service instance"""
    return MarketingService()


@router.post("/marketing_campaigns_logs", response_model=None)
async def post_marketing_campaigns_logs(
    data: Dict[str, Any] = Body(...),
    service: MarketingService = Depends(get_marketing_service)
):
    """
    Forward marketing campaign logs to external API (no auth required)
    
    This endpoint accepts marketing log data and forwards it to the configured
    external marketing campaigns API. No authentication is required to match v1 behavior.
    """
    try:
        result, status_code = service.post_marketing_log(data)
        return JSONResponse(content=result, status_code=status_code)
    except Exception as e:
        add_log(f"MarketingController: Error posting marketing logs: {str(e)} | traceback: {traceback.format_exc()}")
        return JSONResponse(
            content={"error": "Failed to send marketing campaign log"},
            status_code=500
        )
