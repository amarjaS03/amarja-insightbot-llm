"""
Marketing Service for FastAPI v2
Handles marketing campaign log operations
"""
from typing import Dict, Any, Tuple
from v2.utils.marketingCampaign import marketing_campaign_logs
from v2.common.logger import add_log


class MarketingService:
    """Service layer for marketing operations"""
    
    def __init__(self):
        """Initialize marketing service"""
        add_log("MarketingService: Initialized")
    
    def post_marketing_log(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        """
        Forward marketing log data to external API
        
        Args:
            data: Marketing log data dictionary
            
        Returns:
            Tuple of (response_body, status_code)
        """
        try:
            result, status_code = marketing_campaign_logs(data)
            add_log(f"MarketingService: Successfully posted marketing log, status: {status_code}")
            return result, status_code
        except Exception as e:
            add_log(f"MarketingService: Error posting marketing log: {str(e)}")
            return {"error": "Failed to send marketing campaign log"}, 500
