"""
Background tasks for Simple QnA module.

Handles periodic cleanup and maintenance tasks.
"""
import asyncio
from v2.modules.simple_qna.services.chat_job_service import ChatJobService
from v2.common.logger import add_log
import traceback


async def start_chat_cleanup_task(inactivity_hours: int = 6, cleanup_interval: int = 3600):
    """
    Background task to periodically clean up inactive chat services.
    
    This task runs continuously and checks for inactive chats every cleanup_interval seconds.
    Chats that have been inactive for inactivity_hours are closed and their Cloud Run services are deleted.
    
    Similar to how analysis services are cleaned up after execution completes.
    
    Args:
        inactivity_hours: Hours of inactivity before cleanup (default: 6)
        cleanup_interval: Seconds between cleanup checks (default: 3600 = 1 hour)
    """
    chat_service = ChatJobService()
    
    add_log(f"ChatJobService: Started background cleanup task (runs every {cleanup_interval}s, cleans chats inactive for {inactivity_hours}h)")
    
    while True:
        try:
            await asyncio.sleep(cleanup_interval)
            await chat_service.cleanup_inactive_chats(inactivity_hours=inactivity_hours)
        except asyncio.CancelledError:
            add_log("ChatJobService: Background cleanup task cancelled")
            break
        except Exception as e:
            add_log(f"ChatJobService: Error in background cleanup task: {str(e)} | traceback: {traceback.format_exc()}")
            # Continue running even if one cleanup cycle fails
            await asyncio.sleep(60)  # Wait 1 minute before retrying
