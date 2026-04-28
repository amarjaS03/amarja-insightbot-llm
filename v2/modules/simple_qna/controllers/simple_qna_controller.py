"""
Simple QnA Controller for FastAPI v2

Provides chat-based QnA functionality:
- 1 Chat = 1 Long-Running Job
- Multiple queries per chat stored in Firestore subcollection
- Each query executes via Cloud Run (ephemeral)
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends, Body
from v2.common.model.api_response import ApiResponse
from v2.common.logger import add_log
from v2.modules.simple_qna.models.simple_qna_models import (
    ChatCreate, ChatResponse, QueryCreate, QueryResponse,
    ChatListResponse, QueryListResponse
)
from v2.modules.simple_qna.services.chat_job_service import ChatJobService
from v2.modules.simple_qna.services.query_store_service import QueryStoreService
from v2.modules.simple_qna.services.simple_qna_execution_service import SimpleQnaExecutionService
from v2.modules.job_framework.manager.job.job_manager import JobManager
from v2.modules.job_framework.models.job_model import JobUpdate
import traceback


router = APIRouter(prefix="/simple-qna", tags=["Simple QnA"])


def get_chat_job_service() -> ChatJobService:
    """Dependency to get chat job service instance"""
    return ChatJobService()


def get_query_store_service() -> QueryStoreService:
    """Dependency to get query store service instance"""
    return QueryStoreService()


def get_execution_service() -> SimpleQnaExecutionService:
    """Dependency to get execution service instance"""
    return SimpleQnaExecutionService()


def get_job_manager() -> JobManager:
    """Dependency to get job manager instance"""
    return JobManager()


@router.get("/chats", response_model=ApiResponse[Dict[str, Any]])
async def list_chats(
    user_id: str = Query(..., description="User ID"),
    session_id: str = Query(..., description="Session ID"),
    chat_service: ChatJobService = Depends(get_chat_job_service)
):
    """
    Get all chat IDs for a specific user and session.
    
    Returns a list of all chats (chat_ids) that exist for the given user_id and session_id.
    """
    try:
        chats = await chat_service.list_chats_by_user_session(user_id, session_id)
        chat_ids = [chat.chat_id for chat in chats]
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message=f"Retrieved {len(chat_ids)} chats",
            data={
                "chat_ids": chat_ids,
                "chats": [chat.dict() for chat in chats],
                "total": len(chat_ids),
                "user_id": user_id,
                "session_id": session_id
            }
        )
    except Exception as e:
        error_msg = f"Failed to list chats: {str(e)} | traceback: {traceback.format_exc()}"
        add_log(f"SimpleQnA: {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/chats", response_model=ApiResponse[ChatResponse])
async def create_chat(
    chat_data: ChatCreate,
    chat_service: ChatJobService = Depends(get_chat_job_service)
):
    """
    Create a new chat (long-running job).
    
    A chat is a container for multiple queries. Each chat persists as a job
    in the Job Framework and can accept multiple queries sequentially.
    
    Required fields:
    - user_id: User ID (UUID string)
    - session_id: Session ID (UUID string)
    
    Optional fields:
    - label: Chat label/name (defaults to "Chat: {user_id[:8]}")
    - model: LLM model to use (defaults to "gpt-5.4")
    """
    try:
        chat = await chat_service.create_chat_job(chat_data)
        add_log(f"SimpleQnA: Created chat {chat.chat_id}")
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Chat created successfully",
            data=chat
        )
    except Exception as e:
        error_msg = f"Failed to create chat: {str(e)} | traceback: {traceback.format_exc()}"
        add_log(f"SimpleQnA: {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("/chats/{chat_id}", response_model=ApiResponse)
async def delete_chat(
    chat_id: str,
    chat_service: ChatJobService = Depends(get_chat_job_service)
):
    """
    Delete a chat and all its data.
    
    Permanently removes:
    - Cloud Run service for the chat
    - All queries (Q&A history)
    - Job record and session reference
    
    Use with caution - this operation cannot be undone.
    """
    try:
        success, error_msg = await chat_service.delete_chat(chat_id)
        if not success:
            if "not found" in (error_msg or "").lower():
                raise HTTPException(status_code=404, detail=error_msg or "Chat not found")
            raise HTTPException(status_code=400, detail=error_msg or "Failed to delete chat")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Chat deleted successfully",
            data={"chat_id": chat_id}
        )
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to delete chat: {str(e)} | traceback: {traceback.format_exc()}"
        add_log(f"SimpleQnA: {error_msg}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chats/{chat_id}/queries", response_model=ApiResponse[QueryResponse])
async def add_query(
    chat_id: str,
    query_data: QueryCreate,
    execution_service: SimpleQnaExecutionService = Depends(get_execution_service),
    query_store: QueryStoreService = Depends(get_query_store_service),
    chat_service: ChatJobService = Depends(get_chat_job_service),
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    Add a query to a chat and execute it.
    
    Required:
    - chat_id: Path parameter (chat ID)
    - query: Request body (user question)
    
    Optional:
    - model: Defaults to "gpt-5.4" (uses chat's model if available)
    
    This endpoint:
    1. Validates the chat is open (or creates new if needed)
    2. Checks query count - auto-rolls over if limit reached
    3. Creates a query document
    4. Executes the query via Cloud Run (asynchronous)
    5. Returns the query document
    
    The query execution happens in the background. Use GET /chats/{chat_id}/queries/{query_id}
    to check the status and retrieve the answer.
    
    If chat reaches MAX_QUERIES_PER_CHAT (30), it will be automatically closed and a new
    chat will be created. The response will include rollover information.
    """
    try:
        # chat_id comes from path parameter only (no need to validate against body)
        # Execute query (asynchronous - returns immediately)
        # Returns (query_id, rolled_over)
        try:
            # Get default model from environment variable
            import os
            default_model = os.getenv("MODEL_NAME", "gpt-5.4")
            
            # Get chat to retrieve model (uses chat's model if available, otherwise MODEL_NAME from env)
            chat = await chat_service.get_chat_job(chat_id)
            model = chat.model if chat else default_model
            
            query_id, rolled_over = await execution_service.execute_query(
                chat_id=chat_id,
                query=query_data.query,
                model=model
            )
            
            if not query_id:
                raise HTTPException(
                    status_code=400,
                    detail="Failed to start query execution. Chat may be closed or invalid."
                )
        except ValueError as e:
            # ValueError from validation failures (chat not found, not open, etc.)
            raise HTTPException(
                status_code=400,
                detail=str(e)
            )
        except RuntimeError as e:
            # RuntimeError from execution setup failures
            raise HTTPException(
                status_code=400,
                detail=str(e)
            )
        
        # Get the actual chat_id (may have changed due to rollover)
        # Re-fetch query to get current chat_id
        query_doc = await query_store.get_query(query_id, chat_id)
        if not query_doc:
            raise HTTPException(
                status_code=500,
                detail="Query created but could not be retrieved"
            )
        
        # Update chat_id if rolled over
        actual_chat_id = query_doc.chat_id
        
        # Update job label if this is the first query (query_number == 1)
        if query_doc.query_number == 1:
            try:
                # Update job (chat) label to the first query text
                job_update = JobUpdate(label=query_data.query)
                await job_manager._update_job(actual_chat_id, job_update)
                add_log(f"SimpleQnA: Updated job {actual_chat_id} label to first query: '{query_data.query[:50]}...'")
            except Exception as e:
                # Log error but don't fail the query if label update fails
                add_log(f"SimpleQnA: Warning - Failed to update job label for first query: {str(e)} | traceback: {traceback.format_exc()}")
        
        add_log(f"SimpleQnA: Created and started execution for query {query_id} in chat {actual_chat_id} (rolled_over={rolled_over})")
        
        # Re-fetch query to get updated status and answer (execution completed synchronously)
        updated_query_doc = await query_store.get_query(query_id, actual_chat_id)
        if not updated_query_doc:
            raise HTTPException(
                status_code=500,
                detail="Query executed but could not be retrieved"
            )
        
        # Include rollover info in response message
        message = "Query executed successfully"
        if rolled_over:
            message += f" (chat rolled over to {actual_chat_id})"
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message=message,
            data=updated_query_doc
        )
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to add query: {str(e)} | traceback: {traceback.format_exc()}"
        add_log(f"SimpleQnA: {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/chats/{chat_id}/queries", response_model=ApiResponse[QueryListResponse])
async def get_chat_queries(
    chat_id: str,
    order_direction: str = Query(default="asc", description="Order direction: 'asc' (oldest first) or 'desc' (newest first)"),
    query_store: QueryStoreService = Depends(get_query_store_service)
):
    """
    Get all queries for a chat.
    
    Returns queries ordered by query_number (sequential order).
    """
    try:
        if order_direction not in ['asc', 'desc']:
            raise HTTPException(
                status_code=400,
                detail="order_direction must be 'asc' or 'desc'"
            )
        
        queries = await query_store.list_chat_queries(chat_id, order_direction=order_direction)
        
        response = QueryListResponse(
            queries=queries,
            total=len(queries),
            chat_id=chat_id
        )
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message=f"Retrieved {len(queries)} queries",
            data=response
        )
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to get queries: {str(e)} | traceback: {traceback.format_exc()}"
        add_log(f"SimpleQnA: {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


