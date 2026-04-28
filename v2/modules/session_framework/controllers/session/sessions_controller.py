"""
Sessions controller for FastAPI v2
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from v2.common.model.api_response import ApiResponse
from v2.common.model.sessionModel import SessionModel
from v2.modules.session_framework.controllers.session.dtos import SessionRequestDTO
from v2.modules.session_framework.manager.session.session_manager import SessionManager

router = APIRouter(prefix="/sessions")

# Constants
SESSION_NOT_FOUND_MSG = "Session not found"


def get_session_manager() -> SessionManager:
    """Dependency to get session manager instance"""
    return SessionManager()


@router.post("/", response_model=ApiResponse[SessionModel])
async def create_session(session_data: SessionRequestDTO, manager: SessionManager = Depends(get_session_manager)):
    """Create a new session"""
    try:
        session = await manager._create_session(session_data)
        return ApiResponse[SessionModel](status="success", statusCode=200, message="Session created successfully", data=session)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/getByUserId/{user_id}", response_model=ApiResponse[List[SessionModel]])
async def get_sessions_by_user(user_id: str, manager: SessionManager = Depends(get_session_manager)):
    """Get all sessions for a specific user"""
    try:
        sessions = await manager._get_sessions_by_user_id(user_id)
        return ApiResponse[List[SessionModel]](status="success", statusCode=200, message="User sessions fetched successfully", data=sessions)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{session_id}", response_model=ApiResponse[SessionModel])
async def get_session_by_id(session_id: str, manager: SessionManager = Depends(get_session_manager)):
    """Get a session by ID"""
    try:
        session = await manager._get_session_by_id(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=SESSION_NOT_FOUND_MSG)
        return ApiResponse[SessionModel](status="success", statusCode=200, message="Session fetched successfully", data=session)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{session_id}", response_model=ApiResponse[bool])
async def delete_session(session_id: str, manager: SessionManager = Depends(get_session_manager)):
    """Delete a session"""
    try:
        success = await manager._delete_session(session_id)
        if not success:
            raise HTTPException(status_code=404, detail=SESSION_NOT_FOUND_MSG)
        return ApiResponse[bool](status="success", statusCode=200, message="Session deleted successfully", data=True)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{session_id}", response_model=ApiResponse[SessionModel])
async def update_session(session_id: str, session_data: SessionRequestDTO, manager: SessionManager = Depends(get_session_manager)):
    """Update a session"""
    try:
        session = await manager._update_session(session_id, session_data)
        if not session:
            raise HTTPException(status_code=404, detail=SESSION_NOT_FOUND_MSG)
        return ApiResponse[SessionModel](status="success", statusCode=200, message="Session updated successfully", data=session)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
