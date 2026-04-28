"""
Admin Controller for FastAPI v2
Provides REST API endpoints for admin operations
"""
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Request, Header
from v2.common.model.api_response import ApiResponse
from v2.modules.admin_framework.manager.admin.admin_manager import AdminManager
from v2.modules.admin_framework.models.admin_model import (
    UserModel, UserResponse, UserCreateRequest, UserUpdateRequest, AddTokensRequest,
    AdminStatsResponse, TokenHistoryModel
)
from v2.common.gcp import GcpManager
from v2.common.logger import add_log
import os
import traceback

router = APIRouter()


def get_admin_manager() -> AdminManager:
    """Dependency to get admin manager instance"""
    return AdminManager()


async def get_current_user(request: Request) -> Dict[str, Any]:
    """Dependency to get current authenticated user"""
    user = getattr(request.state, 'user', None)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


async def require_admin(request: Request) -> Dict[str, Any]:
    """Dependency to require admin role"""
    user = await get_current_user(request)
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _verify_market_campaign_api_key(api_key: str) -> bool:
    """Verify API key for market campaign endpoints"""
    if not api_key:
        return False
    
    # Check environment variable first
    expected = os.getenv("market_campaign_api_key", "")
    
    # If not in env, try GCP Secret Manager
    if not expected:
        try:
            gcp_manager = GcpManager._get_instance()
            secret_service = gcp_manager._secret_service
            expected = secret_service._get_secret("market_campaign_api_key")
        except Exception as e:
            add_log(f"AdminController: Failed to get market_campaign_api_key from Secret Manager: {str(e)}")
            expected = ""
    
    return bool(expected) and api_key == expected


@router.get("/users", response_model=ApiResponse[List[UserModel]])
async def get_all_users(admin_user: Dict[str, Any] = Depends(require_admin), manager: AdminManager = Depends(get_admin_manager)):
    """Get all users (admin only)"""
    try:
        users = await manager.get_all_users()
        return ApiResponse[List[UserModel]](
            status="success",
            statusCode=200,
            message=f"Retrieved {len(users)} users",
            data=users
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error getting all users: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to fetch users")


@router.get("/users/{email}", response_model=UserResponse)
async def get_user_by_email(email: str, manager: AdminManager = Depends(get_admin_manager)):
    """Get specific user by email (public endpoint - no auth required)"""
    try:
        user = await manager.get_user_by_email(email)
        if not user:
            return UserResponse(
                authorized=False,
                exists=False,
                email=email
            )
        
        # User exists - check if authorized (active status)
        is_authorized = user.status == "active" if user.status else False
        
        return UserResponse(
            authorized=is_authorized,
            exists=True,
            email=user.email,
            role=user.role,
            status=user.status,
            registration_type=user.registration_type,
            created_at=user.created_at,
            updated_at=user.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error getting user {email}: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to fetch user")


@router.post("/users", response_model=ApiResponse[bool])
async def create_user(request: UserCreateRequest, admin_user: Dict[str, Any] = Depends(require_admin), manager: AdminManager = Depends(get_admin_manager)):
    """Create new user (admin only)"""
    try:
        await manager.create_user(request)
        return ApiResponse[bool](
            status="success",
            statusCode=201,
            message="User created successfully",
            data=True
        )
    except ValueError as e:
        if "already exists" in str(e).lower():
            raise HTTPException(status_code=409, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error creating user: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to create user")


@router.put("/users/{email}", response_model=ApiResponse[bool])
async def update_user(email: str, request: UserUpdateRequest, admin_user: Dict[str, Any] = Depends(require_admin), manager: AdminManager = Depends(get_admin_manager)):
    """Update user (admin only)"""
    try:
        await manager.update_user(email, request)
        return ApiResponse[bool](
            status="success",
            statusCode=200,
            message="User updated successfully",
            data=True
        )
    except ValueError as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error updating user {email}: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to update user")


@router.delete("/users/{email}", response_model=ApiResponse[bool])
async def delete_user(email: str, admin_user: Dict[str, Any] = Depends(require_admin), manager: AdminManager = Depends(get_admin_manager)):
    """Delete user (admin only)"""
    try:
        await manager.delete_user(email)
        return ApiResponse[bool](
            status="success",
            statusCode=200,
            message="User deleted successfully",
            data=True
        )
    except ValueError as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error deleting user {email}: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to delete user")


@router.get("/profile", response_model=ApiResponse[UserModel])
async def get_current_user_profile(current_user: Dict[str, Any] = Depends(get_current_user), manager: AdminManager = Depends(get_admin_manager)):
    """Get current user's profile with role information"""
    try:
        user_email = current_user.get('email')
        if not user_email:
            raise HTTPException(status_code=401, detail="User email not found")
        
        user = await manager.get_user_by_email(user_email)
        
        if not user:
            # Create user if doesn't exist (first time login)
            try:
                create_request = UserCreateRequest(
                    email=user_email,
                    name=current_user.get('name', 'Unknown'),
                    role='user',
                    issued_token=1000
                )
                await manager.create_user(create_request)
                user = await manager.get_user_by_email(user_email)
            except Exception as e:
                add_log(f"AdminController: Error creating user profile for {user_email}: {str(e)}")
                raise HTTPException(status_code=500, detail="Failed to create user profile")
        
        return ApiResponse[UserModel](
            status="success",
            statusCode=200,
            message="User profile retrieved successfully",
            data=user
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"AdminController: Error getting user profile: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to fetch user profile")


@router.get("/admin/stats", response_model=ApiResponse[AdminStatsResponse])
async def get_admin_stats(admin_user: Dict[str, Any] = Depends(require_admin), manager: AdminManager = Depends(get_admin_manager)):
    """Get admin dashboard statistics"""
    try:
        stats = await manager.get_admin_stats()
        return ApiResponse[AdminStatsResponse](
            status="success",
            statusCode=200,
            message="Admin statistics retrieved successfully",
            data=stats
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error getting admin stats: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to fetch statistics")


@router.post("/users/{email}/add-tokens", response_model=ApiResponse[Dict[str, Any]])
async def add_tokens_to_user(
    email: str,
    request: AddTokensRequest,
    admin_user: Dict[str, Any] = Depends(require_admin),
    manager: AdminManager = Depends(get_admin_manager)
):
    """Add tokens to user's existing token allocation (admin only)"""
    try:
        added_by = admin_user.get('email', 'system')
        result = await manager.add_tokens_to_user(email, request, added_by)
        
        return ApiResponse[Dict[str, Any]](
            status="success",
            statusCode=200,
            message=f"Added {request.tokens_to_add} tokens to user",
            data=result
        )
    except ValueError as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error adding tokens to user {email}: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to add tokens")


@router.get("/users/{email}/token-history", response_model=ApiResponse[Dict[str, Any]])
async def get_user_token_history(
    email: str,
    limit: int = 50,
    admin_user: Dict[str, Any] = Depends(require_admin),
    manager: AdminManager = Depends(get_admin_manager)
):
    """Get token history for a specific user (admin only)"""
    try:
        if limit < 1 or limit > 100:
            limit = 50  # Default safe limit
        
        history = await manager.get_token_history(email, limit)
        
        return ApiResponse[Dict[str, Any]](
            status="success",
            statusCode=200,
            message=f"Retrieved {len(history)} token history records",
            data={
                'history': history,
                'total_records': len(history)
            }
        )
    except ValueError as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error getting token history for {email}: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to fetch token history")


@router.get("/admin/health", response_model=ApiResponse[Dict[str, bool]])
async def admin_health_check(admin_user: Dict[str, Any] = Depends(require_admin)):
    """Admin health check endpoint"""
    try:
        gcp_manager = GcpManager._get_instance()
        firestore_available = gcp_manager._firestore_service and gcp_manager._firestore_service._client is not None
        
        return ApiResponse[Dict[str, bool]](
            status="success",
            statusCode=200,
            message="Health check completed",
            data={
                'status': True,
                'firebase_available': firestore_available
            }
        )
    except Exception as e:
        add_log(f"AdminController: Error in health check: {str(e)}")
        return ApiResponse[Dict[str, bool]](
            status="success",
            statusCode=200,
            message="Health check completed",
            data={
                'status': True,
                'firebase_available': False
            }
        )


@router.post("/market_campaign_user", response_model=ApiResponse[bool])
async def create_market_campaign_user(
    request: UserCreateRequest,
    x_api_key: str = Header(None, alias="X-API-Key"),
    api_key: str = Header(None, alias="API-Key"),
    manager: AdminManager = Depends(get_admin_manager)
):
    """Create user via market campaign API key (bypasses bearer/global auth)"""
    try:
        # Verify API key
        provided_key = x_api_key or api_key
        if not _verify_market_campaign_api_key(provided_key):
            raise HTTPException(status_code=401, detail="Invalid or missing API Key")
        
        # Check if user already exists
        existing = await manager.get_user_by_email(request.email)
        if existing:
            raise HTTPException(status_code=409, detail="User already exists")
        
        # Create user with market campaign flag
        request.registration_type = request.registration_type or "internal"
        request.created_via = "market_campaign_api"
        
        await manager.create_user(request)
        
        return ApiResponse[bool](
            status="success",
            statusCode=201,
            message="User created successfully via market campaign API",
            data=True
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        add_log(f"AdminController: Error creating market campaign user: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to create user")
