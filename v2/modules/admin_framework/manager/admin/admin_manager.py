"""
Admin Manager for FastAPI v2
Orchestrates service calls and handles business logic
"""
from typing import List, Optional, Dict, Any
from v2.modules.admin_framework.services.admin.admin_service import AdminService
from v2.modules.admin_framework.models.admin_model import (
    UserModel, UserCreateRequest, UserUpdateRequest, AddTokensRequest,
    AdminStatsResponse, TokenHistoryModel
)
from v2.common.logger import add_log
import traceback


class AdminManager:
    """Manager layer for admin operations - orchestrates service calls"""
    
    def __init__(self, service: Optional[AdminService] = None):
        """
        Initialize admin manager
        
        Args:
            service: Optional AdminService instance. If not provided, creates a new one.
        """
        self._service = service or AdminService()
        add_log("AdminManager: Initialized with AdminService")
    
    async def get_all_users(self) -> List[UserModel]:
        """Get all users"""
        try:
            users_data = await self._service.get_all_users()
            return [UserModel(**user) for user in users_data]
        except Exception as e:
            add_log(f"AdminManager: Error getting all users: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_user_by_email(self, email: str) -> Optional[UserModel]:
        """Get user by email"""
        try:
            if not email or not email.strip():
                raise ValueError("Email is required")
            
            user_data = await self._service.get_user_by_email(email)
            if user_data:
                return UserModel(**user_data)
            return None
        except Exception as e:
            add_log(f"AdminManager: Error getting user {email}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def create_user(self, request: UserCreateRequest) -> bool:
        """Create a new user"""
        try:
            # Validate required fields
            if not request.email or not request.email.strip():
                raise ValueError("Email is required")
            if not request.name or not request.name.strip():
                raise ValueError("Name is required")
            if not request.role or not request.role.strip():
                raise ValueError("Role is required")
            if request.issued_token < 0:
                raise ValueError("issued_token must be non-negative")
            
            # Check if user already exists
            existing = await self._service.is_user_authorized(request.email)
            if existing:
                raise ValueError("User already exists")
            
            # Prepare user data
            user_data = {
                'name': request.name,
                'role': request.role,
                'issued_token': request.issued_token,
                'registration_type': request.registration_type,
                'auth_provider': request.auth_provider
            }
            if request.created_via:
                user_data['created_via'] = request.created_via
            
            success = await self._service.create_user(request.email, user_data)
            if not success:
                raise RuntimeError("Failed to create user")
            
            return True
        except Exception as e:
            add_log(f"AdminManager: Error creating user: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def update_user(self, email: str, request: UserUpdateRequest) -> bool:
        """Update user"""
        try:
            if not email or not email.strip():
                raise ValueError("Email is required")
            
            # Check if user exists
            existing = await self._service.get_user_by_email(email)
            if not existing:
                raise ValueError("User not found")
            
            # Prepare updates (only include non-None fields)
            updates = {}
            if request.name is not None:
                updates['name'] = request.name
            if request.role is not None:
                updates['role'] = request.role
            if request.issued_token is not None:
                if request.issued_token < 0:
                    raise ValueError("issued_token must be non-negative")
                updates['issued_token'] = request.issued_token
            if request.used_token is not None:
                if request.used_token < 0:
                    raise ValueError("used_token must be non-negative")
                updates['used_token'] = request.used_token
            if request.report_count is not None:
                if request.report_count < 0:
                    raise ValueError("report_count must be non-negative")
                updates['report_count'] = request.report_count
            if request.registration_type is not None:
                updates['registration_type'] = request.registration_type
            if request.auth_provider is not None:
                updates['auth_provider'] = request.auth_provider
            if request.status is not None:
                updates['status'] = request.status
            if request.created_via is not None:
                updates['created_via'] = request.created_via
            
            if not updates:
                raise ValueError("No fields to update")
            
            success = await self._service.update_user(email, updates)
            if not success:
                raise RuntimeError("Failed to update user")
            
            return True
        except Exception as e:
            add_log(f"AdminManager: Error updating user {email}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def delete_user(self, email: str) -> bool:
        """Delete user"""
        try:
            if not email or not email.strip():
                raise ValueError("Email is required")
            
            # Check if user exists
            existing = await self._service.get_user_by_email(email)
            if not existing:
                raise ValueError("User not found")
            
            success = await self._service.delete_user(email)
            if not success:
                raise RuntimeError("Failed to delete user")
            
            return True
        except Exception as e:
            add_log(f"AdminManager: Error deleting user {email}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_admin_stats(self) -> AdminStatsResponse:
        """Get admin dashboard statistics"""
        try:
            users = await self._service.get_all_users()
            
            total_users = len(users)
            total_reports = sum(user.get('report_count', 0) for user in users)
            total_tokens_used = sum(user.get('used_token', 0) for user in users)
            active_users = len([user for user in users if user.get('report_count', 0) > 0])
            
            return AdminStatsResponse(
                total_users=total_users,
                total_reports=total_reports,
                total_tokens_used=total_tokens_used,
                active_users=active_users
            )
        except Exception as e:
            add_log(f"AdminManager: Error getting admin stats: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def add_tokens_to_user(
        self, 
        email: str, 
        request: AddTokensRequest, 
        added_by: str
    ) -> Dict[str, Any]:
        """Add tokens to user"""
        try:
            if not email or not email.strip():
                raise ValueError("Email is required")
            if request.tokens_to_add <= 0:
                raise ValueError("tokens_to_add must be positive")
            
            # Check if user exists
            existing = await self._service.get_user_by_email(email)
            if not existing:
                raise ValueError("User not found")
            
            result = await self._service.add_tokens_with_history(
                user_email=email,
                tokens_to_add=request.tokens_to_add,
                added_by=added_by,
                reason=request.reason
            )
            
            if not result.get('success'):
                raise RuntimeError(result.get('error', 'Failed to add tokens'))
            
            return result
        except Exception as e:
            add_log(f"AdminManager: Error adding tokens to user {email}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_token_history(self, email: str, limit: int = 50) -> List[TokenHistoryModel]:
        """Get token history for a user"""
        try:
            if not email or not email.strip():
                raise ValueError("Email is required")
            if limit < 1 or limit > 100:
                limit = 50  # Default safe limit
            
            # Check if user exists
            existing = await self._service.get_user_by_email(email)
            if not existing:
                raise ValueError("User not found")
            
            history_data = await self._service.get_token_history(email, limit)
            return [TokenHistoryModel(**record) for record in history_data]
        except Exception as e:
            add_log(f"AdminManager: Error getting token history for {email}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
