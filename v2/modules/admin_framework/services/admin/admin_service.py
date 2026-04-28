"""
Admin Service for FastAPI v2
Handles all Firestore operations via GcpManager
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncio
import uuid
from google.cloud import firestore

from v2.common.gcp import GcpManager
from v2.common.logger import add_log
from v2.modules.admin_framework.models.admin_model import (
    UserModel, TokenHistoryModel
)


class AdminService:
    """Service layer for admin operations using GcpManager for Firestore access"""
    
    COLLECTION_NAME = 'userCollection'
    
    def __init__(self):
        """Initialize admin service with GcpManager"""
        try:
            self.gcp_manager = GcpManager._get_instance()
            self.firestore_service = self.gcp_manager._firestore_service
            
            if not self.firestore_service or not self.firestore_service._client:
                add_log("AdminService: Firestore service not available")
                self.firestore_service = None
            else:
                add_log("AdminService: Initialized with GcpManager Firestore service")
        except Exception as e:
            add_log(f"AdminService: Failed to initialize GcpManager: {str(e)}")
            self.firestore_service = None
    
    def _check_firestore_available(self) -> None:
        """Check if Firestore is available"""
        if not self.firestore_service or not self.firestore_service._client:
            raise RuntimeError("Firestore not available")
    
    def _convert_timestamp(self, timestamp) -> Optional[str]:
        """Convert Firestore timestamp to ISO string"""
        if timestamp is None:
            return None
        if hasattr(timestamp, 'isoformat'):
            return timestamp.isoformat()
        return str(timestamp)
    
    async def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all users from Firestore"""
        self._check_firestore_available()
        
        try:
            users = await self.firestore_service._get_all_documents(self.COLLECTION_NAME)
            
            # Convert timestamps and add email as id
            result = []
            for user in users:
                user['id'] = user.get('email', user.get('id', ''))
                if 'created_at' in user:
                    user['created_at'] = self._convert_timestamp(user['created_at'])
                if 'updated_at' in user:
                    user['updated_at'] = self._convert_timestamp(user['updated_at'])
                result.append(user)
            
            add_log(f"AdminService: Retrieved {len(result)} users")
            return result
        except Exception as e:
            add_log(f"AdminService: Error getting all users: {str(e)}")
            raise
    
    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email from Firestore"""
        self._check_firestore_available()
        
        try:
            email = email.lower().strip()
            user = await self.firestore_service._get_document(self.COLLECTION_NAME, email)
            
            if user:
                user['id'] = email
                if 'created_at' in user:
                    user['created_at'] = self._convert_timestamp(user['created_at'])
                if 'updated_at' in user:
                    user['updated_at'] = self._convert_timestamp(user['updated_at'])
                add_log(f"AdminService: Retrieved user {email}")
            else:
                add_log(f"AdminService: User {email} not found")
            
            return user
        except Exception as e:
            add_log(f"AdminService: Error getting user {email}: {str(e)}")
            raise
    
    async def is_user_authorized(self, email: str) -> bool:
        """Check if user exists and is active"""
        self._check_firestore_available()
        
        try:
            email = email.lower().strip()
            user = await self.firestore_service._get_document(self.COLLECTION_NAME, email)
            
            if user:
                status = user.get('status', 'active')
                return status == 'active'
            return False
        except Exception as e:
            add_log(f"AdminService: Error checking user authorization for {email}: {str(e)}")
            return False
    
    async def create_user(self, email: str, user_data: Dict[str, Any]) -> bool:
        """Create a new user in Firestore"""
        self._check_firestore_available()
        
        try:
            email = email.lower().strip()
            
            # Check if user already exists
            existing = await self.firestore_service._get_document(self.COLLECTION_NAME, email)
            if existing:
                add_log(f"AdminService: User {email} already exists")
                return False
            
            # Prepare user document
            user_doc = {
                'email': email,
                'name': user_data.get('name', ''),
                'role': user_data.get('role', 'user'),
                'issued_token': user_data.get('issued_token', 0),
                'used_token': 0,
                'report_count': 0,
                'registration_type': user_data.get('registration_type', 'internal'),
                'auth_provider': user_data.get('auth_provider'),
                'status': 'active',
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            if 'created_via' in user_data:
                user_doc['created_via'] = user_data['created_via']
            
            await self.firestore_service._set_document(self.COLLECTION_NAME, email, user_doc)
            add_log(f"AdminService: Created user {email}")
            return True
        except Exception as e:
            add_log(f"AdminService: Error creating user {email}: {str(e)}")
            raise
    
    async def update_user(self, email: str, updates: Dict[str, Any]) -> bool:
        """Update user data in Firestore"""
        self._check_firestore_available()
        
        try:
            email = email.lower().strip()
            
            # Check if user exists
            existing = await self.firestore_service._get_document(self.COLLECTION_NAME, email)
            if not existing:
                add_log(f"AdminService: User {email} not found for update")
                return False
            
            # Add updated_at timestamp
            updates['updated_at'] = datetime.utcnow()
            
            await self.firestore_service._update_document(self.COLLECTION_NAME, email, updates)
            add_log(f"AdminService: Updated user {email}")
            return True
        except Exception as e:
            add_log(f"AdminService: Error updating user {email}: {str(e)}")
            raise
    
    async def delete_user(self, email: str) -> bool:
        """Delete user from Firestore"""
        self._check_firestore_available()
        
        try:
            email = email.lower().strip()
            
            # Check if user exists
            existing = await self.firestore_service._get_document(self.COLLECTION_NAME, email)
            if not existing:
                add_log(f"AdminService: User {email} not found for deletion")
                return False
            
            await self.firestore_service._delete_document(self.COLLECTION_NAME, email)
            add_log(f"AdminService: Deleted user {email}")
            return True
        except Exception as e:
            add_log(f"AdminService: Error deleting user {email}: {str(e)}")
            raise
    
    async def add_tokens_with_history(
        self, 
        user_email: str, 
        tokens_to_add: int, 
        added_by: str, 
        reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add tokens to user and create token history record
        
        Returns dict with success status and details
        """
        self._check_firestore_available()
        
        try:
            user_email = user_email.lower().strip()
            client = self.firestore_service._client
            
            # Get current user data
            user = await self.firestore_service._get_document(self.COLLECTION_NAME, user_email)
            if not user:
                return {
                    'success': False,
                    'error': 'User not found'
                }
            
            previous_tokens = user.get('issued_token', 0)
            new_total = previous_tokens + tokens_to_add
            
            # Update user's issued_token
            await self.firestore_service._update_document(
                self.COLLECTION_NAME, 
                user_email, 
                {'issued_token': new_total, 'updated_at': datetime.utcnow()}
            )
            
            # Create token history record
            history_id = str(uuid.uuid4())
            history_data = {
                'history_id': history_id,
                'user_email': user_email,
                'tokens_added': tokens_to_add,
                'previous_tokens': previous_tokens,
                'new_total_tokens': new_total,
                'added_by': added_by,
                'reason': reason,
                'created_at': datetime.utcnow()
            }
            
            # Access subcollection via Firestore client
            def _create_history():
                client.collection(self.COLLECTION_NAME).document(user_email).collection('tokenHistory').document(history_id).set(history_data)
            
            await asyncio.to_thread(_create_history)
            
            add_log(f"AdminService: Added {tokens_to_add} tokens to user {user_email}")
            
            return {
                'success': True,
                'previous_tokens': previous_tokens,
                'new_total': new_total,
                'history_created': True
            }
        except Exception as e:
            add_log(f"AdminService: Error adding tokens to user {user_email}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_token_history(self, user_email: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get token history for a user"""
        self._check_firestore_available()
        
        try:
            user_email = user_email.lower().strip()
            client = self.firestore_service._client
            
            # Query subcollection via Firestore client
            def _query_history():
                query = client.collection(self.COLLECTION_NAME).document(user_email).collection('tokenHistory').order_by('created_at', direction=firestore.Query.DESCENDING).limit(limit)
                results = []
                for doc in query.stream():
                    data = doc.to_dict()
                    data['id'] = doc.id
                    results.append(data)
                return results
            
            history_records = await asyncio.to_thread(_query_history)
            
            # Convert timestamps
            result = []
            for record in history_records:
                if 'created_at' in record:
                    record['created_at'] = self._convert_timestamp(record['created_at'])
                result.append(record)
            
            # Sort by created_at descending (newest first)
            result.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            add_log(f"AdminService: Retrieved {len(result)} token history records for {user_email}")
            return result
        except Exception as e:
            add_log(f"AdminService: Error getting token history for {user_email}: {str(e)}")
            # Return empty list on error (matching v1 behavior)
            return []
