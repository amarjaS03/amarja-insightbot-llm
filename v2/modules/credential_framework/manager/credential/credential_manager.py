"""
Credential Manager for FastAPI v2
Acts as an intermediary between Controller and Service layers
Matches v1 behavior and API structure
"""
from typing import List, Optional, Dict, Any
from v2.modules.credential_framework.models.credential_model import (
    CredentialCreate, CredentialResponse, CredentialUpdate, CredentialListResponse
)
from v2.modules.credential_framework.services.credential.credential_service import CredentialService
from v2.common.logger import add_log
import traceback
import uuid


class CredentialManager:
    """Manager layer for credential operations - orchestrates service calls"""
    
    def __init__(self, service: Optional[CredentialService] = None):
        """
        Initialize credential manager
        
        Args:
            service: Optional CredentialService instance. If not provided, creates a new one.
        """
        self._service = service or CredentialService()
        add_log("CredentialManager: Initialized with CredentialService")
    
    async def save_credential(
        self, 
        user_id: str, 
        connector_type: str, 
        connection_name: str, 
        credentials_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save a new credential for a connector type
        
        Path: userCollection/{user_id}/connections/{connectionId}
        
        Returns:
            Dict with connection_id and status
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            if not connection_name or not connection_name.strip():
                raise ValueError("connection_name is required")
            
            if not credentials_data:
                raise ValueError("credentials object is required")
            
            # Check for duplicate connection_name before saving
            exists = await self._service.credential_exists_by_name(user_id, connector_type, connection_name.strip())
            if exists:
                raise ValueError(f'A credential with the name "{connection_name}" already exists for connector type "{connector_type}"')
            
            # Generate unique connection ID
            connection_id = str(uuid.uuid4())
            
            # Save to Firestore
            saved_connection_id = await self._service.save_credential(
                user_id, 
                connector_type, 
                connection_name.strip(), 
                credentials_data,
                connection_id
            )
            
            add_log(f"CredentialManager: Credential saved: {connector_type}/{saved_connection_id} for user_id {user_id}")
            
            return {
                'connection_id': saved_connection_id,
                'connector_type': connector_type
            }
        except (RuntimeError, ValueError) as e:
            # Propagate encryption and validation errors
            add_log(f"CredentialManager: Error saving credential: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialManager: Error saving credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_credential(
        self, 
        user_id: str, 
        connection_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific credential by connection_id
        
        Returns:
            Dict with credential data matching v1 API response format or None if not found
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            credential = await self._service.get_credential(user_id, connection_id, include_credentials=True)
            
            if not credential:
                return None
            
            # Return in v1 API format
            return {
                'status':'success',
                'data': credential
            }
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            add_log(f"CredentialManager: Error retrieving credential: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialManager: Error retrieving credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_connector_credentials(
        self, 
        user_id: str, 
        connector_type: str
    ) -> Dict[str, Any]:
        """
        Get all credentials for a connector type
        
        Returns:
            Dict with credentials list
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            connections = await self._service.get_all_credentials(user_id, connector_type)
            print(f"Credentials: {connections}")
            print(f"Connector type: {connector_type}")
            connections_list = []
            for cred in connections:
                connections_list.append({
                    'connection_id': cred.get('connection_id'),
                    'connection_name': cred.get('connection_name'),
                    'credentials': cred.get('credentials'),  # Include decrypted credentials
                    'is_valid': cred.get('is_valid', True),
                    'last_validated': cred.get('last_validated').isoformat() if cred.get('last_validated') else None,
                    'created_at': cred.get('created_at').isoformat() if cred.get('created_at') else None
                })
            
            return {
                'status': 'success',
                'connector_type': connector_type,
                'connections': connections_list,
                'total': len(connections_list)
            }
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            add_log(f"CredentialManager: Error retrieving credentials: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialManager: Error retrieving credentials: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_all_connector_credentials(self, user_id: str) -> Dict[str, Any]:
        """
        Get all credentials for all connector types
        
        Returns:
            Dict mapping connector_type to list of credentials
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            all_credentials = await self._service.get_all_connector_credentials(user_id)
            
            result = {}
            for connector_type, creds in all_credentials.items():
                result[connector_type] = [
                    {
                        'connection_id': cred.get('connection_id'),
                        'connection_name': cred.get('connection_name'),
                        'credentials': cred.get('credentials'),
                        'is_valid': cred.get('is_valid', True),
                        'last_validated': cred.get('last_validated').isoformat() if cred.get('last_validated') else None,
                        'created_at': cred.get('created_at').isoformat() if cred.get('created_at') else None
                    }
                    for cred in creds
                ]
            
            return {
                'status': 'success',
                'credentials': result
            }
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            add_log(f"CredentialManager: Error retrieving all credentials: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialManager: Error retrieving all credentials: {str(e)} | traceback: {traceback.format_exc()}")
            raise




    async def update_credential(
        self, 
        user_id: str, 
        connection_id: str, 
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update credential document fields.
        Only the following fields are allowed to be updated:
          - connection_name
          - credentials  (raw dict or already-encrypted string)
          - updated_at   (will be overridden by server to current time)
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            # Allowed top-level fields to update
            allowed_fields = {
                'connection_name',
                'credentials',
                'updated_at'
            }
            # Build updates from allowed fields only
            filtered_updates = {k: v for k, v in updates.items() if k in allowed_fields}
            
            updated_result = await self._service.update_credential(user_id, connection_id, filtered_updates)
           
            if updated_result.get('success'):
                updated_fields = list(filtered_updates.keys())
                add_log(f"CredentialManager: Updated credential fields: {', '.join(updated_fields)}")
                return {
                    'connection_id': connection_id,
                    'updated_fields': updated_fields,
                    'connector_type': updated_result.get('connector_type')
                }
            else:
                raise ValueError('Failed to update credential. Please try again.')
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            add_log(f"CredentialManager: Error updating credential: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialManager: Error updating credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    



    async def delete_credential(
        self, 
        user_id: str, 
        connection_id: str
    ) -> Dict[str, Any]:
        """
        Delete a credential
        
        Returns:
            Dict with status message
        
        Raises:
            ValueError: If credential not found
            RuntimeError: If deletion fails
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            success = await self._service.delete_credential(user_id, connection_id)
            
            if success:
                add_log(f"CredentialManager: Credential deleted: {connection_id} for user_id {user_id}")
                return {
                    'connection_id': connection_id
                }
            else:
                raise RuntimeError('Failed to delete credential. Please try again.')
        except ValueError as e:
            # Credential not found
            add_log(f"CredentialManager: Credential not found: {str(e)}")
            raise
        except RuntimeError as e:
            # Deletion failed
            add_log(f"CredentialManager: Error deleting credential: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialManager: Unexpected error deleting credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise RuntimeError(f'An unexpected error occurred while deleting the credential: {str(e)}')
    



    async def validate_credential(
        self, 
        user_id: str, 
        connector_type: str, 
        connection_id: str
    ) -> Dict[str, Any]:
        """
        Validate a credential (mark as validated with current timestamp)
        
        Returns:
            Dict with status message
        """
        try:
            user_id = user_id.strip()
            if not user_id:
                raise ValueError("user_id is required")
            
            success = await self._service.validate_credential(user_id, connector_type, connection_id)
            
            if success:
                add_log(f"CredentialManager: Credential validated: {connector_type}/{connection_id} for user_id {user_id}")
                return {
                    'status': 'success',
                    'message': 'Credential validated successfully'
                }
            else:
                raise ValueError('Failed to validate credential')
        except Exception as e:
            add_log(f"CredentialManager: Error validating credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise
