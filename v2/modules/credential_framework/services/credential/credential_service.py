"""
Credential service for FastAPI v2
Service layer for credential operations - handles Firestore storage
Uses separate connections collection with user_id and state fields
"""
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime
from v2.modules.credential_framework.models.credential_model import (
    CredentialCreate, CredentialResponse, CredentialUpdate, CredentialListResponse
)
from v2.common.gcp import GcpManager
from v2.common.logger import add_log
from v2.utils.encryption import encrypt_credentials, decrypt_credentials, is_encrypted
import traceback
import uuid


class CredentialService:
    """Service layer for credential operations with Firestore storage"""
    
    def __init__(self):
        """Initialize credential service with Firestore"""
        self._gcp_manager = GcpManager._get_instance()
        self._connections_collection = "connections"
        self._firestore_service = self._gcp_manager._firestore_service
        add_log("CredentialService: Initialized with Firestore storage")
    
    def _convert_to_response(self, connection_id: str, doc_data: dict, include_credentials: bool = False) -> CredentialResponse:
        """Convert Firestore document to CredentialResponse"""
        return CredentialResponse(
            credential_id=connection_id,
            user_id=doc_data.get('user_id', ''),
            user_email=doc_data.get('user_email'),
            connection_type=doc_data.get('connector_type', ''),
            connection_name=doc_data.get('connection_name', ''),
            credentials=doc_data.get('credentials') if include_credentials else None,
            session_id=doc_data.get('session_id'),
            created_at=doc_data.get('created_at'),
            updated_at=doc_data.get('updated_at'),
            status='active' if doc_data.get('is_valid', True) else 'inactive'
        )
    
    def _convert_to_list_response(self, connection_id: str, doc_data: dict) -> CredentialListResponse:
        """Convert Firestore document to CredentialListResponse (no sensitive data)"""
        return CredentialListResponse(
            credential_id=connection_id,
            user_id=doc_data.get('user_id', ''),
            user_email=doc_data.get('user_email'),
            connection_type=doc_data.get('connector_type', ''),
            connection_name=doc_data.get('connection_name', ''),
            session_id=doc_data.get('session_id'),
            created_at=doc_data.get('created_at'),
            status='active' if doc_data.get('is_valid', True) else 'inactive'
        )
    
    async def credential_exists_by_name(self, user_id: str, connector_type: str, connection_name: str) -> bool:
        """
        Check if a credential with the same connection_name already exists for a user and connector_type
        
        Collection: connections (filtered by user_id, connector_type and connection_name)
        """
        await asyncio.sleep(0)
        try:
            filters = [
                ('user_id', '==', user_id),
                ('connector_type', '==', connector_type),
                ('connection_name', '==', connection_name)
            ]
            docs = await self._firestore_service._query_collection(self._connections_collection, filters=filters, limit=1)
            return len(docs) > 0
        except Exception as e:
            add_log(f"CredentialService: Error checking credential existence: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def save_credential(self, user_id: str, connector_type: str, connection_name: str, credentials_data: Dict[str, Any], connection_id: Optional[str] = None) -> str:
        """
        Save or update credential for a user
        
        Collection: connections (flat collection with user_id and state fields)
        
        Note: Duplicate connection_name check should be performed before calling this method
        Credentials are encrypted before storing to Firestore.
        State is set to "created" by default on save.
        
        Returns:
            str: connection_id
        """
        await asyncio.sleep(0)
        try:
            if not connection_id:
                connection_id = str(uuid.uuid4())
            
            now = datetime.utcnow()
            doc_data = {
                'connection_id': connection_id,
                'user_id': user_id,
                'connector_type': connector_type,
                'connection_name': connection_name,
                'state': 'created',  # Default state on save
                'is_valid': True,
                'created_at': now,
                'updated_at': now
            }
            
            # Encrypt credentials before storing (only the credentials field)
            if credentials_data:
                # Only encrypt if not already encrypted (for backward compatibility)
                if not is_encrypted(credentials_data):
                    # encrypt_credentials will raise RuntimeError or ValueError on failure
                    encrypted_creds = encrypt_credentials(credentials_data)
                    doc_data['credentials'] = encrypted_creds
                else:
                    doc_data['credentials'] = credentials_data
            
            # Store in connections collection
            await self._firestore_service._set_document(self._connections_collection, connection_id, doc_data)
            
            add_log(f"CredentialService: Saved {connector_type} credential: {connection_name} for user_id {user_id} with state 'created'")
            return connection_id
            
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors to API layer
            add_log(f"CredentialService: Failed to save credential for user_id {user_id}: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to save credential for user_id {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_credential(self, user_id: str, connection_id: str, include_credentials: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get a specific credential
        
        Collection: connections (filtered by connection_id and user_id)
        
        Credentials are automatically decrypted when retrieved from Firestore.
        
        Returns:
            Dict with full credential data (CredentialDocument-like) or None if not found
        """
        await asyncio.sleep(0)
        try:
            # Get document from connections collection
            doc = await self._firestore_service._get_document(self._connections_collection, connection_id)
            print(f"CredentialService: Retrieved document for connection_id {connection_id}: {doc}")
            if not doc:
                return None
            
            # Verify user_id matches (security check)
            if doc.get('user_id') != user_id:
                add_log(f"CredentialService: User ID mismatch for connection {connection_id}")
                return None
            
            # Decrypt credentials if encrypted
            if doc.get('credentials'):
                credentials_data = doc['credentials']
                if is_encrypted(credentials_data):
                    try:
                        decrypted_creds = decrypt_credentials(credentials_data)
                        doc['credentials'] = decrypted_creds
                    except (RuntimeError, ValueError) as e:
                        # Propagate encryption errors to API layer
                        add_log(f"CredentialService: Failed to decrypt credentials: {str(e)}")
                        raise
                    except Exception as e:
                        add_log(f"CredentialService: Failed to decrypt credentials: {str(e)}")
                        # If decryption fails for other reasons, return None
                        return None
            
            # Ensure connection_id is set
            if 'connection_id' not in doc:
                doc['connection_id'] = connection_id
            
            # Return full credential document (matching CredentialDocument structure)
            return doc
            
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to get credential: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    async def get_all_credentials(self, user_id: str, connector_type: str) -> List[Dict[str, Any]]:
        """
        Get all credentials for a specific connector type
        
        Collection: connections (filtered by user_id and connector_type)
        
        Credentials are automatically decrypted when retrieved from Firestore.
        """
        await asyncio.sleep(0)
        try:
            # Query connections collection filtered by user_id and connector_type
            filters = [
                ('user_id', '==', user_id),
                ('connector_type', '==', connector_type)
            ]
            docs = await self._firestore_service._query_collection(self._connections_collection, filters=filters)
            
            credentials = []
            for doc in docs:
                try:
                    connection_id = doc.get('id') or doc.get('connection_id')
                    
                    # Decrypt credentials if encrypted
                    if doc.get('credentials'):
                        credentials_data = doc['credentials']
                        if is_encrypted(credentials_data):
                            try:
                                decrypted_creds = decrypt_credentials(credentials_data)
                                doc['credentials'] = decrypted_creds
                            except (RuntimeError, ValueError) as e:
                                # Propagate encryption errors to API layer
                                add_log(f"CredentialService: Failed to decrypt credentials for {connection_id}: {str(e)}")
                                raise
                            except Exception as e:
                                add_log(f"CredentialService: Failed to decrypt credentials for {connection_id}: {str(e)}")
                                # Skip this credential if decryption fails for other reasons
                                continue
                    
                    doc['connection_id'] = connection_id
                    credentials.append(doc)
                except Exception as e:
                    add_log(f"CredentialService: Error parsing credential: {e}")
            
            return credentials
            
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to get credentials: {str(e)} | traceback: {traceback.format_exc()}")
            return []
    
    async def get_all_connector_credentials(self, user_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all credentials for all connector types for a user
        
        Collection: connections (filtered by user_id)
        
        Credentials are automatically decrypted when retrieved from Firestore.
        
        Returns:
            Dictionary mapping connector_type to list of credentials
        """
        await asyncio.sleep(0)
        try:
            all_credentials = {}
            
            # Query connections collection filtered by user_id
            filters = [('user_id', '==', user_id)]
            docs = await self._firestore_service._query_collection(self._connections_collection, filters=filters)
            
            for doc in docs:
                try:
                    connection_id = doc.get('id') or doc.get('connection_id')
                    
                    # Decrypt credentials if encrypted
                    if doc.get('credentials'):
                        credentials_data = doc['credentials']
                        if is_encrypted(credentials_data):
                            try:
                                decrypted_creds = decrypt_credentials(credentials_data)
                                doc['credentials'] = decrypted_creds
                            except (RuntimeError, ValueError) as e:
                                # Propagate encryption errors to API layer
                                add_log(f"CredentialService: Failed to decrypt credentials for {connection_id}: {str(e)}")
                                raise
                            except Exception as e:
                                add_log(f"CredentialService: Failed to decrypt credentials for {connection_id}: {str(e)}")
                                # Skip this credential if decryption fails for other reasons
                                continue
                    
                    connector_type = doc.get('connector_type')
                    if not connector_type:
                        continue
                    
                    doc['connection_id'] = connection_id
                    
                    if connector_type not in all_credentials:
                        all_credentials[connector_type] = []
                    
                    all_credentials[connector_type].append(doc)
                except Exception as e:
                    add_log(f"CredentialService: Error parsing credential: {e}")
            
            return all_credentials
            
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to get all connector credentials: {str(e)} | traceback: {traceback.format_exc()}")
            return {}
    
    async def update_credential(self, user_id: str, connection_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update a specific credential
        
        Collection: connections (filtered by connection_id and user_id)
        
        If credentials are being updated, they will be merged with existing credentials
        and encrypted before storing.
        """
        await asyncio.sleep(0)
        try:
            # First verify the credential exists and belongs to the user
            # Get WITH credentials to merge partial updates
            credential = await self.get_credential(user_id, connection_id, include_credentials=True)
            if not credential:
                add_log(f"CredentialService: Credential not found: {connection_id}")
                return False
            updates['updated_at'] = datetime.utcnow()
            
            # Merge partial credentials update with existing credentials
            if 'credentials' in updates:
                credentials_dict = updates['credentials']
                # Only merge if not already encrypted (partial update)
                if not is_encrypted(credentials_dict):
                    # Get existing credentials (already decrypted from get_credential)
                    existing_credentials = credential.get('credentials', {})
                    if not isinstance(existing_credentials, dict):
                        existing_credentials = {}
                    
                    # Merge: new values override existing ones
                    merged_credentials = {**existing_credentials, **credentials_dict}
                    
                    # Encrypt the merged credentials
                    encrypted_creds = encrypt_credentials(merged_credentials)
                    updates['credentials'] = encrypted_creds
                # If already encrypted, use as-is (full replacement)
            
            # Update document in connections collection
            await self._firestore_service._update_document(self._connections_collection, connection_id, updates)
            
            add_log(f"CredentialService: Updated credential {connection_id} for user_id {user_id}")
            return {
                'connector_type': credential.get('connector_type'),
                'success': True
            }
            
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors to API layer
            add_log(f"CredentialService: Failed to update credential: {str(e)}")
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to update credential: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def delete_credential(self, user_id: str, connection_id: str) -> bool:
        """
        Delete a specific credential
        
        Collection: connections (filtered by connection_id and user_id)
        
        Raises:
            ValueError: If credential not found or doesn't belong to user
        """
        await asyncio.sleep(0)
        try:
            # First verify the credential exists and belongs to the user
            credential = await self.get_credential(user_id, connection_id, include_credentials=False)
            if not credential:
                add_log(f"CredentialService: Credential not found: {connection_id} for user_id {user_id}")
                raise ValueError(f"Credential with connection_id '{connection_id}' not found")
            
            # Delete document from connections collection
            await self._firestore_service._delete_document(self._connections_collection, connection_id)
            
            add_log(f"CredentialService: Deleted credential {connection_id} for user_id {user_id}")
            return True
            
        except ValueError:
            # Re-raise ValueError (not found) to be handled by manager
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to delete credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise RuntimeError(f"Failed to delete credential: {str(e)}")
    
    async def validate_credential(self, user_id: str, connector_type: str, connection_id: str) -> bool:
        """
        Validate a credential (mark as validated with current timestamp)
        
        Collection: connections (filtered by connection_id and user_id)
        """
        await asyncio.sleep(0)
        try:
            # First verify the credential exists and belongs to the user
            credential = await self.get_credential(user_id, connection_id, include_credentials=False)
            if not credential:
                add_log(f"CredentialService: Credential not found: {connection_id}")
                return False
            
            updates = {
                'is_valid': True,
                'last_validated': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Update document in connections collection
            await self._firestore_service._update_document(self._connections_collection, connection_id, updates)
            
            add_log(f"CredentialService: Validated {connector_type} credential {connection_id} for user_id {user_id}")
            return True
            
        except Exception as e:
            add_log(f"CredentialService: Failed to validate credential: {str(e)} | traceback: {traceback.format_exc()}")
            return False

    async def get_cred_for_connection(self, user_id: str, connection_id: str, connector_type: str, include_credentials: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get a credential filtered by connector_type first, then connection_id.
        
        Collection: connections (filtered by user_id, connector_type, and connection_id)
        
        This method ensures that credentials are filtered by connector_type first,
        then by connection_id, preventing mismatched connector types.
        
        Args:
            user_id: User identifier
            connection_id: Connection identifier (can be document ID or field)
            connector_type: Connector type to filter by (validated first)
            include_credentials: Whether to include credentials in response
            
        Returns:
            Dict with full credential data or None if not found or type mismatch
        """
        await asyncio.sleep(0)
        try:
            # Normalize connector_type by trimming whitespace (case-insensitive comparison)
            normalized_connector_type = (connector_type or "").strip().lower()
            
            # First, try to get document directly by connection_id (in case it's the document ID)
            doc = await self._firestore_service._get_document(self._connections_collection, connection_id)
            
            # If found by document ID, validate connector_type and user_id
            if doc:
                # Verify user_id matches (security check)
                if doc.get('user_id') != user_id:
                    add_log(f"CredentialService: User ID mismatch for connection {connection_id}")
                    return None
                
                # Validate connector_type matches (normalize both values for comparison)
                doc_connector_type = (doc.get('connector_type') or "").strip().lower()
                if doc_connector_type != normalized_connector_type:
                    add_log(f"CredentialService: Connector type mismatch. Expected '{connector_type}' (normalized: '{normalized_connector_type}'), got '{doc.get('connector_type')}' (normalized: '{doc_connector_type}') for connection_id '{connection_id}'")
                    return None
            else:
                # If not found by document ID, query by filters (connection_id as field)
                # Note: Firestore queries are case-sensitive, so we query with the original connector_type
                # but we'll validate the normalized version after retrieval
                filters = [
                    ('user_id', '==', user_id),
                    ('connector_type', '==', connector_type.strip()),
                    ('connection_id', '==', connection_id)
                ]
                docs = await self._firestore_service._query_collection(self._connections_collection, filters=filters, limit=1)
                
                if not docs:
                    add_log(f"CredentialService: Credential not found for connection_id '{connection_id}' with connector_type '{connector_type}'")
                    return None
                
                doc = docs[0]
            
            # Decrypt credentials if encrypted
            if doc.get('credentials') and include_credentials:
                credentials_data = doc['credentials']
                if is_encrypted(credentials_data):
                    try:
                        decrypted_creds = decrypt_credentials(credentials_data)
                        doc['credentials'] = decrypted_creds
                    except (RuntimeError, ValueError) as e:
                        add_log(f"CredentialService: Failed to decrypt credentials: {str(e)}")
                        raise
                    except Exception as e:
                        add_log(f"CredentialService: Failed to decrypt credentials: {str(e)}")
                        return None
            elif not include_credentials:
                # Remove credentials from response if not requested
                doc.pop('credentials', None)
            
            # Ensure connection_id is set
            if 'connection_id' not in doc:
                doc['connection_id'] = connection_id
            
            add_log(f"CredentialService: Retrieved credential {connection_id} for connector_type {connector_type}")
            return doc
            
        except (RuntimeError, ValueError) as e:
            # Propagate encryption errors
            raise
        except Exception as e:
            add_log(f"CredentialService: Failed to get credential for connection: {str(e)} | traceback: {traceback.format_exc()}")
            return None

