"""
Connector Credentials Service for V2 Framework
Service layer for connector credential operations - handles credential retrieval with connector_type validation
"""
import asyncio
from typing import Optional, Dict, Any, List
from v2.modules.credential_framework.manager.credential.credential_manager import CredentialManager
from v2.modules.credential_framework.services.credential.credential_service import CredentialService
from v2.modules.credential_framework.models.credential_model import CredentialApiResponse
from v2.common.logger import add_log
from v2.common.gcp import GcpManager
import traceback


class ConnectorCredentialsService:
    """
    Service layer for connector credential operations.
    Handles credential retrieval with connector_type validation.
    """
    
    def __init__(self, credential_manager: Optional[CredentialManager] = None, credential_service: Optional[CredentialService] = None):
        """
        Initialize connector credentials service.
        
        Args:
            credential_manager: Optional CredentialManager instance. If not provided, creates a new one.
            credential_service: Optional CredentialService instance. If not provided, creates a new one.
        """
        self._credential_manager = credential_manager or CredentialManager()
        self._credential_service = credential_service or CredentialService()
        self._gcp_manager = GcpManager._get_instance()
        self._firestore_service = self._gcp_manager._firestore_service
        self._sessions_collection = "sessions"
        add_log("ConnectorCredentialsService: Initialized")
    
    async def get_credential(
        self,
        user_id: str,
        connection_id: str,
        connector_type: str
    ) -> Dict[str, Any]:
        """
        Get a specific credential filtered by connector_type first, then connection_id.
        
        Args:
            user_id: User identifier
            connection_id: Connection identifier
            connector_type: Connector type to validate against
            
        Returns:
            Dict with credential data or error response
            
        Raises:
            ValueError: If connector type doesn't match
        """
        try:
            # Get credential with connector_type validation
            connection_data = await self._credential_manager.get_credential(
                user_id=user_id,
                connection_id=connection_id,
                connector_type=connector_type
            )
            
            if not connection_data:
                return {
                    "status": "fail",
                    "statusCode": 404,
                    "message": f"Credential with connection_id '{connection_id}' not found for connector_type '{connector_type}'",
                    "data": {
                        "error_code": "CREDENTIAL_NOT_FOUND",
                        "connection_id": connection_id,
                        "connector_type": connector_type
                    }
                }
            
            credentials = connection_data.get('data', {})
            
            # Validate connector_type match
            cred_connector_type = credentials.get('connector_type')
            if cred_connector_type and cred_connector_type != connector_type:
                return {
                    "status": "fail",
                    "statusCode": 400,
                    "message": f"Credential type mismatch. Credential is for '{cred_connector_type}' but requested type is '{connector_type}'.",
                    "data": {
                        "error_code": "CONNECTOR_TYPE_MISMATCH",
                        "expected_type": connector_type,
                        "actual_type": cred_connector_type,
                        "connection_id": connection_id
                    }
                }
            
            return {
                "status": "success",
                "statusCode": 200,
                "message": "Credential fetched successfully",
                "connector_type": connector_type,
                "data": credentials
            }
            
        except (RuntimeError, ValueError) as e:
            error_msg = str(e)
            if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
                add_log(f"ConnectorCredentialsService: Encryption error retrieving credential: {error_msg}")
            raise
        except Exception as e:
            add_log(f"ConnectorCredentialsService: Error retrieving credential: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_all_credentials(
        self,
        user_id: str,
        connector_type: str,
        connection_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get all credentials for a connector type, optionally filtered by connection_id.
        
        Args:
            user_id: User identifier
            connector_type: Connector type to filter by
            connection_id: Optional connection_id to filter the results
            
        Returns:
            Dict with credentials list or single credential if connection_id provided
        """
        try:
            # First, fetch all credentials for the connector type
            result = await self._credential_manager.get_connector_credentials(
                user_id,
                connector_type
            )
            
            connections = result.get('connections', [])
            
            # If connection_id is provided, filter the connections
            if connection_id:
                filtered_connection = next(
                    (conn for conn in connections if conn.get('connection_id') == connection_id),
                    None
                )
                
                if not filtered_connection:
                    return {
                        "status": "fail",
                        "statusCode": 404,
                        "message": f"Credential with connection_id '{connection_id}' not found for connector_type '{connector_type}'",
                        "data": {
                            "error_code": "CREDENTIAL_NOT_FOUND",
                            "connection_id": connection_id,
                            "connector_type": connector_type
                        }
                    }
                
                return {
                    "status": "success",
                    "statusCode": 200,
                    "message": "Credential fetched successfully",
                    "connector_type": connector_type,
                    "data": filtered_connection
                }
            
            # Return all connections for the connector type
            return {
                "status": "success",
                "statusCode": 200,
                "message": f"Credentials fetched successfully",
                "connector_type": result.get('connector_type', connector_type),
                "data": {
                    'connections': result.get('connections', []),
                    'total': result.get('total', len(result.get('connections', [])))
                }
            }
            
        except (RuntimeError, ValueError) as e:
            error_msg = str(e)
            if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
                add_log(f"ConnectorCredentialsService: Encryption error retrieving credentials: {error_msg}")
            raise
        except Exception as e:
            add_log(f"ConnectorCredentialsService: Error retrieving credentials: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_credential_for_connector(
        self,
        user_id: str,
        connection_id: Optional[str],
        connector_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get credential for connector connection - filters by connector_type first, then connection_id.
        
        This method is designed to be used in connector_manager._connect() without changing
        the current implementation. It ensures credentials are validated by connector_type first.
        
        Args:
            user_id: User identifier
            connection_id: Connection identifier (optional)
            connector_type: Connector type to filter by (validated first)
            
        Returns:
            Dict with 'status' and 'data' keys (matching credential_manager format) or None if not found
            
        Note:
            Returns None if:
            - connection_id is None and connector_type is not "sample_data"
            - Credential not found for the given connector_type and connection_id
            - Connector type mismatch
        """
        try:
            # For sample_data, upload_file credentials are optional
            if not connection_id and connector_type not in ["sample_data", "file_upload"]:
                add_log(f"ConnectorCredentialsService: No connection_id provided for connector_type '{connector_type}'")
                return None
            
            # If no connection_id, return None (sample_data will handle this separately)
            if not connection_id:
                return None
            
            # Normalize connector_type by trimming whitespace (case-insensitive comparison)
            normalized_connector_type = (connector_type or "").strip().lower()
            
            # Use credential service to get credential filtered by connector_type first
            credential = await self._credential_service.get_cred_for_connection(
                user_id=user_id,
                connection_id=connection_id,
                connector_type=connector_type.strip(),  # Pass trimmed version to service
                include_credentials=True
            )
            
            if not credential:
                add_log(f"ConnectorCredentialsService: Credential not found for connection_id '{connection_id}' with connector_type '{connector_type}'")
                return None
            
            # Validate connector_type match (double-check with normalized values)
            cred_connector_type = (credential.get('connector_type') or "").strip().lower()
            if cred_connector_type and cred_connector_type != normalized_connector_type:
                add_log(f"ConnectorCredentialsService: Connector type mismatch. Expected '{connector_type}' (normalized: '{normalized_connector_type}'), got '{credential.get('connector_type')}' (normalized: '{cred_connector_type}')")
                return None
            
            # Return in the format expected by connector_manager._connect()
            # This matches the format returned by credential_manager.get_credential()
            return {
                'status': 'success',
                'data': credential
            }
            
        except (RuntimeError, ValueError) as e:
            error_msg = str(e)
            if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
                add_log(f"ConnectorCredentialsService: Encryption error retrieving credential: {error_msg}")
            raise
        except Exception as e:
            add_log(f"ConnectorCredentialsService: Error retrieving credential for connector: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    async def save_connection_result(
        self,
        session_id: str,
        connection_result: Dict[str, Any],
        connector_type: str
    ) -> bool:
        """
        Save connection result directly in the session document.
        Path: sessions/{session_id}
        
        Args:
            session_id: Session identifier
            connection_result: Connection result data to save
            connector_type: Type of connector
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            from datetime import datetime
            
            # Prepare document data to merge into session document
            doc_data = {
                'connection_result': connection_result,
                'connection_result_updated_at': datetime.utcnow()
            }
            
            # Save directly to the session document
            # Path: sessions/{session_id}
            session_doc_ref = self._firestore_service._client.collection(
                self._sessions_collection
            ).document(session_id)
            
            await asyncio.to_thread(session_doc_ref.set, doc_data, merge=True)
            
            add_log(f"ConnectorCredentialsService: Saved connection result to sessions/{session_id}")
            return True
        except Exception as e:
            # Log error but don't fail the connection if Firestore save fails
            add_log(f"ConnectorCredentialsService: Warning - Failed to save connection result to Firestore: {str(e)} | traceback: {traceback.format_exc()}")
            return False