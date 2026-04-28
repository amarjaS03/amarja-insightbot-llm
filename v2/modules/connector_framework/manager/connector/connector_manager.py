"""
Connector Manager for V2 Framework
Orchestrates connector operations with real implementations
"""
import asyncio
import io
import os
import time
import urllib.parse
import json
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
import pandas as pd
import requests

from v2.common.logger import add_log
from v2.common.gcp import GcpManager
from v2.utils.env import init_env
from v2.modules.connector_framework.factory.factory import ConnectorFactory
from v2.modules.connector_framework.core.base_connector import BaseConnector, ConnectorError
from v2.modules.connector_framework.models.connector_models import (
    ConnectorActionRequest,
    ConnectorConnectRequest,
)
from v2.modules.connector_framework.connectors.file_upload_connector import FileUploadConnector
from v2.modules.connector_framework.connectors.connectzify_connector import ConnectzifyConnector
from v2.modules.credential_framework.manager.credential.credential_manager import CredentialManager
from v2.modules.connector_framework.services.connector_credentials import ConnectorCredentialsService
from v2.modules.session_framework.manager.session.session_manager import SessionManager
import traceback
from v2.modules.connector_framework.services.data_service import (
    create_salesforce_queries_for_subject_area,
    create_acumatica_urls_for_subject_area,
    create_mssql_queries_for_tables,
    create_mysql_queries_for_tables
)
from v2.modules.connector_framework.services.data_processing.acumatica import (
    process_sales_order_data,
    process_sales_invoice_data,
    process_shipment_data,
    process_invoice_data,
    # process_item_warehouse_data,  # Removed - StockItem already provides warehouse.pkl
    process_inventory_receipt_data,
    process_stock_item_data,
    generate_preview_data
)
from v2.modules.connector_framework.config.subject_area_entity import acumatica_subject_area_entities
from v2.utils.encryption import _get_secret_from_gcp

class ConnectorManager:
    """
    Manager for connector operations.
    Handles real database/API connections and data fetching.
    One connector per session; stored in memory with session reference.
    """

    # Global in-memory registry shared across instances
    _connector_registry: Dict[str, BaseConnector] = {}
    _connector_types: Dict[str, str] = {}  # session_id -> connection_type

    def __init__(self, credential_manager: Optional[CredentialManager] = None):
        """
        Initialize connector manager.
        
        Args:
            credential_manager: Optional CredentialManager instance for credential retrieval
        """
        self._lock = asyncio.Lock()
        self._connectors = ConnectorManager._connector_registry
        self._types = ConnectorManager._connector_types
        self._credential_manager = credential_manager or CredentialManager()
        self._connector_cred_service = ConnectorCredentialsService()
        self._session_manager = SessionManager()
        self._sessions_collection = "sessions"
        add_log("ConnectorManager: Initialized with CredentialManager and SessionManager")
    
    def _get_project_root(self) -> Path:
        """Get the project root directory"""
        current_file = Path(__file__).resolve()
        # v2/modules/connector_framework/manager/connector/connector_manager.py
        return current_file.parents[5]
    
    def _get_connector_key(self, session_id: str) -> str:
        """Generate connector registry key for a session"""
        return f"connector_{session_id}"

    async def _get_connection_id_from_session(self, session_id: str) -> Optional[str]:
        """
        Retrieve connection_id from session's connection_result in Firebase.
        
        Args:
            session_id: Session identifier
            
        Returns:
            connection_id string if found, None otherwise
        """
        try:
            # Get session document from Firebase
            session_doc = await self._connector_cred_service._firestore_service._get_document(
                self._sessions_collection,
                session_id
            )
            
            if not session_doc:
                add_log(f"ConnectorManager: Session document not found for session_id: {session_id}")
                return None
            
            # Extract connection_result from session document
            connection_result = session_doc.get('connection_result')
            
            if not connection_result:
                add_log(f"ConnectorManager: connection_result not found in session {session_id}")
                return None
            
            # Extract connection_id from connection_result
            connection_id = connection_result.get('connection_id')
            
            if connection_id:
                add_log(f"ConnectorManager: Retrieved connection_id '{connection_id}' from session {session_id}")
            else:
                add_log(f"ConnectorManager: connection_id not found in connection_result for session {session_id}")
            
            return connection_id
            
        except Exception as e:
            add_log(f"ConnectorManager: Error retrieving connection_id from session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return None

    def _generate_salesforce_oauth_url(
        self,
        environment: str,
        redirect_uri: str,
        scope: str = "api refresh_token openid"
    ) -> str:
        """
        Generate Salesforce OAuth authorization URL.
        
        Uses OAuth credentials from Secret Manager (application-level).
        Same logic as v1's _generate_salesforce_oauth_url.
        
        Args:
            environment: 'sandbox' or 'production'
            redirect_uri: OAuth redirect URI
            scope: OAuth scope (default: "api refresh_token openid")
        
        Returns:
            OAuth authorization URL string
        
        Raises:
            RuntimeError: If Secret Manager access fails or credentials invalid
        """
        try:
            # Get OAuth credentials from Secret Manager
            oauth_credentials_json = _get_secret_from_gcp('salesforce_oauth_credentials')
            
            if not oauth_credentials_json:
                raise RuntimeError("Failed to retrieve OAuth credentials from Secret Manager")
            
            # Parse JSON
            try:
                oauth_credentials = json.loads(oauth_credentials_json)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid OAuth credentials format in Secret Manager: {str(e)}")
            
            # Validate required fields
            required_fields = ['client_id', 'client_secret']
            missing_fields = [f for f in required_fields if f not in oauth_credentials]
            if missing_fields:
                raise RuntimeError(f"Missing required OAuth fields: {missing_fields}")
            
            client_id = oauth_credentials['client_id']
            
            # Get base URL from constants
            constants = init_env()
            if environment.lower() == "production":
                base_url = constants.get('salesforce_base_url_prod')
            else:
                base_url = constants.get('salesforce_base_url_sandbox')
            
            if not base_url:
                raise RuntimeError(f"Salesforce base URL not configured for environment: {environment}")
            
            # Build OAuth URL (same as v1)
            params = {
                'response_type': 'code',
                'client_id': client_id,
                'redirect_uri': redirect_uri,
                'scope': scope,
                'state': environment,
                'prompt': 'login consent'
            }
            
            query_parts = []
            for key, value in params.items():
                if key in ['redirect_uri', 'scope']:
                    query_parts.append(f"{key}={value}")
                else:
                    query_parts.append(f"{key}={urllib.parse.quote(value)}")
            
            query_string = '&'.join(query_parts)
            oauth_url = f"{base_url}?{query_string}"
            
            add_log(f"ConnectorManager: Generated Salesforce OAuth URL for environment: {environment}")
            return oauth_url
            
        except Exception as e:
            add_log(f"ConnectorManager: Failed to generate Salesforce OAuth URL: {str(e)} | traceback: {traceback.format_exc()}")
            raise RuntimeError(f"Failed to generate Salesforce OAuth URL: {str(e)}")

    async def _exchange_salesforce_authorization_code(
        self,
        authorization_code: str,
        state: str,
        redirect_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Exchange Salesforce authorization code for access tokens.
        
        Uses OAuth credentials from Secret Manager (application-level).
        Same logic as v1's oauth_callback.
        
        Args:
            authorization_code: Authorization code from Salesforce
            state: OAuth state parameter (contains environment)
            redirect_uri: Must match the redirect_uri used in the OAuth request (from session or constants)
        
        Returns:
            Dict with access_token, instance_url, refresh_token, etc.
        
        Raises:
            RuntimeError: If Secret Manager access fails or token exchange fails
            ConnectionError: If token exchange returns error
        """
        try:
            # Get OAuth credentials from Secret Manager
            oauth_credentials_json = await asyncio.to_thread(_get_secret_from_gcp, 'salesforce_oauth_credentials')
            
            if not oauth_credentials_json:
                raise RuntimeError("Failed to retrieve OAuth credentials from Secret Manager")
            
            # Parse JSON
            try:
                oauth_credentials = json.loads(oauth_credentials_json)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid OAuth credentials format in Secret Manager: {str(e)}")
            
            # Validate required fields
            required_fields = ['client_id', 'client_secret']
            missing_fields = [f for f in required_fields if f not in oauth_credentials]
            if missing_fields:
                raise RuntimeError(f"Missing required OAuth fields: {missing_fields}")
            
            # Determine environment from state
            environment = state if state in ['sandbox', 'production'] else 'production'
            
            # Get token URL and redirect_uri (use passed value or constants)
            constants = init_env()
            if environment == 'production':
                token_url = constants.get('salesforce_token_url_prod')
            else:
                token_url = constants.get('salesforce_token_url_sandbox')
            
            if not redirect_uri:
                redirect_uri = constants.get('salesforce_redirect_url')
            
            if not token_url or not redirect_uri:
                raise RuntimeError(f"Salesforce token URL or redirect URI not configured")
            
            # Prepare token exchange payload
            token_payload = {
                'grant_type': 'authorization_code',
                'code': authorization_code,
                'client_id': oauth_credentials['client_id'],
                'client_secret': oauth_credentials['client_secret'],
                'redirect_uri': redirect_uri
            }
            
            # Exchange code for tokens (use asyncio.to_thread for sync requests.post)
            add_log(f"ConnectorManager: Exchanging Salesforce authorization code for tokens")
            token_response = await asyncio.to_thread(
                requests.post,
                token_url,
                data=token_payload,
                timeout=30
            )
            
            if token_response.status_code == 200:
                token_data = token_response.json()
                add_log(f"ConnectorManager: Successfully exchanged Salesforce authorization code")
                return token_data
            else:
                error_msg = f"Salesforce token exchange failed: {token_response.status_code} - {token_response.text}"
                add_log(f"ConnectorManager: {error_msg}")
                raise ConnectionError(error_msg)
                
        except requests.RequestException as e:
            add_log(f"ConnectorManager: Network error during Salesforce token exchange: {str(e)}")
            raise RuntimeError(f"Network error during token exchange: {str(e)}")
        except Exception as e:
            add_log(f"ConnectorManager: Failed to exchange Salesforce authorization code: {str(e)} | traceback: {traceback.format_exc()}")
            raise RuntimeError(f"Failed to exchange authorization code: {str(e)}")

    async def _connect(self, req: ConnectorConnectRequest) -> Dict[str, Any]:
        """
        Create and connect a connector for a session.
        
        Args:
            req: Connection request with user_id, session_id, type, and cred_id
            
        Returns:
            Dict with connection result
        """
        async with self._lock:
            try:
                add_log(f"ConnectorManager._connect: user={req.user_id}, session={req.session_id}, type={req.type}")
                
                # Normalize connector_type by trimming whitespace
                normalized_type = (req.type or "").strip()
                
                # Handle salesforce-oauth: Skip credential check and go directly to OAuth URL generation
                if normalized_type == "salesforce-oauth":
                    try:
                        # Get environment from request params (default to sandbox)
                        environment = "sandbox"
                        if req.params:
                            environment = req.params.get("environment", "sandbox").lower()
                        if environment not in ["sandbox", "production"]:
                            environment = "sandbox"
                        
                        # Get redirect_uri: prefer frontend origin (params) so OAuth callback lands where user started
                        constants = init_env()
                        redirect_uri = (req.params or {}).get("redirect_uri") or constants.get("salesforce_redirect_url")
                        scope = constants.get("salesforce_oauth_scope", "api refresh_token openid")
                        
                        if not redirect_uri:
                            return {
                                "result": "fail",
                                "message": "Salesforce redirect URI not configured",
                                "data": None
                            }
                        
                        # Generate OAuth URL using Secret Manager OAuth credentials
                        oauth_url = self._generate_salesforce_oauth_url(environment, redirect_uri, scope)
                        
                        # Store redirect_uri in session so token exchange uses the same value
                        await self._session_manager._update_session_fields(req.session_id, {"_oauth_redirect_uri": redirect_uri})
                        
                        # Store normalized type as "salesforce" for callback compatibility
                        self._types[req.session_id] = "salesforce"
                        
                        # Update session status and steps for OAuth flow
                        try:
                            await self._session_manager._update_session_status(req.session_id, "initiated")
                            
                            # Set initial step (step 1: Connect to Salesforce)
                            from v2.modules.connector_framework.config.workflow_steps import CONNECTOR_WORKFLOW_STEPS
                            from v2.modules.job_framework.core.manager import _normalize_connector_type
                            
                            connector_type = _normalize_connector_type("salesforce")  # Normalize to salesforce
                            workflow = CONNECTOR_WORKFLOW_STEPS.get(connector_type, {})
                            initial_step = "1"
                            next_step = workflow.get("1", {}).get("next", "2") if workflow else "2"
                            
                            await self._session_manager._update_session_fields(req.session_id, {
                                "current_step": initial_step,
                                "next_step": next_step,
                                "credential_id": req.cred_id,
                                "data_source": "salesforce"  # Use normalized type
                            })
                            add_log(f"ConnectorManager: Updated session {req.session_id} to step {initial_step} (next: {next_step}) for salesforce-oauth")
                        except Exception as step_error:
                            add_log(f"ConnectorManager: Warning - Failed to update session step: {str(step_error)} | traceback: {traceback.format_exc()}")
                        
                        # Return CHALLENGE status with authorization_url
                        return {
                            "result": "success",
                            "status": "CHALLENGE",
                            "message": "OAuth authorization required",
                            "data": {
                                "status": "CHALLENGE",
                                "authorization_url": oauth_url,
                                "state": environment,
                                "challenge": {
                                    "authorization_url": oauth_url,
                                    "state": environment
                                },
                                "connector_type": "salesforce"  # Normalized to salesforce
                            }
                        }
                    except Exception as oauth_err:
                        add_log(f"ConnectorManager: Failed to generate Salesforce OAuth URL: {str(oauth_err)} | traceback: {traceback.format_exc()}")
                        return {
                            "result": "fail",
                            "message": f"Failed to generate OAuth URL: {str(oauth_err)}",
                            "data": None
                        }
                
                # Get credentials from credential_framework - filtered by connector_type first
                connection_data = await self._connector_cred_service.get_credential_for_connector(
                    user_id=req.user_id,
                    connection_id=req.cred_id if req.cred_id else None,
                    connector_type=normalized_type
                )
                
                credentials = connection_data.get('data', {}) if connection_data else {}

                # For sample_data, credentials are optional
                if not connection_data and normalized_type not in ["sample_data", "file_upload"]:
                    return {
                        "result": "fail",
                        "message": "No credentials found. Please save credentials first.",
                        "data": None
                    }
                
                # Use empty dict for sample_data if no credentials
                if not connection_data:
                    connection_data = {}
                
                # Extract credentials - handle both nested and flat structures
                creds_dict = credentials.get('credentials', credentials) if isinstance(credentials, dict) else {}
                
                # Add any extra params from request to credentials
                if req.params:
                    creds_dict.update(req.params)
                
                # Connectzify expects Acumatica URL in "site" and OAuth "redirect_uri"; app may not store them
                if normalized_type == "acumatica":
                    if not creds_dict.get("site"):
                        creds_dict["site"] = creds_dict.get("tenant_url") or creds_dict.get("instance_url")
                    if not creds_dict.get("redirect_uri"):
                        constants = init_env()
                        creds_dict["redirect_uri"] = constants.get("acumatica_redirect_url")
                
                # Create connector via factory (factory will normalize internally, but we pass normalized for consistency)
                connector = ConnectorFactory.create_connector(normalized_type, creds_dict)
                
                # Establish connection
                connection_result = await connector.connect()
                print(f"Connection result Manager: {connection_result}")
                # Check if result is a dict (from ConnectzifyConnector) or bool (from other connectors)
                # if isinstance(connection_result, dict):
                    # For Connectzify connectors, extract status and return with status field
                status = connection_result.get("status")
                    # Store in registry
                connector_key = self._get_connector_key(req.session_id)
                self._connectors[connector_key] = connector
                self._types[req.session_id] = req.type
                
                add_log(f"ConnectorManager: Connected {req.type} connector for session {req.session_id}, status={status}")
                
                    # Update session status to "initiated" and set initial step when connector connects
                try:
                    await self._session_manager._update_session_status(req.session_id, "initiated")
                        
                    # Set initial step based on connector type
                    try:
                        from v2.modules.connector_framework.config.workflow_steps import CONNECTOR_WORKFLOW_STEPS
                        from v2.modules.job_framework.core.manager import _normalize_connector_type
                        
                            # Use proper normalization function (handles csv/excel, Sample Data, etc.)
                        connector_type = _normalize_connector_type(req.type)
                        workflow = CONNECTOR_WORKFLOW_STEPS.get(connector_type, {})
                        initial_step = "1"
                        next_step = workflow.get("1", {}).get("next", "2") if workflow else "2"
                            
                            # Use _update_session_fields for dict updates
                        await self._session_manager._update_session_fields(req.session_id, {
                           "current_step": initial_step,
                           "next_step": next_step,
                           "credential_id": req.cred_id,
                           "data_source": req.type
                        })
                        add_log(f"ConnectorManager: Updated session {req.session_id} to step {initial_step} (next: {next_step}) for connector type '{connector_type}'")
                    except Exception as step_error:
                        add_log(f"ConnectorManager: Warning - Failed to update session step: {str(step_error)} | traceback: {traceback.format_exc()}")
                        
                    add_log(f"ConnectorManager: Updated session {req.session_id} status to 'initiated'")
                except Exception as status_error:
                    add_log(f"ConnectorManager: Warning - Failed to update session status: {str(status_error)}")
                
                # Save connection result to Firestore
                try:
                    await self._connector_cred_service.save_connection_result(
                        session_id=req.session_id,
                        connection_result=connection_result,
                        connector_type=normalized_type
                    )
                except Exception as save_error:
                    # Log but don't fail the connection if save fails
                    add_log(f"ConnectorManager: Warning - Failed to save connection result: {str(save_error)}")
                
                # Return connection result with status field extracted from ConnectionResult
                return {
                    "result": "success",
                    "status": status,  # Extracted from ConnectionResult.status (e.g., "CHALLENGE")
                    "data": connection_result
                    }
                # else:
                #     # For other connectors that return bool (direct connections like SQL)
                #     # Store in registry
                #     connector_key = self._get_connector_key(req.session_id)
                #     self._connectors[connector_key] = connector
                #     self._types[req.session_id] = req.type
                    
                #     add_log(f"ConnectorManager: Connected {req.type} connector for session {req.session_id}")
                    
                #     # Update session status to "initiated" and set initial step when connector connects
                #     try:
                #         await self._session_manager._update_session_status(req.session_id, "initiated")
                        
                #         # Set initial step based on connector type
                #         try:
                #             from v2.modules.connector_framework.config.workflow_steps import CONNECTOR_WORKFLOW_STEPS
                #             from v2.modules.job_framework.core.manager import _normalize_connector_type
                            
                #             # Use proper normalization function (handles csv/excel, Sample Data, etc.)
                #             connector_type = _normalize_connector_type(req.type)
                #             workflow = CONNECTOR_WORKFLOW_STEPS.get(connector_type, {})
                #             initial_step = "1"
                #             next_step = workflow.get("1", {}).get("next", "2") if workflow else "2"
                            
                #             # Use _update_session_fields for dict updates
                #             await self._session_manager._update_session_fields(req.session_id, {
                #                 "current_step": initial_step,
                #                 "next_step": next_step
                #             })
                #             add_log(f"ConnectorManager: Updated session {req.session_id} to step {initial_step} (next: {next_step}) for connector type '{connector_type}'")
                #         except Exception as step_error:
                #             add_log(f"ConnectorManager: Warning - Failed to update session step: {str(step_error)} | traceback: {traceback.format_exc()}")
                        
                #         add_log(f"ConnectorManager: Updated session {req.session_id} status to 'initiated'")
                #     except Exception as status_error:
                #         add_log(f"ConnectorManager: Warning - Failed to update session status: {str(status_error)}")
                    
                #     # Return success response for direct connections (SQL, etc.)
                #     return {
                #         "result": "success",
                #         "status": "success",  # Direct connections are always successful
                #         "data": {
                #             "type": req.type,
                #             "session_id": req.session_id,
                #             "connected": True
                #         }
                #     }
            except ConnectorError as e:
                add_log(f"ConnectorManager._connect error: {str(e)} | traceback: {traceback.format_exc()}")
                return {
                    "result": "fail",
                    "message": str(e),
                    "data": None
                }
            except Exception as e:
                add_log(f"ConnectorManager._connect error: {str(e)} | traceback: {traceback.format_exc()}")
                return {
                    "result": "fail",
                    "message": f"Connection failed: {str(e)}",
                    "data": None
                }

    def _get_connector(self, session_id: str) -> Optional[BaseConnector]:
        """Get the connector for a session"""
        connector_key = self._get_connector_key(session_id)
        return self._connectors.get(connector_key)

    async def _handle_oauth_callback(
        self,
        session_id: str,
        user_id: str,
        connector_type: str,
        authorization_code: str,
        state: str
    ) -> Dict[str, Any]:
        """
        Handle OAuth callback for Salesforce and Acumatica connectors.
        
        Args:
            session_id: Session identifier
            user_id: User identifier
            connector_type: Connector type (must be 'salesforce' or 'acumatica')
            authorization_code: OAuth authorization code from provider
            state: OAuth state parameter for CSRF protection
            
        Returns:
            Dict with connection result after code exchange
            
        Raises:
            ValueError: If connector_type is not salesforce or acumatica
            ConnectionError: If connector not found or exchange fails
        """
        async with self._lock:
            try:
                # Normalize connector type
                normalized_type = (connector_type or "").strip().lower()
                
                # Validate connector type - only Salesforce and Acumatica support OAuth
                if normalized_type not in ["salesforce", "acumatica"]:
                    raise ValueError(
                        f"OAuth callback is only supported for 'salesforce' and 'acumatica'. "
                        f"Received: '{connector_type}'"
                    )
                
                # Validate required parameters
                if not authorization_code:
                    raise ValueError("Authorization code is required")
                if not state:
                    raise ValueError("State parameter is required")
                if not session_id:
                    raise ValueError("Session ID is required")
                
                add_log(
                    f"ConnectorManager._handle_oauth_callback: "
                    f"session={session_id}, user={user_id}, type={normalized_type}"
                )
                
                # Salesforce-specific OAuth callback handling (direct OAuth, not Connectzify)
                # Handle Salesforce before checking for connector (Salesforce doesn't create connector until callback)
                if normalized_type == "salesforce":
                    try:
                        # Use redirect_uri from session (stored at OAuth initiation) so it matches the request
                        session_doc = await self._connector_cred_service._firestore_service._get_document(
                            "sessions", session_id
                        )
                        oauth_redirect_uri = (session_doc or {}).get("_oauth_redirect_uri")
                        # Exchange authorization code for tokens using direct API call
                        token_data = await self._exchange_salesforce_authorization_code(
                            authorization_code, state, redirect_uri=oauth_redirect_uri
                        )
                        
                        # Extract tokens
                        access_token = token_data.get("access_token")
                        instance_url = token_data.get("instance_url")
                        refresh_token = token_data.get("refresh_token")
                        
                        if not access_token or not instance_url:
                            raise ConnectionError("Failed to obtain access_token or instance_url from Salesforce")
                        
                        # Prepare credentials dict with tokens (for Firestore - user-specific)
                        user_creds_dict = {
                            "access_token": access_token,
                            "instance_url": instance_url
                        }
                        if refresh_token:
                            user_creds_dict["refresh_token"] = refresh_token
                        
                        # Save tokens to Firestore via credential framework
                        # Get connection_id from session if available, otherwise create new
                        connection_id = await self._get_connection_id_from_session(session_id)
                        
                        if not connection_id:
                            # Create new credential entry
                            connection_name = f"Salesforce Connection {session_id[:8]}"
                            try:
                                save_result = await self._credential_manager.save_credential(
                                    user_id=user_id,
                                    connector_type=normalized_type,
                                    connection_name=connection_name,
                                    credentials_data=user_creds_dict
                                )
                                # Extract connection_id string from dict response
                                connection_id = save_result.get('connection_id') if isinstance(save_result, dict) else str(save_result)
                                add_log(f"ConnectorManager: Saved Salesforce tokens to Firestore with connection_id: {connection_id}")
                            except Exception as save_err:
                                add_log(f"ConnectorManager: Warning - Failed to save tokens to Firestore: {str(save_err)}")
                                # Continue anyway - tokens are in memory
                        else:
                            # Update existing credential
                            try:
                                await self._credential_manager.update_credential(
                                    user_id=user_id,
                                    connection_id=connection_id,
                                    credentials_data=user_creds_dict
                                )
                                add_log(f"ConnectorManager: Updated Salesforce tokens in Firestore for connection_id: {connection_id}")
                            except Exception as update_err:
                                add_log(f"ConnectorManager: Warning - Failed to update tokens in Firestore: {str(update_err)}")
                                # Continue anyway - tokens are in memory
                        
                        # For Salesforce OAuth: ConnectzifyConnector only needs access_token and instance_url
                        # (NOT client_id, client_secret, redirect_uri - those are only for Acumatica)
                        # Create connector with user tokens only
                        connector_creds_dict = user_creds_dict.copy()  # Contains access_token, instance_url, refresh_token
                        
                        add_log(f"ConnectorManager: Creating Salesforce connector with user tokens (OAuth flow)")
                        
                        # Validate tokens by making a test API call to Salesforce
                        # We don't call connector.connect() because Connectzify would try to initiate OAuth again
                        try:
                            test_url = f"{instance_url}/services/oauth2/userinfo"
                            headers = {"Authorization": f"Bearer {access_token}"}
                            test_response = await asyncio.to_thread(requests.get, test_url, headers=headers, timeout=10)
                            
                            if test_response.status_code == 200:
                                user_info = test_response.json()
                                add_log(f"ConnectorManager: Validated Salesforce tokens successfully for user: {user_info.get('preferred_username', 'unknown')}")
                            else:
                                add_log(f"ConnectorManager: Warning - Token validation returned status {test_response.status_code}")
                        except Exception as validate_err:
                            add_log(f"ConnectorManager: Warning - Token validation failed: {str(validate_err)}")
                            # Continue anyway - tokens might still be valid
                        
                        # Create connector with tokens only (no OAuth credentials needed for Salesforce)
                        # We don't call connect() here because we've already validated tokens
                        connector = ConnectorFactory.create_connector(normalized_type, connector_creds_dict)
                        
                        # Store connector in registry (without calling connect - tokens already validated)
                        connector_key = self._get_connector_key(session_id)
                        self._connectors[connector_key] = connector
                        self._types[session_id] = normalized_type
                        
                        # Generate a connection_id if we don't have one
                        if not connection_id:
                            import uuid
                            connection_id = f"salesforce{uuid.uuid4().hex[:32]}"
                        
                        # Create connection result with READY status
                        connection_result = {
                            "status": "READY",
                            "connection_id": connection_id,
                            "session": {
                                "status": "READY",
                                "connected": True,
                                "access_token": access_token[:20] + "..." if access_token else None,  # Partial token for logging
                                "instance_url": instance_url
                            }
                        }
                        
                        # Save connection result to Firestore
                        try:
                            await self._connector_cred_service.save_connection_result(
                                session_id=session_id,
                                connection_result=connection_result,
                                connector_type=normalized_type
                            )
                        except Exception as save_err:
                            add_log(f"ConnectorManager: Warning - Failed to save connection result: {str(save_err)}")
                        
                        # Update session steps: Connection complete, advance to step 2 (Select Subject)
                        try:
                            from v2.modules.connector_framework.config.workflow_steps import CONNECTOR_WORKFLOW_STEPS
                            from v2.modules.job_framework.core.manager import _normalize_connector_type
                            
                            connector_type = _normalize_connector_type(normalized_type)
                            workflow = CONNECTOR_WORKFLOW_STEPS.get(connector_type, {})
                            
                            # Connection complete - advance to step 2 (Select Subject)
                            current_step = "2"
                            next_step = workflow.get("2", {}).get("next", "3") if workflow else "3"
                            
                            await self._session_manager._update_session_fields(session_id, {
                                "current_step": current_step,
                                "next_step": next_step,
                                "credential_id": connection_id,
                                "data_source": normalized_type
                            })
                            add_log(f"ConnectorManager: Updated session {session_id} to step {current_step} (next: {next_step}) after Salesforce OAuth callback")
                        except Exception as step_error:
                            add_log(f"ConnectorManager: Warning - Failed to update session step after callback: {str(step_error)} | traceback: {traceback.format_exc()}")
                        
                        # Return READY status (same format as Acumatica)
                        return {
                            "result": "success",
                            "status": "READY",
                            "message": "Salesforce OAuth callback processed successfully",
                            "data": {
                                "status": "READY",
                                "connection_id": connection_id,
                                "session": connection_result.get("session")
                            }
                        }
                    except Exception as sf_err:
                        add_log(f"ConnectorManager: Salesforce OAuth callback failed: {str(sf_err)} | traceback: {traceback.format_exc()}")
                        return {
                            "result": "fail",
                            "message": f"Salesforce OAuth callback failed: {str(sf_err)}",
                            "data": None
                        }
                
                # Get connector from registry (for Acumatica/Connectzify)
                connector = self._get_connector(session_id)
                
                if not connector:
                    raise ConnectionError(
                        f"Connector not found for session {session_id}. "
                        f"Please connect first before handling OAuth callback."
                    )
                
                # Verify connector type matches
                stored_type = self._types.get(session_id, "").lower()
                if stored_type != normalized_type:
                    add_log(
                        f"ConnectorManager: Warning - Stored type '{stored_type}' "
                        f"does not match requested type '{normalized_type}'"
                    )
                    try:
                        # Exchange authorization code for tokens using direct API call
                        token_data = await self._exchange_salesforce_authorization_code(authorization_code, state)
                        
                        # Extract tokens
                        access_token = token_data.get("access_token")
                        instance_url = token_data.get("instance_url")
                        refresh_token = token_data.get("refresh_token")
                        
                        if not access_token or not instance_url:
                            raise ConnectionError("Failed to obtain access_token or instance_url from Salesforce")
                        
                        # Prepare credentials dict with tokens
                        user_creds_dict = {
                            "access_token": access_token,
                            "instance_url": instance_url
                        }
                        if refresh_token:
                            user_creds_dict["refresh_token"] = refresh_token
                        
                        # Save tokens to Firestore via credential framework
                        # Get connection_id from session if available, otherwise create new
                        connection_id = await self._get_connection_id_from_session(session_id)
                        
                        if not connection_id:
                            # Create new credential entry
                            connection_name = f"Salesforce Connection {session_id[:8]}"
                            try:
                                save_result = await self._credential_manager.save_credential(
                                    user_id=user_id,
                                    connector_type=normalized_type,
                                    connection_name=connection_name,
                                    credentials_data=user_creds_dict
                                )
                                # Extract connection_id string from dict response
                                connection_id = save_result.get('connection_id') if isinstance(save_result, dict) else str(save_result)
                                add_log(f"ConnectorManager: Saved Salesforce tokens to Firestore with connection_id: {connection_id}")
                            except Exception as save_err:
                                add_log(f"ConnectorManager: Warning - Failed to save tokens to Firestore: {str(save_err)}")
                                # Continue anyway - tokens are in memory
                        else:
                            # Update existing credential
                            try:
                                await self._credential_manager.update_credential(
                                    user_id=user_id,
                                    connection_id=connection_id,
                                    credentials_data=user_creds_dict
                                )
                                add_log(f"ConnectorManager: Updated Salesforce tokens in Firestore for connection_id: {connection_id}")
                            except Exception as update_err:
                                add_log(f"ConnectorManager: Warning - Failed to update tokens in Firestore: {str(update_err)}")
                                # Continue anyway - tokens are in memory
                        
                        # Create connector with tokens (ConnectzifyConnector will use these tokens)
                        connector = ConnectorFactory.create_connector(normalized_type, user_creds_dict)
                        
                        # Connect with tokens
                        connection_result = await connector.connect()
                        
                        # Store connector in registry
                        connector_key = self._get_connector_key(session_id)
                        self._connectors[connector_key] = connector
                        self._types[session_id] = normalized_type
                        
                        # Extract status from connection result
                        status = connection_result.get("status") if isinstance(connection_result, dict) else "READY"
                        
                        # Save connection result to Firestore
                        try:
                            await self._connector_cred_service.save_connection_result(
                                session_id=session_id,
                                connection_result={
                                    "status": status,
                                    "connection_id": getattr(connector, "_connection_id", connection_id),
                                    "session": connection_result if isinstance(connection_result, dict) else None
                                },
                                connector_type=normalized_type
                            )
                        except Exception as save_err:
                            add_log(f"ConnectorManager: Warning - Failed to save connection result: {str(save_err)}")
                        
                        # Return READY status (same format as Acumatica)
                        return {
                            "result": "success",
                            "status": "READY",
                            "message": "Salesforce OAuth callback processed successfully",
                            "data": {
                                "status": "READY",
                                "connection_id": getattr(connector, "_connection_id", connection_id),
                                "session": connection_result if isinstance(connection_result, dict) else None
                            }
                        }
                    except Exception as sf_err:
                        add_log(f"ConnectorManager: Salesforce OAuth callback failed: {str(sf_err)} | traceback: {traceback.format_exc()}")
                        return {
                            "result": "fail",
                            "message": f"Salesforce OAuth callback failed: {str(sf_err)}",
                            "data": None
                        }
                
                # Check if connector has exchange_code method (should be ConnectzifyConnector)
                if not hasattr(connector, "exchange_code"):
                    raise ConnectionError(
                        f"Connector for session {session_id} does not support OAuth code exchange. "
                        f"Expected ConnectzifyConnector."
                    )
                
                # Exchange authorization code for tokens
                add_log(f"ConnectorManager: Exchanging authorization code for session {session_id}")
                connection_result = await connector.exchange_code(authorization_code, state)
                
                # Extract status from result
                status = connection_result.get("status") if isinstance(connection_result, dict) else None
                
                add_log(
                    f"ConnectorManager: OAuth code exchange successful for session {session_id}, "
                    f"status={status}"
                )
                
                # Save updated connection result to Firestore
                try:
                    await self._connector_cred_service.save_connection_result(
                        session_id=session_id,
                        connection_result=connection_result,
                        connector_type=normalized_type
                    )
                    add_log(f"ConnectorManager: Saved updated connection result for session {session_id}")
                except Exception as save_error:
                    # Log but don't fail the callback if save fails
                    add_log(
                        f"ConnectorManager: Warning - Failed to save connection result: "
                        f"{str(save_error)} | traceback: {traceback.format_exc()}"
                    )
                
                # Update session status if connection is now READY
                if status == "READY":
                    try:
                        await self._session_manager._update_session_status(session_id, "initiated")
                        add_log(f"ConnectorManager: Updated session {session_id} status to 'initiated'")
                    except Exception as status_error:
                        add_log(
                            f"ConnectorManager: Warning - Failed to update session status: "
                            f"{str(status_error)}"
                        )
                
                # Return success response
                return {
                    "result": "success",
                    "status": status or "success",
                    "message": "OAuth callback processed successfully",
                    "data": connection_result
                }
                
            except ValueError as e:
                add_log(f"ConnectorManager._handle_oauth_callback validation error: {str(e)}")
                return {
                    "result": "fail",
                    "message": str(e),
                    "data": None
                }
            except ConnectionError as e:
                add_log(f"ConnectorManager._handle_oauth_callback connection error: {str(e)}")
                return {
                    "result": "fail",
                    "message": str(e),
                    "data": None
                }
            except Exception as e:
                add_log(
                    f"ConnectorManager._handle_oauth_callback error: {str(e)} | "
                    f"traceback: {traceback.format_exc()}"
                )
                return {
                    "result": "fail",
                    "message": f"OAuth callback failed: {str(e)}",
                    "data": None
                }

    async def health(self, session_id: str) -> bool:
        """
        Check if connector for session is healthy.
        
        Args:
            session_id: Session identifier
            
        Returns:
            bool: True if connector is healthy
        """
        async with self._lock:
            connector = self._get_connector(session_id)
            if not connector:
                return False

            try:
                # For Salesforce OAuth connectors, check if credentials exist instead of connection test
                # (OAuth flow doesn't call connect(), so test_connection() would fail)
                connector_type = self._types.get(session_id, "").lower()
                if connector_type == "salesforce" and isinstance(connector, ConnectzifyConnector):
                    # Check if credentials are available (OAuth flow where connect() wasn't called)
                    if hasattr(connector, '_credentials') and connector._credentials:
                        access_token = connector._credentials.get('access_token')
                        instance_url = connector._credentials.get('instance_url')
                        if access_token and instance_url:
                            add_log(f"ConnectorManager: Salesforce OAuth connector has valid credentials")
                            return True
                    
                    # If credentials not in _credentials, try session (normal flow)
                    if connector._session:
                        return await connector.test_connection()
                    
                    # No credentials and no session
                    add_log(f"ConnectorManager: Salesforce connector has no credentials or session")
                    return False
                
                # For other connectors, use normal health check
                return await connector.test_connection()
            except Exception as e:
                add_log(f"ConnectorManager: Health check error: {str(e)}")
                return False

    async def _fetch_schema(self, req: ConnectorActionRequest) -> Dict[str, Any]:
        """
        Fetch schema from the connected data source.
        
        Args:
            req: Action request with user_id and session_id
            
        Returns:
            Dict with schema data (tables, views, objects)
        """
        try:
            # Check connector health
            healthy = await self.health(req.session_id)
            if not healthy:
                return {
                    "result": "fail",
                    "message": "Connector not active. Please connect again.",
                    "data": None
                }

            connector = self._get_connector(req.session_id)
            if not connector:
                return {
                    "result": "fail",
                    "message": "Connector not found for session",
                    "data": None
                }
            
            # Get schema from connector
            schema = await connector.get_schema()
            
            add_log(f"ConnectorManager: Retrieved schema for session {req.session_id}")
            
            return {
                "result": "success",
                "message": "Schema retrieved successfully",
                "data": schema
            }
        except ConnectorError as e:
            add_log(f"ConnectorManager._fetch_schema error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": str(e),
                "data": None
            }
        except Exception as e:
            add_log(f"ConnectorManager._fetch_schema error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": f"Failed to fetch schema: {str(e)}",
                "data": None
            }
    
    async def _list_tables(self, req: ConnectorActionRequest) -> Dict[str, Any]:
        """
        List tables and views for database connectors (MSSQL/MySQL).
        
        Similar to v1's /db/list_tables endpoint, uses connector's get_schema() method
        to fetch tables and views, then transforms to simple string arrays.
        
        Args:
            req: Action request with user_id, session_id, and connector_type
            
        Returns:
            Dict with 'tables' and 'views' as simple string arrays
        """
        try:
            # Only support database connectors
            normalized_type = req.connector_type.lower().strip()
            if normalized_type not in ["mssql", "mysql"]:
                return {
                    "result": "fail",
                    "message": f"list-tables endpoint only supports database connectors (mssql, mysql), got: {req.connector_type}",
                    "data": None
                }
            
            # Check connector health
            healthy = await self.health(req.session_id)
            if not healthy:
                return {
                    "result": "fail",
                    "message": "Connector not active. Please connect again.",
                    "data": None
                }

            connector = self._get_connector(req.session_id)
            if not connector:
                return {
                    "result": "fail",
                    "message": "Connector not found for session",
                    "data": None
                }
            
            # Get schema from connector
            schema = await connector.get_schema()
            
            # Transform schema to simple string arrays (matching v1 format)
            tables = []
            views = []
            
            if schema and isinstance(schema, dict):
                # Extract tables
                tables_metadata = schema.get("tables", [])
                if tables_metadata and isinstance(tables_metadata, list):
                    if len(tables_metadata) > 0 and isinstance(tables_metadata[0], dict):
                        # Extract table_name from objects
                        tables = [item.get("table_name", item) if isinstance(item, dict) else item for item in tables_metadata]
                    else:
                        # Already strings
                        tables = tables_metadata
                
                # Extract views
                views_metadata = schema.get("views", [])
                if views_metadata and isinstance(views_metadata, list):
                    if len(views_metadata) > 0 and isinstance(views_metadata[0], dict):
                        # Extract table_name from objects
                        views = [item.get("table_name", item) if isinstance(item, dict) else item for item in views_metadata]
                    else:
                        # Already strings
                        views = views_metadata
            
            add_log(f"ConnectorManager: Listed tables for {normalized_type} - {len(tables)} tables, {len(views)} views")
            
            return {
                "result": "success",
                "message": "Tables and views retrieved successfully",
                "data": {
                    "tables": tables if isinstance(tables, list) else [],
                    "views": views if isinstance(views, list) else []
                }
            }
        except ConnectorError as e:
            add_log(f"ConnectorManager._list_tables error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": str(e),
                "data": None
            }
        except Exception as e:
            add_log(f"ConnectorManager._list_tables error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": f"Failed to list tables: {str(e)}",
                "data": None
            }

    def _get_acumatica_credentials_from_connector(self, connector: BaseConnector) -> Optional[Dict[str, str]]:
        """
        Extract access_token and tenant_url from Connectzify connector's Acumatica session.
        
        Args:
            connector: ConnectzifyConnector instance
            
        Returns:
            Dict with 'access_token' and 'tenant_url', or None if not available
        """
        try:
            if not isinstance(connector, ConnectzifyConnector):
                return None
            
            if not connector._session:
                add_log("ConnectorManager: No Connectzify session available")
                return None
            
            # Access the underlying Acumatica connector from Connectzify session
            acumatica_connector = connector._session.connector
            
            # Extract access_token
            access_token = getattr(acumatica_connector, 'access_token', None)
            if not access_token:
                add_log("ConnectorManager: No access_token found in Acumatica connector")
                return None
            
            # Extract tenant_url (stored as 'site' in credentials)
            credentials = getattr(acumatica_connector, 'credentials', {})
            tenant_url = credentials.get('site') or credentials.get('tenant_url') or credentials.get('instance_url')
            
            if not tenant_url:
                add_log("ConnectorManager: No tenant_url found in Acumatica connector credentials")
                return None
            
            return {
                'access_token': access_token,
                'tenant_url': tenant_url.rstrip('/')
            }
        except Exception as e:
            add_log(f"ConnectorManager: Error extracting Acumatica credentials: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    def _get_salesforce_credentials_from_connector(self, connector: BaseConnector) -> Optional[Dict[str, str]]:
        """
        Extract access_token and instance_url from Connectzify connector's Salesforce session.
        
        Supports both:
        1. Normal flow: credentials from Connectzify session (after connect())
        2. OAuth flow: credentials from connector._credentials (when connect() wasn't called)
        
        Args:
            connector: ConnectzifyConnector instance
            
        Returns:
            Dict with 'access_token' and 'instance_url', or None if not available
        """
        try:
            if not isinstance(connector, ConnectzifyConnector):
                return None
            
            # First, try to get from session (for connectors that called connect())
            if connector._session:
                # Access the underlying Salesforce connector from Connectzify session
                salesforce_connector = connector._session.connector
                
                # Extract access_token
                access_token = getattr(salesforce_connector, 'access_token', None)
                if not access_token:
                    add_log("ConnectorManager: No access_token found in Salesforce connector")
                    return None
                
                # Extract instance_url
                instance_url = getattr(salesforce_connector, 'instance_url', None)
                if not instance_url:
                    # Try to get from credentials
                    credentials = getattr(salesforce_connector, 'credentials', {})
                    instance_url = credentials.get('instance_url') or credentials.get('instanceURL')
                
                if not instance_url:
                    add_log("ConnectorManager: No instance_url found in Salesforce connector")
                    return None
                
                return {
                    'access_token': access_token,
                    'instance_url': instance_url.rstrip('/')
                }
            
            # If no session (OAuth flow where connect() wasn't called), try to get from _credentials
            if hasattr(connector, '_credentials') and connector._credentials:
                access_token = connector._credentials.get('access_token')
                instance_url = connector._credentials.get('instance_url')
                
                if access_token and instance_url:
                    add_log("ConnectorManager: Extracted Salesforce credentials from connector._credentials (OAuth flow)")
                    return {
                        'access_token': access_token,
                        'instance_url': instance_url.rstrip('/')
                    }
                else:
                    add_log("ConnectorManager: Missing access_token or instance_url in connector._credentials")
                    return None
            
            add_log("ConnectorManager: No Connectzify session or credentials available")
            return None
            
        except Exception as e:
            add_log(f"ConnectorManager: Error extracting Salesforce credentials: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    async def _fetch_acumatica_data_direct(
        self, 
        tenant_url: str, 
        entity_endpoint: str, 
        access_token: str,
        api_version: str = "23.200.001"
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Fetch data from Acumatica API with direct HTTP call.
        Uses same entity configs as cloud function for exact compatibility.
        
        Args:
            tenant_url: Acumatica tenant URL
            entity_endpoint: Entity name (e.g., 'SalesOrder', 'StockItem')
            access_token: OAuth access token
            api_version: API version (default: 23.200.001)
            
        Returns:
            Tuple of (status_code, response_data)
        """
        try:
            # Use same entity configs as cloud function (hardcoded for exact compatibility)
            entity_configs = {
                'StockItem': {
                    'select_fields': [
                        "InventoryID",
                        "Description",
                        "ItemClass",
                        "BaseUOM",
                        "DefaultPrice",
                        "DefaultWarehouseID",
                        "IsAKit",
                        "ItemType",
                        "ItemStatus",
                        "WarehouseDetails/WarehouseID",
                        "WarehouseDetails/QtyOnHand",
                        "WarehouseDetails/ReplenishmentWarehouse"
                    ],
                    'expand': 'WarehouseDetails'
                },
                'ItemWarehouse': {
                    'select_fields': [
                        "InventoryID",
                        "WarehouseID",
                        "QtyOnHand",
                        "QtyAvailable",
                        "ReplenishmentSource"
                    ],
                    'expand': None
                },
                'InventoryReceipt': {
                    'select_fields': [
                        "ReferenceNbr",
                        "Date",
                        "Status",
                        "TotalQty",
                        "TotalCost"
                    ],
                    'expand': None
                },
                'SalesOrder': {
                    'select_fields': [
                        "OrderType",
                        "CustomerID",
                        "CustomerOrder",
                        "OrderNbr",
                        "LocationID",
                        "Status",
                        "Date",
                        "RequestedOn",
                        "CustomerOrder",
                        "Description",
                        "OrderedQty",
                        "OrderTotal",
                        "ShipVia",
                        "Details/Branch",
                        "Details/InventoryID",
                        "Details/FreeItem",
                        "Details/TaxZone",
                        "Details/UOM",
                        "Details/WarehouseID",
                        "Details/SalespersonID",
                        "Details/OrderQty",
                        "Details/UnitPrice",
                        "Details/ExtendedPrice",
                        "Details/DiscountPercent",
                        "Details/DiscountAmount",
                        "Details/Amount",
                        "Details/UnbilledAmount",
                        "Details/RequestedOn",
                        "Details/ShipOn",
                        "Details/Completed",
                        "Details/POSource",
                        "Details/TaxCategory",
                        "Details/LineNbr",
                        "Details/TaxZone",
                        "Details/UnitCost",
                        "Details/LineType",
                        "TaxDetails/TaxID",
                        "TaxDetails/TaxRate",
                        "TaxDetails/TaxableAmount",
                        "TaxDetails/TaxAmount",
                        "ShipToContact/BusinessName",
                        "ShipToContact/Attention",
                        "ShipToContact/Phone1",
                        "ShipToContact/Email",
                        "ShipToAddress/AddressLine1",
                        "ShipToAddress/AddressLine2",
                        "ShipToAddress/City",
                        "ShipToAddress/Country",
                        "ShipToAddress/State",
                        "ShipToAddress/PostalCode",
                        "BillToContact/Attention",
                        "BillToContact/Phone1",
                        "BillToContact/Email",
                        "BillToContact/BusinessName",
                        "BillToAddress/AddressLine1",
                        "BillToAddress/AddressLine2",
                        "BillToAddress/City",
                        "BillToAddress/Country",
                        "BillToAddress/State",
                        "BillToAddress/PostalCode",
                        "Shipments/ShipmentType",
                        "Shipments/ShipmentNbr",
                        "Shipments/Status",
                        "Shipments/ShipmentDate",
                        "Shipments/ShippedQty",
                        "Shipments/ShippedWeight",
                        "Shipments/InvoiceNbr",
                        "Shipments/InventoryDocType",
                        "Shipments/InventoryRefNbr"
                    ],
                    'expand': "Details,TaxDetails,ShipToContact,BillToContact,ShipToAddress,BillToAddress,Shipments,Payments"
                },
                'Shipment': {
                    'select_fields': [
                        "ShippingSettings/ShipToContact/Attention",
                        "ShippingSettings/ShipToContact/BusinessName",
                        "ShippingSettings/ShipToContact/Email",
                        "ShippingSettings/ShipToContact/Phone1",
                        "ShippingSettings/ShipToAddress/AddressLine1",
                        "ShippingSettings/ShipToAddress/AddressLine2",
                        "ShippingSettings/ShipToAddress/City",
                        "ShippingSettings/ShipToAddress/Country",
                        "ShippingSettings/ShipToAddress/State",
                        "ShippingSettings/ShipToAddress/PostalCode",
                        "Details/OrderType",
                        "Details/OrderNbr",
                        "Details/InventoryID",
                        "Details/LocationID",
                        "Details/UOM",
                        "Details/OrderedQty",
                        "Details/ShippedQty",
                        "Packages/BoxID",
                        "Packages/Confirmed",
                        "Packages/Type",
                        "Packages/Length",
                        "Packages/Width",
                        "Packages/Height",
                        "Packages/UOM",
                        "Packages/Weight",
                        "Packages/TrackingNbr",
                        "ShipmentNbr",
                        "Type",
                        "Status",
                        "Operation",
                        "ShipmentDate",
                        "Description",
                        "CustomerID",
                        "LocationID",
                        "WarehouseID",
                        "ShippedQty",
                        "ShippedWeight",
                        "ShippedVolume",
                        "PackageWeight",
                        "ShipVia"
                    ],
                    'expand': "Details,Packages,ShippingSettings/ShipToContact,ShippingSettings/ShipToAddress"
                },
                'SalesInvoice': {
                    'select_fields': [
                        "BillingSettings/BillToContact/BusinessName",
                        "BillingSettings/BillToContact/Attention",
                        "BillingSettings/BillToContact/Email",
                        "BillingSettings/BillToContact/Phone1",
                        "BillingSettings/BillToAddress/AddressLine1",
                        "BillingSettings/BillToAddress/AddressLine2",
                        "BillingSettings/BillToAddress/City",
                        "BillingSettings/BillToAddress/Country",
                        "BillingSettings/BillToAddress/State",
                        "BillingSettings/BillToAddress/PostalCode",
                        "Commissions/CommissionAmount",
                        "Commissions/TotalCommissionableAmount",
                        "ReferenceNbr",
                        "Status",
                        "Date",
                        "Type",
                        "Description",
                        "CustomerID",
                        "DetailTotal",
                        "TaxTotal",
                        "Amount",
                        "Balance",
                        "Details/Location",
                        "Details/BranchID",
                        "Details/ShipmentNbr",
                        "Details/OrderType",
                        "Details/OrderNbr",
                        "Details/InventoryID",
                        "Details/TransactionDescr",
                        "Details/WarehouseID",
                        "Details/UOM",
                        "Details/Qty",
                        "Details/UnitPrice",
                        "Details/DiscountAmount",
                        "Details/DiscountPercent",
                        "Details/Amount",
                        "DiscountDetails/DiscountCode",
                        "DiscountDetails/SequenceID",
                        "DiscountDetails/Type",
                        "DiscountDetails/DiscountableAmount",
                        "DiscountDetails/DiscountableQty",
                        "DiscountDetails/DiscountPercent",
                        "DiscountDetails/DiscountAmount"
                    ],
                    'expand': "Details,Commissions,DiscountDetails,BillingSettings/BillToContact,BillingSettings/BillToAddress"
                },
                'Invoice': {
                    'select_fields': [
                        "ReferenceNbr",
                        "LocationID",
                        "PostPeriod",
                        "Details/ExtendedPrice"
                    ],
                    'expand': "Details"
                }
            }
            
            # Get entity-specific config or use default
            config = entity_configs.get(entity_endpoint, {
                'select_fields': [],
                'expand': None
            })
            
            select_fields = config.get('select_fields', [])
            expand = config.get('expand')
            
            # Build API URL
            api_url = f'{tenant_url}/entity/Default/{api_version}/{entity_endpoint}'
            
            # Add query parameters
            query_params = []
            if select_fields:
                select_string = ",".join(select_fields)
                query_params.append(f"$select={select_string}")
            if expand:
                query_params.append(f"$expand={expand}")
            
            # Add query string if we have parameters
            if query_params:
                api_url += '?' + '&'.join(query_params)
            
            add_log(f"ConnectorManager: Fetching from Acumatica API: {api_url}")
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Make HTTP request (use asyncio.to_thread for sync requests)
            resp = await asyncio.to_thread(requests.get, api_url, headers=headers, timeout=(10, 3600))
            
            # Handle specific error status codes
            if resp.status_code == 401:
                error_detail = resp.text
                try:
                    error_json = resp.json()
                    error_detail = str(error_json)
                except Exception:
                    pass
                add_log(f"ConnectorManager: Acumatica API 401 Unauthorized. Response: {error_detail}")
                raise Exception(
                    f"Acumatica authentication failed (401). Token may be invalid or expired. "
                    f"Response: {error_detail}"
                )
            
            # Check for API Login Limit error (500 with HTML response)
            if resp.status_code == 500:
                response_text = resp.text
                if "API Login Limit" in response_text:
                    raise Exception(
                        "Acumatica API Login Limit reached. The instance has reached its maximum "
                        "number of concurrent API users. Please wait for other API sessions to close "
                        "or contact your Acumatica administrator to increase the API user limit."
                    )
                # For other 500 errors, provide more context
                add_log(f"ConnectorManager: Acumatica API 500 Server Error. Response: {response_text[:500]}")
                raise Exception(
                    f"Acumatica server error (500). This may be due to: "
                    f"1. API Login Limit reached, 2. Server configuration issue, or 3. Invalid request. "
                    f"Response: {response_text[:200]}"
                )
            
            data = {}
            try:
                data = resp.json()
            except Exception:
                data = {'raw': resp.text}
            
            add_log(f"ConnectorManager: Acumatica API Response Status: {resp.status_code}")
            if resp.status_code != 200:
                add_log(f"ConnectorManager: Acumatica API Error Response: {data}")
                raise Exception(f"Acumatica API returned status {resp.status_code}: {data}")
            
            return resp.status_code, data
            
        except requests.RequestException as e:
            add_log(f"ConnectorManager: Network error fetching Acumatica data: {str(e)}")
            return 500, {'error': f'Network error: {str(e)}'}
        except Exception as e:
            add_log(f"ConnectorManager: Error fetching Acumatica data: {str(e)} | traceback: {traceback.format_exc()}")
            return 500, {'error': f'Unexpected error: {str(e)}'}
    
    async def _create_salesforce_bulk_job(
        self, 
        soql_query: str, 
        access_token: str, 
        instance_url: str, 
        api_version: str = "v59.0"
    ) -> str:
        """Create Salesforce Bulk API query job and return job_id"""
        url = f"{instance_url}/services/data/{api_version}/jobs/query"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        payload = {
            "operation": "query",
            "query": soql_query,
        }
        
        resp = await asyncio.to_thread(requests.post, url, headers=headers, json=payload, timeout=30)
        
        if resp.status_code != 200:
            error_msg = f"Salesforce bulk job creation failed: {resp.status_code} - {resp.text}"
            add_log(f"ConnectorManager: {error_msg}")
            raise Exception(error_msg)
        
        job = resp.json()
        job_id = job['id']
        add_log(f"ConnectorManager: Created Salesforce bulk job: {job_id}")
        return job_id
    
    async def _wait_for_salesforce_job(
        self, 
        job_id: str, 
        access_token: str, 
        instance_url: str, 
        api_version: str = "v59.0"
    ) -> Dict[str, Any]:
        """Wait for Salesforce Bulk API job to complete"""
        url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        add_log(f"ConnectorManager: Waiting for Salesforce job {job_id} to complete...")
        
        while True:
            resp = await asyncio.to_thread(requests.get, url, headers=headers, timeout=30)
            
            if resp.status_code != 200:
                error_msg = f"Failed to check Salesforce job status: {resp.status_code} - {resp.text}"
                add_log(f"ConnectorManager: {error_msg}")
                raise Exception(error_msg)
            
            data = resp.json()
            job_state = data['state']
            
            add_log(f"ConnectorManager: Salesforce job status: {job_state}")
            
            if job_state == 'JobComplete':
                add_log("ConnectorManager: Salesforce job completed successfully")
                return data
            elif job_state in ['Failed', 'Aborted']:
                error_msg = f"Salesforce job {job_state}: {data.get('stateMessage', 'Unknown error')}"
                add_log(f"ConnectorManager: {error_msg}")
                raise Exception(error_msg)
            
            # Wait before next status check (5 seconds)
            await asyncio.sleep(5)
    
    async def _get_salesforce_job_results(
        self, 
        job_id: str, 
        access_token: str, 
        instance_url: str, 
        api_version: str = "v59.0"
    ) -> str:
        """Get CSV results from Salesforce Bulk API job"""
        url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}/results"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        add_log(f"ConnectorManager: Downloading Salesforce job results for {job_id}...")
        resp = await asyncio.to_thread(requests.get, url, headers=headers, timeout=60)
        
        if resp.status_code != 200:
            error_msg = f"Failed to retrieve Salesforce job results: {resp.status_code} - {resp.text}"
            add_log(f"ConnectorManager: {error_msg}")
            raise Exception(error_msg)
        
        csv_data = resp.text
        add_log(f"ConnectorManager: Downloaded Salesforce results ({len(csv_data)} characters)")
        return csv_data
    
    async def _fetch_salesforce_data_direct(
        self, 
        soql_query: str, 
        access_token: str, 
        instance_url: str,
        api_version: str = "v59.0"
    ) -> pd.DataFrame:
        """
        Fetch data from Salesforce using Bulk API.
        Similar to cloud function's fetch_salesforce_data but returns DataFrame.
        
        Args:
            soql_query: SOQL query string
            access_token: OAuth access token
            instance_url: Salesforce instance URL
            api_version: API version (default: v59.0)
            
        Returns:
            DataFrame with fetched data
        """
        try:
            add_log(f"ConnectorManager: Starting Salesforce data fetch...")
            add_log(f"ConnectorManager: Query: {soql_query[:100]}{'...' if len(soql_query) > 100 else ''}")
            
            # Step 1: Create bulk query job
            job_id = await self._create_salesforce_bulk_job(soql_query, access_token, instance_url, api_version)
            
            # Step 2: Wait for job completion
            await self._wait_for_salesforce_job(job_id, access_token, instance_url, api_version)
            
            # Step 3: Download results
            csv_data = await self._get_salesforce_job_results(job_id, access_token, instance_url, api_version)
            
            # Step 4: Convert CSV to DataFrame
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Step 5: Handle date field conversions
            if 'CreatedDate' in df.columns:
                df['CreatedDate'] = pd.to_datetime(df['CreatedDate'], errors='coerce').dt.date.astype('datetime64[ns]')
            if 'CloseDate' in df.columns:
                df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce').dt.date.astype('datetime64[ns]')
            
            add_log(f"ConnectorManager: Successfully fetched Salesforce DataFrame with {len(df)} records")
            return df
            
        except Exception as e:
            add_log(f"ConnectorManager: Salesforce data fetch failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _fetch_data(
        self,
        req: ConnectorActionRequest,
        milestone_callback: Optional[Callable[[str, Optional[Dict[str, Any]]], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch data from tables and save as .pkl files to GCS.
        
        Args:
            req: Action request with user_id, session_id, and params containing 'tables' list
            milestone_callback: Optional async callback(description, data) for progress milestones.
                data may include "dependency": "parallelizable" for optimization hints.
            
        Returns:
            Dict with saved files and preview data
        """
        try:
            # Check connector health
            healthy = await self.health(req.session_id)
            if not healthy:
                return {
                    "result": "fail",
                    "message": "Connector not active. Please connect again.",
                    "data": None
                }
            
            # Get connection_id from session's connection_result in Firebase
            connection_id = await self._get_connection_id_from_session(req.session_id)
            
            if not connection_id:
                add_log(f"ConnectorManager: No connection_id found in session {req.session_id}, proceeding without it")

            # Get connector first
            connector = self._get_connector(req.session_id)
            if not connector:
                return {
                    "result": "fail",
                    "message": "Connector not found for session",
                    "data": None
                }
            
            # Get tables/queries/URLs based on connector type and subject area
            tables = None
            if req.connector_type == 'salesforce' and req.subject_area:
                # For Salesforce: create SOQL queries for subject area
                # Returns dict: {object_name: soql_query}
                tables = create_salesforce_queries_for_subject_area(req.subject_area)
                add_log(f"ConnectorManager: Generated {len(tables)} SOQL queries for Salesforce subject_area '{req.subject_area}'")
            elif req.connector_type == 'acumatica' and req.subject_area:
                # For Acumatica: need tenant_url from credentials
                # For OAuth flows, get tenant_url from connector's stored credentials (already in memory)
                # For direct connections, fetch from credential service
                tenant_url = None
                
                # First, try to get tenant_url from connector's stored credentials (OAuth flow)
                if connector and hasattr(connector, '_credentials'):
                    connector_creds = connector._credentials
                    tenant_url = connector_creds.get("tenant_url") or connector_creds.get("instance_url")
                    if tenant_url:
                        add_log(f"ConnectorManager: Retrieved tenant_url from connector's stored credentials for OAuth flow")
                
                # If not found in connector, try to get from session's connection_result (OAuth flow)
                if not tenant_url:
                    try:
                        session_doc = await self._connector_cred_service._firestore_service._get_document(
                            self._sessions_collection,
                            req.session_id
                        )
                        if session_doc:
                            connection_result = session_doc.get('connection_result', {})
                            # Check if connection_result has credentials embedded
                            if isinstance(connection_result, dict):
                                # Try to extract from connection_result data
                                result_data = connection_result.get('data', {})
                                if isinstance(result_data, dict):
                                    tenant_url = result_data.get("tenant_url") or result_data.get("instance_url")
                                # Also check credentials field if present
                                if not tenant_url:
                                    creds = connection_result.get('credentials', {})
                                    tenant_url = creds.get("tenant_url") or creds.get("instance_url")
                        if tenant_url:
                            add_log(f"ConnectorManager: Retrieved tenant_url from session's connection_result")
                    except Exception as session_err:
                        add_log(f"ConnectorManager: Could not get tenant_url from session: {str(session_err)}")
                
                # If still not found, try to get from credential service (direct connection flow)
                # Only use this for non-OAuth flows - don't use Connectzify connection_id
                if not tenant_url:
                    # For OAuth flows, we should have tenant_url by now
                    # For direct connections, we need the credential's connection_id (not Connectzify connection_id)
                    # Try to get from session's credential_id if available
                    try:
                        session_doc = await self._connector_cred_service._firestore_service._get_document(
                            self._sessions_collection,
                            req.session_id
                        )
                        if session_doc:
                            credential_id = session_doc.get('credential_id')
                            if credential_id:
                                # Use credential_id (not Connectzify connection_id) to fetch credentials
                                connection_data = await self._connector_cred_service.get_credential_for_connector(
                                    user_id=req.user_id,
                                    connection_id=credential_id,  # Use credential_id, not Connectzify connection_id
                                    connector_type=req.connector_type
                                )
                                print(f"connection_data_manager_fetch_data: {connection_data}")
                                if connection_data:
                                    credentials = connection_data.get('data', {}).get('credentials', {})
                                    tenant_url = credentials.get("tenant_url") or credentials.get("instance_url") or credentials.get("site")
                                    if tenant_url:
                                        add_log(f"ConnectorManager: Retrieved tenant_url from credential service using credential_id")
                    except Exception as cred_err:
                        add_log(f"ConnectorManager: Could not get tenant_url from credential service: {str(cred_err)}")
                
                if not tenant_url:
                    return {
                        "result": "fail",
                        "message": "tenant_url or instance_url is required in credentials for Acumatica. Cannot generate Acumatica URLs.",
                        "data": None
                    }
                
                # Returns dict: {entity_name: relative_path}
                # Use return_relative_paths=True for Connectzify connector (it already has tenant_url)
                tables = create_acumatica_urls_for_subject_area(
                    tenant_url, 
                    req.subject_area,
                    return_relative_paths=True
                )
                add_log(f"ConnectorManager: Generated {len(tables)} relative paths for Acumatica subject_area '{req.subject_area}'")
                add_log(f"ConnectorManager: Tables dict keys: {list(tables.keys())}")
                add_log(f"ConnectorManager: Tables dict sample (first item): {list(tables.items())[0] if tables else 'empty'}")
            elif req.connector_type == 'mysql' and req.tables:
                tables = create_mysql_queries_for_tables(req.tables)
                add_log(f"ConnectorManager: Generated {len(tables)} MySQL queries for tables '{req.tables}'")
            elif req.connector_type == 'mssql' and req.tables:
                tables = create_mssql_queries_for_tables(req.tables)
                add_log(f"ConnectorManager: Generated {len(tables)} MSSQL queries for tables '{req.tables}'")
            else:
                # For other connectors or when subject_area not provided, use tables from request
                tables_list = req.tables if req.tables else ((req.params or {}).get('tables', []))
                if not tables_list:
                    return {
                        "result": "fail",
                        "message": "No tables specified. Provide 'tables' in request body or in params, or provide 'subject_area' for Salesforce/Acumatica.",
                        "data": None
                    }
                # Convert list to dict for consistent handling
                tables = {table: table for table in tables_list}
            # if not connector:
            #     return {
            #         "result": "fail",
            #         "message": "Connector not found for session",
            #         "data": None

            #     }
            
            # Get tables from params
            # params = req.params or {}
            # tables = params.get('tables', [])
            
            # if not tables:
            #     return {
            #         "result": "fail",
            #         "message": "No tables specified. Provide 'tables' in params.",
            #         "data": None
            #     }
            
            # Get GCS configuration
            try:
                gcp_manager = GcpManager._get_instance()
                storage_service = gcp_manager._storage_service
                constants = init_env()
                bucket_name = constants.get('storage_bucket')
                
                if not bucket_name:
                    raise ValueError("storage_bucket not configured in constants")
            except Exception as gcp_err:
                add_log(f"ConnectorManager: GCS not available, falling back to local storage: {str(gcp_err)}")
                # Fallback to local storage if GCS not configured
                # Convert dict to list of table names for local fetch
                tables_list = list(tables.keys()) if isinstance(tables, dict) else tables
                return await self._fetch_data_local(req, connector, tables_list, milestone_callback)
            
            saved_files = []
            preview_map = {}
            gcs_paths = []
            
            # Log how many items we're processing
            add_log(f"ConnectorManager: Processing {len(tables)} tables/queries for session {req.session_id}")
            await self._session_manager._update_session_status(req.session_id, "in_progress")
            add_log(f"ConnectorManager: Updated session {req.session_id} status to 'in_progress'")
            
            # Entity-to-processing-function mapping for Acumatica
            acumatica_processing_map = {
                'SalesOrder': process_sales_order_data,
                'SalesInvoice': process_sales_invoice_data,
                'Shipment': process_shipment_data,
                'Invoice': process_invoice_data,
                # 'ItemWarehouse': process_item_warehouse_data,  # Removed - StockItem already provides warehouse.pkl
                'InventoryReceipt': process_inventory_receipt_data,
                'StockItem': process_stock_item_data  # Returns tuple: (stock_df, warehouse_df)
            }
            
            # Handle Salesforce and Acumatica with direct API calls
            access_token = None
            tenant_url = None
            instance_url = None
            
            if req.connector_type in ['salesforce', 'acumatica']:
                # Extract credentials from Connectzify session
                if req.connector_type == 'acumatica':
                    creds = self._get_acumatica_credentials_from_connector(connector)
                    if not creds:
                        return {
                            "result": "fail",
                            "message": "Failed to extract Acumatica credentials from connector. Please reconnect.",
                            "data": None
                        }
                    access_token = creds['access_token']
                    tenant_url = creds['tenant_url']
                elif req.connector_type == 'salesforce':
                    creds = self._get_salesforce_credentials_from_connector(connector)
                    if not creds:
                        return {
                            "result": "fail",
                            "message": "Failed to extract Salesforce credentials from connector. Please reconnect.",
                            "data": None
                        }
                    access_token = creds['access_token']
                    instance_url = creds['instance_url']
            
            # Iterate over tables dict: key is object/entity name, value is query/URL
            # For Salesforce/Acumatica: key = object name (e.g., 'Account'), value = SOQL query/API URL
            # For others: key == value (table name)
            sales_invoice_df_temp = None  # For merging SalesInvoice with Invoice
            
            for idx, (object_name, query_or_url) in enumerate(tables.items(), 1):
                try:
                    if milestone_callback:
                        await milestone_callback(
                            f"Data fetch: Fetching {object_name} ({idx}/{len(tables)})",
                            {"table": object_name, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                        )
                    add_log(f"ConnectorManager: Processing {idx}/{len(tables)}: {object_name}")
                    
                    df = None
                    
                    # Route Salesforce/Acumatica to direct API calls
                    if req.connector_type == 'acumatica':
                        # Extract entity name from relative path (e.g., "/entity/Default/23.200.001/SalesOrder?...")
                        # Pattern: /entity/Default/{api_version}/{entity_name}?...
                        entity_name = None
                        
                        if '/' in query_or_url:
                            # Extract entity name from path
                            # Path structure: /entity/Default/{api_version}/{entity_name}?...
                            parts = [p for p in query_or_url.split('/') if p]  # Remove empty strings
                            
                            # Find 'Default' and get the part after the API version
                            if 'Default' in parts:
                                default_idx = parts.index('Default')
                                # The entity name is 2 positions after 'Default' (Default -> api_version -> entity_name)
                                if default_idx + 2 < len(parts):
                                    entity_name = parts[default_idx + 2].split('?')[0].split('&')[0]
                        
                        # Fallback to object_name if extraction failed or if URL doesn't contain '/'
                        if not entity_name:
                            # Use object_name if it doesn't look like an API version (contains dots)
                            if object_name and '.' not in object_name:
                                entity_name = object_name
                            else:
                                # Last resort: use object_name even if it might be wrong
                                entity_name = object_name
                                add_log(f"ConnectorManager: Warning - Could not extract entity name from URL '{query_or_url[:100]}', using object_name '{object_name}'")
                        
                        add_log(f"ConnectorManager: Fetching Acumatica entity '{entity_name}' via direct API call (object_name='{object_name}', query_or_url='{query_or_url[:100]}...')")
                        
                        # Fetch raw JSON from Acumatica API
                        # _fetch_acumatica_data_direct raises exceptions on errors, returns (200, data) on success
                        try:
                            _, raw_data = await self._fetch_acumatica_data_direct(
                                tenant_url=tenant_url,
                                entity_endpoint=entity_name,
                                access_token=access_token
                            )
                        except Exception as api_err:
                            add_log(f"ConnectorManager: Acumatica API error for {entity_name}: {str(api_err)}")
                            raise Exception(f"Acumatica API error for {entity_name}: {str(api_err)}")
                        
                        # If we get here, status_code is 200 (exceptions are raised on errors)
                        # Handle Acumatica API response format (usually has 'value' key)
                        fetch_result = raw_data.get('value', raw_data) if isinstance(raw_data, dict) else raw_data
                        
                        # Debug: Log raw fetch result structure (matching cloud function)
                        add_log(f"ConnectorManager: DEBUG: Fetch result type: {type(fetch_result)}")
                        if isinstance(fetch_result, dict):
                            if 'value' in fetch_result:
                                add_log(f"ConnectorManager: DEBUG: Found 'value' key with {len(fetch_result.get('value', []))} items")
                            else:
                                add_log(f"ConnectorManager: DEBUG: No 'value' key found. Keys are: {list(fetch_result.keys())}")
                        add_log(f"ConnectorManager: DEBUG: Raw fetch_result (first 500 chars): {str(fetch_result)[:500]}")
                        
                        # Apply entity-specific processing function (matching cloud function exactly)
                        processing_func = acumatica_processing_map.get(entity_name)
                        
                        if entity_name == 'SalesOrder':
                            # Process SalesOrder data (returns single merged DataFrame with all data)
                            # MATCHING CLOUD FUNCTION EXACTLY
                            sales_order_df = process_sales_order_data(fetch_result) if processing_func else None
                            add_log(f"ConnectorManager: DEBUG: SalesOrder DF shape: {sales_order_df.shape if sales_order_df is not None else 'None'}")
                            if sales_order_df is not None and not sales_order_df.empty:
                                add_log(f"ConnectorManager: DEBUG: SalesOrder DF head:\n{sales_order_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: SalesOrder DF is empty or None")
                            
                            # Check if dataframe is None (matching cloud function)
                            if sales_order_df is None:
                                add_log(f"ConnectorManager: Error - Processed SalesOrder dataframe is None, skipping")
                                continue
                            
                            # Check if dataframe is empty (matching cloud function)
                            if sales_order_df.empty:
                                add_log(f"ConnectorManager: WARNING: SalesOrder dataframe is empty for entity {entity_name}")
                                add_log(f"ConnectorManager: WARNING: This usually means the API returned no data or data in unexpected format")
                                continue
                            
                            # Save dataframe (matching cloud function filename)
                            pkl_filename = "sales_order.pkl"
                            gcs_path = await storage_service.upload_pkl_to_session(
                                data=sales_order_df,
                                user_id=req.user_id,
                                session_id=req.session_id,
                                filename=pkl_filename
                            )
                            
                            if gcs_path:
                                saved_files.append(pkl_filename)
                                gcs_paths.append(gcs_path)
                                preview_map[pkl_filename] = generate_preview_data(sales_order_df)
                                add_log(f"ConnectorManager: Successfully uploaded sales_order ({len(sales_order_df)} rows) to {gcs_path}")
                                if milestone_callback:
                                    await milestone_callback(
                                        f"Data fetch: Saved {pkl_filename} ({idx}/{len(tables)})",
                                        {"file": pkl_filename, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                    )
                            
                            continue
                        
                        elif entity_name == 'Shipment':
                            # Process Shipment data (simple entity without expansion)
                            # MATCHING CLOUD FUNCTION EXACTLY
                            shipment_df = process_shipment_data(fetch_result) if processing_func else None
                            add_log(f"ConnectorManager: DEBUG: Shipment DF shape: {shipment_df.shape if shipment_df is not None else 'None'}")
                            if shipment_df is not None and not shipment_df.empty:
                                add_log(f"ConnectorManager: DEBUG: Shipment DF head:\n{shipment_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: Shipment DF is empty or None")
                            
                            # Check if dataframe is None or empty (matching cloud function)
                            if shipment_df is None or shipment_df.empty:
                                add_log(f"ConnectorManager: WARNING: Shipment dataframe is empty for entity {entity_name}")
                                continue
                            
                            # Save dataframe (matching cloud function filename)
                            pkl_filename = "shipment.pkl"
                            gcs_path = await storage_service.upload_pkl_to_session(
                                data=shipment_df,
                                user_id=req.user_id,
                                session_id=req.session_id,
                                filename=pkl_filename
                            )
                            
                            if gcs_path:
                                saved_files.append(pkl_filename)
                                gcs_paths.append(gcs_path)
                                preview_map[pkl_filename] = generate_preview_data(shipment_df)
                                add_log(f"ConnectorManager: Successfully uploaded shipment ({len(shipment_df)} rows) to {gcs_path}")
                                if milestone_callback:
                                    await milestone_callback(
                                        f"Data fetch: Saved {pkl_filename} ({idx}/{len(tables)})",
                                        {"file": pkl_filename, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                    )
                            
                            continue
                        
                        elif entity_name == 'SalesInvoice':
                            # Process SalesInvoice data (returns single merged DataFrame with all data)
                            # MATCHING CLOUD FUNCTION EXACTLY
                            sales_invoice_df = process_sales_invoice_data(fetch_result) if processing_func else None
                            add_log(f"ConnectorManager: DEBUG: SalesInvoice DF shape: {sales_invoice_df.shape if sales_invoice_df is not None else 'None'}")
                            if sales_invoice_df is not None and not sales_invoice_df.empty:
                                add_log(f"ConnectorManager: DEBUG: SalesInvoice DF head:\n{sales_invoice_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: SalesInvoice DF is empty or None")
                            
                            # Check if dataframe is None (matching cloud function)
                            if sales_invoice_df is None:
                                add_log(f"ConnectorManager: Error - Processed SalesInvoice dataframe is None, skipping")
                                continue
                            
                            # Check if dataframe is empty (matching cloud function)
                            if sales_invoice_df.empty:
                                add_log(f"ConnectorManager: WARNING: SalesInvoice dataframe is empty for entity {entity_name}")
                                add_log(f"ConnectorManager: WARNING: This usually means the API returned no data or data in unexpected format")
                                continue
                            
                            # Store SalesInvoice dataframe temporarily for merging with Invoice later
                            # Don't save it yet - will merge with Invoice and save together (matching cloud function)
                            sales_invoice_df_temp = sales_invoice_df.copy()
                            add_log("ConnectorManager: Stored SalesInvoice dataframe for merging with Invoice")
                            continue
                        
                        elif entity_name == 'Invoice':
                            # Process Invoice data (returns single merged DataFrame with Details expansion)
                            # MATCHING CLOUD FUNCTION EXACTLY
                            invoice_df = process_invoice_data(fetch_result) if processing_func else None
                            add_log(f"ConnectorManager: DEBUG: Invoice DF shape: {invoice_df.shape if invoice_df is not None else 'None'}")
                            if invoice_df is not None and not invoice_df.empty:
                                add_log(f"ConnectorManager: DEBUG: Invoice DF head:\n{invoice_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: Invoice DF is empty or None")
                            
                            # Check if dataframe is None (matching cloud function)
                            if invoice_df is None:
                                add_log(f"ConnectorManager: Error - Processed Invoice dataframe is None, skipping")
                                continue
                            
                            # Check if dataframe is empty (matching cloud function)
                            if invoice_df.empty:
                                add_log(f"ConnectorManager: WARNING: Invoice dataframe is empty for entity {entity_name}")
                                continue
                            
                            # Merge SalesInvoice and Invoice dataframes on ReferenceNbr if SalesInvoice exists (matching cloud function exactly)
                            merged_invoice_df = invoice_df.copy()
                            if sales_invoice_df_temp is not None and not sales_invoice_df_temp.empty:
                                add_log("ConnectorManager: Merging SalesInvoice and Invoice dataframes on ReferenceNbr (row-by-row)")
                                # Add row numbers per ReferenceNbr for row-by-row matching
                                sales_invoice_df_temp_copy = sales_invoice_df_temp.copy()
                                invoice_df_temp = invoice_df.copy()
                                
                                sales_invoice_df_temp_copy['row_num'] = sales_invoice_df_temp_copy.groupby('ReferenceNbr').cumcount()
                                invoice_df_temp['row_num'] = invoice_df_temp.groupby('ReferenceNbr').cumcount()
                                
                                # Merge Invoice data into SalesInvoice (left join to keep all SalesInvoice records)
                                # Merge on both ReferenceNbr and row_num for row-by-row matching
                                merged_invoice_df = sales_invoice_df_temp_copy.merge(
                                    invoice_df_temp, 
                                    on=['ReferenceNbr', 'row_num'], 
                                    how='left', 
                                    suffixes=('', '_Invoice')
                                )
                                
                                # Drop helper column
                                merged_invoice_df.drop(columns=['row_num'], inplace=True)
                                
                                add_log(f"ConnectorManager: DEBUG: Merged Invoice DF shape: {merged_invoice_df.shape}")
                                add_log(f"ConnectorManager: DEBUG: Merged Invoice DF columns: {list(merged_invoice_df.columns)}")
                            else:
                                add_log("ConnectorManager: WARNING: SalesInvoice dataframe not available for merging, using Invoice dataframe only")
                                merged_invoice_df = invoice_df.copy()
                            
                            # Save merged SalesInvoice dataframe (matching cloud function filename)
                            pkl_filename = "sales_invoice.pkl"
                            gcs_path = await storage_service.upload_pkl_to_session(
                                data=merged_invoice_df,
                                user_id=req.user_id,
                                session_id=req.session_id,
                                filename=pkl_filename
                            )
                            
                            if gcs_path:
                                saved_files.append(pkl_filename)
                                gcs_paths.append(gcs_path)
                                preview_map[pkl_filename] = generate_preview_data(merged_invoice_df)
                                add_log(f"ConnectorManager: Successfully uploaded sales_invoice ({len(merged_invoice_df)} rows) to {gcs_path}")
                                if milestone_callback:
                                    await milestone_callback(
                                        f"Data fetch: Saved {pkl_filename} ({idx}/{len(tables)})",
                                        {"file": pkl_filename, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                    )
                            
                            continue
                        
                        # ItemWarehouse removed - StockItem already provides warehouse.pkl with warehouse data
                        # No need for separate ItemWarehouse entity processing
                        
                        elif entity_name == 'InventoryReceipt':
                            # Process InventoryReceipt data (simple entity, no expansions)
                            # MATCHING CLOUD FUNCTION EXACTLY
                            inventory_receipt_df = process_inventory_receipt_data(fetch_result) if processing_func else None
                            add_log(f"ConnectorManager: DEBUG: InventoryReceipt DF shape: {inventory_receipt_df.shape if inventory_receipt_df is not None else 'None'}")
                            if inventory_receipt_df is not None and not inventory_receipt_df.empty:
                                add_log(f"ConnectorManager: DEBUG: InventoryReceipt DF head:\n{inventory_receipt_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: InventoryReceipt DF is empty or None")
                            
                            # Check if dataframe is None or empty (matching cloud function)
                            if inventory_receipt_df is None or inventory_receipt_df.empty:
                                add_log(f"ConnectorManager: WARNING: InventoryReceipt dataframe is empty for entity {entity_name}")
                                continue
                            
                            # Save dataframe (matching cloud function filename)
                            pkl_filename = "inventory_receipt.pkl"
                            gcs_path = await storage_service.upload_pkl_to_session(
                                data=inventory_receipt_df,
                                user_id=req.user_id,
                                session_id=req.session_id,
                                filename=pkl_filename
                            )
                            
                            if gcs_path:
                                saved_files.append(pkl_filename)
                                gcs_paths.append(gcs_path)
                                preview_map[pkl_filename] = generate_preview_data(inventory_receipt_df)
                                add_log(f"ConnectorManager: Successfully uploaded inventory_receipt ({len(inventory_receipt_df)} rows) to {gcs_path}")
                                if milestone_callback:
                                    await milestone_callback(
                                        f"Data fetch: Saved {pkl_filename} ({idx}/{len(tables)})",
                                        {"file": pkl_filename, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                    )
                            
                            continue
                        
                        elif entity_name == 'StockItem':
                            # Handle other entities (StockItem, etc.) with existing logic
                            # MATCHING CLOUD FUNCTION EXACTLY - uses processed_data function
                            from v2.modules.connector_framework.services.data_processing.acumatica.common_utils import _to_list_of_dicts, flatten_stock_items_data
                            import pandas as pd
                            
                            # Use processed_data equivalent (process_stock_item_data)
                            stock_df, warehouse_df = process_stock_item_data(fetch_result) if processing_func else (None, None)
                            add_log(f"ConnectorManager: DEBUG: Stock DF shape: {stock_df.shape if stock_df is not None else 'None'}")
                            add_log(f"ConnectorManager: DEBUG: Warehouse DF shape: {warehouse_df.shape if warehouse_df is not None else 'None'}")
                            if stock_df is not None and not stock_df.empty:
                                add_log(f"ConnectorManager: DEBUG: Stock DF head:\n{stock_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: Stock DF is empty or None")
                            if warehouse_df is not None and not warehouse_df.empty:
                                add_log(f"ConnectorManager: DEBUG: Warehouse DF head:\n{warehouse_df.head()}")
                            else:
                                add_log("ConnectorManager: DEBUG: Warehouse DF is empty or None")
                            
                            # Check if dataframes are None (matching cloud function)
                            if stock_df is None or warehouse_df is None:
                                add_log(f"ConnectorManager: Error - Processed dataframes are None, skipping")
                                continue
                            
                            # Check if both dataframes are empty (matching cloud function)
                            if stock_df.empty and warehouse_df.empty:
                                add_log(f"ConnectorManager: WARNING: Both dataframes are empty for entity {entity_name}")
                                add_log(f"ConnectorManager: WARNING: This usually means the API returned no data or data in unexpected format")
                                continue
                            
                            # Save dataframes to storage (matching cloud function - saves even if empty with status "empty")
                            pkl_filename_stock = "stock_items.pkl"
                            if not stock_df.empty:
                                gcs_path_stock = await storage_service.upload_pkl_to_session(
                                    data=stock_df,
                                    user_id=req.user_id,
                                    session_id=req.session_id,
                                    filename=pkl_filename_stock
                                )
                                if gcs_path_stock:
                                    saved_files.append(pkl_filename_stock)
                                    gcs_paths.append(gcs_path_stock)
                                    preview_map[pkl_filename_stock] = generate_preview_data(stock_df)
                                    add_log(f"ConnectorManager: Successfully uploaded stock_items ({len(stock_df)} rows) to {gcs_path_stock}")
                                    if milestone_callback:
                                        await milestone_callback(
                                            f"Data fetch: Saved {pkl_filename_stock} ({idx}/{len(tables)})",
                                            {"file": pkl_filename_stock, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                        )
                            
                            pkl_filename_wh = "warehouse.pkl"
                            if not warehouse_df.empty:
                                gcs_path_wh = await storage_service.upload_pkl_to_session(
                                    data=warehouse_df,
                                    user_id=req.user_id,
                                    session_id=req.session_id,
                                    filename=pkl_filename_wh
                                )
                                if gcs_path_wh:
                                    saved_files.append(pkl_filename_wh)
                                    gcs_paths.append(gcs_path_wh)
                                    preview_map[pkl_filename_wh] = generate_preview_data(warehouse_df)
                                    add_log(f"ConnectorManager: Successfully uploaded warehouse ({len(warehouse_df)} rows) to {gcs_path_wh}")
                                    if milestone_callback:
                                        await milestone_callback(
                                            f"Data fetch: Saved {pkl_filename_wh} ({idx}/{len(tables)})",
                                            {"file": pkl_filename_wh, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                        )
                            
                            continue
                        
                        else:
                            # Unknown entity - log warning and skip
                            add_log(f"ConnectorManager: WARNING: Unknown entity '{entity_name}', skipping")
                            continue
                    
                    elif req.connector_type == 'salesforce':
                        # Salesforce: query_or_url is SOQL query
                        add_log(f"ConnectorManager: Fetching Salesforce object '{object_name}' via Bulk API")
                        
                        df = await self._fetch_salesforce_data_direct(
                            soql_query=query_or_url,
                            access_token=access_token,
                            instance_url=instance_url
                        )
                        
                        if df is None or df.empty:
                            add_log(f"ConnectorManager: Warning - No data returned for {object_name}, skipping")
                            continue
                    
                    else:
                        # Other connectors: use existing flow
                        get_data_kwargs = {}
                        if req.subject_area:
                            get_data_kwargs["subject_area"] = req.subject_area
                        if connection_id:
                            get_data_kwargs["connection_id"] = connection_id
                        
                        add_log(f"ConnectorManager: Fetching data for {object_name} using connector.get_data()")
                        df = await connector.get_data(query_or_url, **get_data_kwargs)
                        if df is None or df.empty:
                            add_log(f"ConnectorManager: Warning - No data returned for {object_name}, skipping")
                            continue
                    
                    # Save DataFrame (for non-special cases)
                    if df is not None and not df.empty:
                        add_log(f"ConnectorManager: Fetched {len(df)} rows for {object_name}")
                        
                        # Use object_name (key) as the filename, convert to lowercase with underscores
                        pkl_filename = f"{object_name.lower().replace(' ', '_')}.pkl"
                        
                        # Use utility method for upload
                        gcs_path = await storage_service.upload_pkl_to_session(
                            data=df,
                            user_id=req.user_id,
                            session_id=req.session_id,
                            filename=pkl_filename
                        )
                        
                        if gcs_path:
                            saved_files.append(pkl_filename)
                            gcs_paths.append(gcs_path)
                            # Generate preview
                            preview_map[pkl_filename] = connector.get_preview(df)
                            if milestone_callback:
                                await milestone_callback(
                                    f"Data fetch: Saved {pkl_filename} ({idx}/{len(tables)})",
                                    {"file": pkl_filename, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                                )
                            add_log(f"ConnectorManager: Successfully uploaded {len(df)} rows for {object_name} to {gcs_path}")
                
                except Exception as table_err:
                    add_log(f"ConnectorManager: Error fetching {object_name}: {str(table_err)} | traceback: {traceback.format_exc()}")
                    # Continue to next table instead of failing completely
                    continue
            
            if not saved_files:
                return {
                    "result": "fail",
                    "message": "Failed to fetch any tables",
                    "data": None
                }
            
            add_log(f"ConnectorManager: Saved {len(saved_files)} files to GCS for session {req.session_id}")
            # Note: Status updates are handled by JobManager when job completes
            # Do not update status here to avoid overriding JobManager's step-based status updates

            return {
                "result": "success",
                "message": f"Data fetched and saved to GCS: files",
                "data": {
                    "session_id": req.session_id,
                    "user_id": req.user_id,
                    "saved_files": saved_files,
                    "gcs_paths": gcs_paths,
                    "preview_data": preview_map
                }
            }
        except ConnectorError as e:
            add_log(f"ConnectorManager._fetch_data error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": str(e),
                "data": None
            }
        except Exception as e:
            add_log(f"ConnectorManager._fetch_data error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": f"Failed to fetch data: {str(e)}",
                "data": None
            }

    async def _fetch_data_local(
        self,
        req: ConnectorActionRequest,
        connector: BaseConnector,
        tables: List[str],
        milestone_callback: Optional[Callable[[str, Optional[Dict[str, Any]]], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        """
        Fallback: Fetch data and save as .pkl files to local filesystem.
        Used when GCS is not available.
        
        Args:
            req: Action request
            connector: Active connector
            tables: List of table names to fetch
            milestone_callback: Optional async callback(description, data) for progress milestones.
            
        Returns:
            Dict with saved files and preview data
        """
        # Prepare local output directory
        project_root = self._get_project_root()
        input_data_dir = project_root / "execution_layer" / "input_data" / req.session_id
        
        # Clear existing data
        if input_data_dir.exists():
            for f in input_data_dir.iterdir():
                if f.is_file():
                    f.unlink()
        
        os.makedirs(input_data_dir, exist_ok=True)
        
        saved_files = []
        preview_map = {}
        
        for idx, table_name in enumerate(tables, 1):
            try:
                if milestone_callback:
                    await milestone_callback(
                        f"Data fetch: Fetching {table_name} ({idx}/{len(tables)})",
                        {"table": table_name, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                    )
                # Fetch data from connector
                df = await connector.get_data(table_name)
                
                # Save as pickle locally
                pkl_filename = f"{table_name}.pkl"
                pkl_path = input_data_dir / pkl_filename
                df.to_pickle(str(pkl_path))
                saved_files.append(pkl_filename)
                
                # Generate preview
                preview_map[pkl_filename] = connector.get_preview(df)
                
                add_log(f"ConnectorManager: Saved {len(df)} rows to local {pkl_filename}")
                if milestone_callback:
                    await milestone_callback(
                        f"Data fetch: Saved {pkl_filename} ({idx}/{len(tables)})",
                        {"file": pkl_filename, "index": idx, "total": len(tables), "dependency": "parallelizable", "is_llm_call": False},
                    )
            except Exception as table_err:
                add_log(f"ConnectorManager: Error fetching {table_name}: {str(table_err)}")
                continue
        
        if not saved_files:
            return {
                "result": "fail",
                "message": "Failed to fetch any tables",
                "data": None
            }
        
        add_log(f"ConnectorManager: Saved {len(saved_files)} files locally for session {req.session_id}")
        
        return {
            "result": "success",
            "message": f"Data fetched and saved locally: {len(saved_files)} files (GCS not available)",
            "data": {
                "session_id": req.session_id,
                "saved_files": saved_files,
                "local_path": str(input_data_dir),
                "preview_data": preview_map
            }
        }

    async def _copy_sample_data(self, req: ConnectorActionRequest) -> Dict[str, Any]:
        """
        Special method for sample data: copy sample files to session folder.
        
        Args:
            req: Action request with session_id and params containing 'filename'
            
        Returns:
            Dict with copied file info
        """
        try:
            connector = self._get_connector(req.session_id)
            if not connector:
                return {
                    "result": "fail",
                    "message": "Sample data connector not active. Please connect first.",
                    "data": None
                }
            
            params = req.params or {}
            filename = params.get('filename', '')
            
            if not filename:
                return {
                    "result": "fail",
                    "message": "No filename specified. Provide 'filename' in params.",
                    "data": None
                }
            
            # Check if this is a SampleDataConnector
            if not hasattr(connector, 'copy_to_session'):
                return {
                    "result": "fail",
                    "message": "Current connector does not support sample data operations",
                    "data": None
                }
            
            # Copy sample data to session (with user_id for GCS upload)
            result = await connector.copy_to_session(
                filename=filename,
                session_id=req.session_id,
                user_id=req.user_id,
                clear_existing=True
            )

            return {
                "result": "success",
                "message": "Sample data copied successfully",
                "data": result
            }
        except Exception as e:
            add_log(f"ConnectorManager._copy_sample_data error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": f"Failed to copy sample data: {str(e)}",
                "data": None
            }

    async def _fetch_n8n_data(self, req: ConnectorActionRequest) -> Dict[str, Any]:
        """
        Special method for n8n connector: fetch CSV data from URL and save to session folder.
        
        Args:
            req: Action request with session_id and params containing 'url'
            
        Returns:
            Dict with fetched file info
        """
        try:
            connector = self._get_connector(req.session_id)
            if not connector:
                return {
                    "result": "fail",
                    "message": "N8N connector not active. Please connect first.",
                    "data": None
                }
            
            params = req.params or {}
            url = params.get('url', '')
            
            if not url:
                return {
                    "result": "fail",
                    "message": "No URL specified. Provide 'url' in params.",
                    "data": None
                }
            
            # Check if this is an N8nConnector
            if not hasattr(connector, 'fetch_from_url'):
                return {
                    "result": "fail",
                    "message": "Current connector does not support n8n data fetching operations",
                    "data": None
                }
            
            # Fetch n8n data from URL (with user_id for GCS upload)
            # Ensure IDs are strings (defensive check)
            user_id_str = str(req.user_id) if req.user_id else None
            session_id_str = str(req.session_id) if req.session_id else None
            
            result = await connector.fetch_from_url(
                url=url,
                session_id=session_id_str,
                user_id=user_id_str,
                clear_existing=True
            )

            return {
                "result": "success",
                "message": "N8N data fetched successfully",
                "data": result
            }
        except Exception as e:
            add_log(f"ConnectorManager._fetch_n8n_data error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": f"Failed to fetch n8n data: {str(e)}",
                "data": None
            }

    async def _disconnect(self, session_id: str) -> bool:
        """
        Disconnect and cleanup connector for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            bool: True if disconnected successfully
        """
        async with self._lock:
            try:
                connector = self._get_connector(session_id)
                if connector:
                    await connector.disconnect()
                
                connector_key = self._get_connector_key(session_id)
                if connector_key in self._connectors:
                    del self._connectors[connector_key]
                if session_id in self._types:
                    del self._types[session_id]
                
                add_log(f"ConnectorManager: Disconnected connector for session {session_id}")
                return True
            except Exception as e:
                add_log(f"ConnectorManager._disconnect error: {str(e)}")
                return False

    async def _upload_file(self, file, user_id: str, session_id: str) -> Dict[str, Any]:
        """
        Upload and process a CSV/Excel file.
        
        Args:
            file: UploadFile from FastAPI
            user_id: User identifier
            session_id: Session identifier
            
        Returns:
            Dict with upload result and preview
        """
        try:
            add_log(f"ConnectorManager._upload_file: user={user_id}, session={session_id}, filename={file.filename}")
            
            # Read file content
            file_content = await file.read()
            filename = file.filename or "uploaded_file"
            
            # Create FileUploadConnector
            connector = FileUploadConnector({'session_id': session_id})
            await connector.connect()
            
            # Process the file (validate and parse)
            process_result = await connector.process_file(
                file_content=file_content,
                filename=filename,
                session_id=session_id
            )
            
            if not process_result.get('valid'):
                return {
                    "result": "fail",
                    "message": process_result.get('errors', ['Validation failed'])[0] if process_result.get('errors') else 'Validation failed',
                    "data": None
                }
            
            # Save to session folder (GCS or local)
            save_result = await connector.save_to_session(
                session_id=session_id,
                user_id=user_id,
                clear_existing=True
            )
            
            # Store connector in registry
            connector_key = self._get_connector_key(session_id)
            self._connectors[connector_key] = connector
            self._types[session_id] = "file_upload"
            
            add_log(f"ConnectorManager: Uploaded and saved {filename} for session {session_id}")
            
            return {
                "result": "success",
                "message": "File uploaded and validated successfully",
                "data": {
                    "filename": filename,
                    "saved_file": save_result.get('saved_file'),
                    "rows": save_result.get('rows'),
                    "columns": save_result.get('columns'),
                    "preview_data": save_result.get('preview_data'),
                    "validation_warnings": process_result.get('warnings', [])
                }
            }
        except Exception as e:
            add_log(f"ConnectorManager._upload_file error: {str(e)} | traceback: {traceback.format_exc()}")
            return {
                "result": "fail",
                "message": str(e),
                "data": None
            }
