from flask import Blueprint, request, jsonify, current_app, g
import os
import json
import re
import requests
import urllib.parse
import uuid
from typing import Dict, Any, List
from datetime import datetime
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound, AlreadyExists, PermissionDenied
from google.oauth2 import service_account
from google.cloud import storage as gcs
from logger import add_log
from utils.env import init_env, KEY_FILE_PATH, GCP_PROJECT
from .data import _clear_session_input_data
from api_layer.firebase_data_models import get_data_manager, CredentialDocument
from api_layer.credentials import handle_encryption_error

acumatica_bp = Blueprint('acumatica_bp', __name__)

constants = init_env()

def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'Acumatica operation failed'}), status

@acumatica_bp.route('/acumatica/subject_areas', methods=['GET'])
def get_acumatica_subject_areas():
    """Get available Acumatica subject areas."""
    try:
        return jsonify({
            'status': 'success',
            'status_code': 200,
            'subject_areas': constants.get('acumatica_subject_areas')
        }), 200
    except Exception as e:
        add_log(f"Acumatica get_acumatica_subject_areas error: {str(e)}")
        return fail(f'Failed to get subject areas: {str(e)}', 500)


def _validate_non_empty_string(value: Any, field: str, min_len: int = 8) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field} is required")
    if len(trimmed) < min_len:
        raise ValueError(f"{field} must be at least {min_len} characters")
    return trimmed


def _camel_to_snake(name: str) -> str:
    """
    Convert CamelCase to snake_case to match storage file naming convention.
    Examples: 'SalesOrder' -> 'sales_order', 'SalesInvoice' -> 'sales_invoice'
    """
    # Insert underscore before uppercase letters (except the first one)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insert underscore before uppercase letters that follow lowercase letters or numbers
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


def _sanitize_secret_id_component(text: str) -> str:
    """Sanitize user-provided text (e.g., email) for Secret Manager ID component."""
    component = (text or '').strip().lower()
    component = re.sub(r'[^a-z0-9_-]+', '-', component)
    component = component.strip('-_') or 'user'
    # Keep it reasonably short to leave room for suffix
    return component[:128]


def _get_secret_from_gcp(secret_id: str) -> str:
    """Retrieve a secret from Google Secret Manager using explicit keyfile credentials."""
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            raise RuntimeError("KEY_FILE_PATH is not set or file does not exist; cannot access Secret Manager without ADC")

        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        effective_project = os.getenv('GCP_PROJECT') or GCP_PROJECT or getattr(creds, 'project_id', None)
        if not effective_project:
            raise RuntimeError("GCP project not provided and could not be inferred from keyfile")

        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        secret_name = f"projects/{effective_project}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        secret_value = response.payload.data.decode("UTF-8")
        print(f"Successfully retrieved secret: {secret_id}")
        return secret_value.strip()
    except Exception as e:
        print(f"Failed to retrieve secret {secret_id}: {str(e)}")
        add_log(f"Acumatica _get_secret_from_gcp error for {secret_id}: {str(e)}")
        raise RuntimeError(f"Failed to retrieve secret {secret_id}: {str(e)}")


def get_secret_from_secret_manager(secret_name):
    """Wrapper function for backward compatibility."""
    try:
        return _get_secret_from_gcp(secret_name)
    except Exception as e:
        print(f"Failed to retrieve secret {secret_name}: {str(e)}")
        add_log(f"Acumatica get_secret_from_secret_manager error for {secret_name}: {str(e)}")
        return None


def _store_secret_in_gcp(secret_id: str, payload: Dict[str, Any]) -> None:
    """Create or update a secret in Google Secret Manager with JSON payload."""
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            raise RuntimeError("KEY_FILE_PATH is not set or file does not exist; cannot access Secret Manager without ADC")

        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        effective_project = os.getenv('GCP_PROJECT') or GCP_PROJECT or getattr(creds, 'project_id', None)
        if not effective_project:
            raise RuntimeError("GCP project not provided and could not be inferred from keyfile")

        client = secretmanager.SecretManagerServiceClient(credentials=creds)

        parent = f"projects/{effective_project}"
        secret_name = f"projects/{effective_project}/secrets/{secret_id}"

        # Ensure secret exists without requiring secrets.get (avoid 403 on get)
        try:
            client.create_secret(
                parent=parent,
                secret_id=secret_id,
                secret={"replication": {"automatic": {}}},
            )
        except AlreadyExists:
            pass
        except PermissionDenied:
            # If we can add versions but not create or get, continue; add_version may still succeed
            pass

        # Add new version
        payload_bytes = json.dumps(payload).encode('utf-8')
        client.add_secret_version(
            parent=secret_name,
            payload={"data": payload_bytes},
        )
    except Exception as e:
        add_log(f"Acumatica _store_secret_in_gcp error for {secret_id}: {str(e)}")
        raise RuntimeError(f"Failed to store secret: {str(e)}")


def _normalize_filename_remove_timestamp(filename: str) -> str:
    name, ext = os.path.splitext(filename)
    patterns = [
        r"[_-]?\d{8}(?:[_-]?\d{6,})?$",
        r"[ _-]?\d{4}-\d{2}-\d{2}(?:[ T_-]?\d{2}[:_-]?\d{2}[:_-]?\d{2}(?:[._-]?\d+)?)?$",
        r"\(\d{4}.*\)$",
        r"[_-]?\d{10,}$",
    ]
    for pat in patterns:
        name = re.sub(pat, '', name)
    name = name.rstrip(' -_') or 'file'
    return f"{name}{ext}"


def _generate_acumatica_oauth_url(client_id: str, redirect_uri: str, tenant_url: str, scope: str = None) -> str:
    """Generate Acumatica OAuth authorization URL."""
    # Acumatica OAuth endpoint
    base_url = f"{tenant_url}/identity/connect/authorize"
    
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': constants.get('acumatica_scope'),
        'state': 'acumatica_auth'  # Can be customized
    }
    
    # Build the URL
    query_parts = []
    for key, value in params.items():
        if key == 'redirect_uri':
            query_parts.append(f"{key}={value}")
        else:
            query_parts.append(f"{key}={urllib.parse.quote(value)}")
    
    query_string = '&'.join(query_parts)
    return f"{base_url}?{query_string}"


@acumatica_bp.route('/acumatica/oauth_login', methods=['POST'])
def acumatica_oauth_login():
    """
    Generate OAuth authorization URL using credentials stored in Firestore.
    
    This allows the backend to fetch the stored OAuth credentials (client_id, 
    client_secret, tenant_url) and generate the appropriate OAuth URL.
    """
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        user_email = user_email.strip().lower()
        raw_conn = data.get('connection_id')
        connection_id = (str(raw_conn).strip() if raw_conn is not None else '')
        connection_name = (data.get('connection_name') or '').strip()
        add_log(f"Acumatica OAuth: Fetching credentials from Firestore for user: {user_email}")
        
        # Get OAuth credentials from Firestore
        data_manager = get_data_manager()
        acumatica_credentials = None
        try:
            # Only attempt direct fetch if we have a non-empty id to avoid invalid Firestore paths
            if connection_id:
                acumatica_credentials = data_manager.get_credential(user_email, connection_id)
            # Fallback: resolve by connection_name
            if not acumatica_credentials and connection_name:
                creds_list = data_manager.get_all_credentials(user_email, 'acumatica')
                for cdoc in creds_list:
                    if getattr(cdoc, 'connection_name', '') == connection_name:
                        acumatica_credentials = getattr(cdoc, 'credentials', None)
                        break
            # Final fallback: if user has exactly one acumatica credential, use it
            if not acumatica_credentials and not connection_id and not connection_name:
                creds_list = data_manager.get_all_credentials(user_email, 'acumatica')
                if len(creds_list) == 1:
                    acumatica_credentials = getattr(creds_list[0], 'credentials', None)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error retrieving credentials: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        # Validate that we actually found credential material
        if not acumatica_credentials or not isinstance(acumatica_credentials, dict):
            add_log(f"Failed to get credential: No Acumatica credentials found for user {user_email} (id='{connection_id}', name='{connection_name}')")
            return fail('No Acumatica credentials found. Provide a valid connection_id or connection_name, or save credentials first.', 404)
        
        # Use the first credential (maintains backward compatibility when user has only one)
        # If multiple credentials exist, use the first one
        oauth_credentials = acumatica_credentials
        
        # Validate that all required fields are present
        required_fields = ['client_id', 'client_secret', 'tenant_url']
        missing_fields = [field for field in required_fields if not oauth_credentials.get(field)]
        
        if missing_fields:
            add_log(f"Acumatica OAuth: Missing required OAuth fields in Firestore: {missing_fields}")
            return fail(f'Missing required OAuth fields: {missing_fields}', 500)
        
        # Validate client credentials format
        client_id = oauth_credentials['client_id']
        client_secret = oauth_credentials['client_secret']
        tenant_url = oauth_credentials['tenant_url'].rstrip('/')
        
        # Get redirect_uri and scope from environment variables or use defaults
        redirect_uri = constants.get('acumatica_redirect_url')
        scope = constants.get('acumatica_scope')
        
        # Validate inputs
        if not client_id or not redirect_uri or not tenant_url:
            return fail('client_id, tenant_url, and redirect_uri are required', 400)
        
        # Generate the OAuth URL
        oauth_url = _generate_acumatica_oauth_url(client_id, redirect_uri, tenant_url, scope)
        
        add_log(f"Acumatica OAuth: Generated OAuth URL for user {user_email}")
        if job_id:
            add_log(job_id, "Acumatica OAuth URL generated")
        
        return jsonify({
            'status': 'success',
            'status_code': 200,
            'message': 'OAuth authorization URL generated successfully using stored credentials',
            'oauth_url': oauth_url,
            'client_id': client_id[:20] + '...',  # Masked for security
            'redirect_uri': redirect_uri,
            'scope': scope,
            'tenant_url': tenant_url
        }), 200
        
    except ValueError as ve:
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"Acumatica OAuth: Error generating OAuth URL: {str(e)}")
        return fail(f'Failed to generate OAuth URL: {str(e)}', 500)


@acumatica_bp.route('/acumatica/oauth_callback', methods=['GET', 'POST'])
def acumatica_oauth_callback():
    """Handle Acumatica OAuth callback and extract authorization code."""
    try:
        # Get parameters from query string or request data
        if request.method == 'GET':
            authorization_code = request.args.get('code')
            state = request.args.get('state')
            error = request.args.get('error')
            error_description = request.args.get('error_description')
            user_email = request.args.get('email')
            session_id = request.args.get('session_id')
        else:
            data = request.get_json(silent=True) or {}
            authorization_code = data.get('code')
            state = data.get('state')
            error = data.get('error')
            error_description = data.get('error_description')
            user_email = data.get('email')
            session_id = data.get('session_id')
            connection_id= data.get('connection_id')
        
        # If email not provided in request, try to get from session or use default
        if not user_email:
            user_email = request.headers.get('X-User-Email') or 'user@example.com'
            print(f"No email provided in request, using: {user_email}")
        
        # Check for OAuth errors
        if error:
            print(f"OAuth Error: {error}")
            if error_description:
                print(f"Error Description: {error_description}")
            return fail(f'OAuth authorization failed: {error_description or error}', 400)
        
        # Validate required parameters
        if not authorization_code:
            return fail('Authorization code is required', 400)
        
        # Normalize user_email
        user_email = user_email.strip().lower()
        
        add_log(f"Acumatica OAuth Callback: Fetching credentials from Firestore for user: {user_email}")
        
        # Get OAuth credentials from Firestore
        data_manager = get_data_manager()
        acumatica_credentials = None
        try:
            # Only attempt direct fetch if we have a non-empty id to avoid invalid Firestore paths
            if connection_id:
                acumatica_credentials = data_manager.get_credential(user_email, connection_id)
            # Fallback: resolve by connection_name
            if not acumatica_credentials:
                connection_name = (data.get('connection_name') or '').strip()
                if connection_name:
                    creds_list = data_manager.get_all_credentials(user_email, 'acumatica')
                    for cdoc in creds_list:
                        if getattr(cdoc, 'connection_name', '') == connection_name:
                            acumatica_credentials = getattr(cdoc, 'credentials', None)
                            break
            # Final fallback: if user has exactly one acumatica credential, use it
            if not acumatica_credentials and not connection_id and not data.get('connection_name'):
                creds_list = data_manager.get_all_credentials(user_email, 'acumatica')
                if len(creds_list) == 1:
                    acumatica_credentials = getattr(creds_list[0], 'credentials', None)
            print("acumatica_credentials: ",acumatica_credentials)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error retrieving credentials: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        # if not acumatica_credentials or len(acumatica_credentials) == 0:
        #     add_log(f"Acumatica OAuth Callback: No credentials found in Firestore for user: {user_email}")
        #     return fail('Failed to retrieve OAuth credentials from Firestore for token exchange. Please save credentials first.', 404)
        
        # Use the first credential (maintains backward compatibility when user has only one)
        # If multiple credentials exist, use the first one
        print("acumatica_credentials: ",acumatica_credentials)
        oauth_credentials = acumatica_credentials
        
        # Validate that all required fields are present
        required_fields = ['client_id', 'client_secret', 'tenant_url']
        missing_fields = [field for field in required_fields if not oauth_credentials.get(field)]
        
        if missing_fields:
            print(f"Missing required OAuth fields: {missing_fields}")
            return fail(f'Missing required OAuth fields: {missing_fields}', 500)
        
        # Exchange the authorization code for an access token
        tenant_url = oauth_credentials['tenant_url'].rstrip('/')
        token_url = f'{tenant_url}/identity/connect/token'
        redirect_uri = constants.get('acumatica_redirect_url')
        
        token_payload = {
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'client_id': oauth_credentials['client_id'],
            'client_secret': oauth_credentials['client_secret'],
            'redirect_uri': redirect_uri
        }
        
        try:
            token_response = requests.post(token_url, data=token_payload, timeout=30)
            
            if token_response.status_code == 200:
                token_data = token_response.json()
                access_token = token_data.get('access_token')
                refresh_token = token_data.get('refresh_token')
                
                add_log(f"Acumatica OAuth Callback: Successfully obtained access token for user {user_email}")
                
                # NOTE: We do NOT store tokens in Firestore
                # Firestore only stores OAuth credentials (client_id, client_secret, tenant_url)
                # Access tokens are returned to frontend and stored in localStorage
                # This keeps Firestore clean and avoids overwriting credentials
                
                # OAuth successful - return success without fetching data
                # Data will be fetched later when user selects subject area
                return jsonify({
                    'status': 'success',
                    'status_code': 200,
                    'message': 'OAuth flow completed successfully. Please select a subject area to fetch data.',
                    'authorization_code': authorization_code,
                    'token_response': token_data,
                    'state': state,
                    'tenant_url': tenant_url,
                    'user_email': user_email,
                    'session_id': session_id
                }), 200
            else:
                return fail('Failed to exchange authorization code for access token', 400)
                
        except requests.RequestException as e:
            return fail(f'Network error during token exchange: {str(e)}', 500)
        
    except Exception as e:
        add_log(f"Acumatica OAuth callback error: {str(e)}")
        return fail(f'Failed to handle OAuth callback: {str(e)}', 500)


@acumatica_bp.route('/acumatica/check_credentials', methods=['POST'])
def check_acumatica_credentials():
    try:
        data = request.get_json(silent=True) or {}

        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)

        # Build secret id from sanitized email
        original_user_email = user_email
        sanitized_email_for_secret = original_user_email.replace('@', '_').replace('.', '_')
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_acumatica"

        # Determine if secret already exists
        secret_exists = False
        try:
            creds = None
            if KEY_FILE_PATH and KEY_FILE_PATH.exists():
                creds = service_account.Credentials.from_service_account_file(str(KEY_FILE_PATH.resolve()))
            project_id = (
                os.getenv('GCP_PROJECT')
                or GCP_PROJECT
                or os.getenv('GOOGLE_CLOUD_PROJECT')
                or (getattr(creds, 'project_id', None) if creds else None)
            )
            if not project_id:
                raise RuntimeError("GCP project not configured. Set GCP_PROJECT or GOOGLE_CLOUD_PROJECT, or provide a keyfile with project_id.")
            client = secretmanager.SecretManagerServiceClient(credentials=creds) if creds else secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}"
            try:
                client.get_secret(name=secret_name)
                secret_exists = True
            except NotFound:
                secret_exists = False
            except PermissionDenied:
                # Treat permission denied as exists to avoid leaking existence
                secret_exists = True
        except Exception:
            secret_exists = False

        # If secret exists, return it with 200
        if secret_exists:
            creds_payload = None
            try:
                secret_value = get_secret_from_secret_manager(secret_id)
                if secret_value:
                    creds_payload = json.loads(secret_value)
            except Exception:
                creds_payload = None

            return jsonify({
                'status': 'success',
                'status_code': 200,
                'message': 'Credentials already exist',
                'secret_id': secret_id,
            }), 200

        # Secret does not exist
        return fail('Credentials not found for user', 404)
    except ValueError as ve:
        add_log(f"Acumatica check_credentials validation error: {str(ve)}")
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"Acumatica check_credentials error: {str(e)}")
        return fail(f'Failed to check credentials: {str(e)}', 500)


# @acumatica_bp.route('/acumatica/save_credentials', methods=['POST'])
# def save_acumatica_credentials():
#     """
#     Save Acumatica credentials using the unified credential storage pattern.
    
#     Supports both formats:
#     - New format: {user_email, connection_name, credentials: {client_id, client_secret, tenant_url}}
#     - Old format: {user_email, client_id, client_secret, tenant_url} (backward compatible)
    
#     Path: userCollection/{userEmail}/connections/{connectionId}
#     """
#     try:
#         data = request.get_json()
#         if not data:
#             return fail('Missing request body', 400)
        
#         user_email = data.get('user_email', '').strip().lower()
#         if not user_email:
#             return fail('user_email is required in request body', 400)
        
#         # Support both new format (credentials object) and old format (individual fields)
#         credentials_data = data.get('credentials', {})
        
#         # If credentials object not provided, construct from individual fields (backward compatibility)
#         if not credentials_data:
#             client_id = data.get('client_id', '').strip()
#             client_secret = data.get('client_secret', '').strip()
#             tenant_url = data.get('tenant_url', '').strip()
            
#             if not client_id or not client_secret or not tenant_url:
#                 return fail('Either credentials object or individual fields (client_id, client_secret, tenant_url) are required', 400)
            
#             credentials_data = {
#                 'client_id': client_id,
#                 'client_secret': client_secret,
#                 'tenant_url': tenant_url.rstrip('/')
#             }
#         else:
#             # Validate credentials object has required fields
#             if not credentials_data.get('client_id') or not credentials_data.get('client_secret') or not credentials_data.get('tenant_url'):
#                 return fail('credentials object must contain client_id, client_secret, and tenant_url', 400)
#             # Normalize tenant_url
#             credentials_data['tenant_url'] = credentials_data['tenant_url'].rstrip('/')
        
#         # Get connection_name (required for new format, optional for old format with default)
#         connection_name = data.get('connection_name', '').strip()
#         if not connection_name:
#             # Generate default connection name for backward compatibility
#             tenant_domain = credentials_data.get('tenant_url', '').replace('https://', '').replace('http://', '').split('/')[0]
#             connection_name = f"Acumatica - {tenant_domain}" if tenant_domain else "Acumatica Connection"
        
#         connector_type = 'acumatica'
        
#         # Generate unique connection ID
#         connection_id = str(uuid.uuid4())
        
#         # Create credential document
#         credential = CredentialDocument(
#             connection_id=connection_id,
#             connector_type=connector_type,
#             connection_name=connection_name,
#             credentials=credentials_data,
#             is_valid=True,
#             created_at=datetime.utcnow(),
#             updated_at=datetime.utcnow()
#         )
        
#         # Check for duplicate connection_name before saving
#         data_manager = get_data_manager()
        
#         # Check if connection_name already exists for this connector_type
#         if data_manager.credential_exists_by_name(user_email, connector_type, connection_name):
#             return fail(f'A credential with the name "{connection_name}" already exists for connector type "{connector_type}"', 409)
        
#         # Save to Firestore
#         try:
#             success = data_manager.save_credential(user_email, credential)
#         except (RuntimeError, ValueError) as e:
#             # Handle encryption errors specifically
#             if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
#                 add_log(f"Encryption error saving credential: {str(e)}")
#                 return handle_encryption_error(e)
#             raise
        
#         if success:
#             add_log(f"Credential saved: {connector_type}/{connection_id} for {user_email}")
#             return jsonify({
#                 'status': 'success',
#                 'message': f'{connector_type} credential saved successfully',
#                 'connection_id': connection_id,
#                 'connector_type': connector_type
#             }), 201
#         else:
#             return fail('Failed to save credential. Please try again.', 500)
    
#     except (RuntimeError, ValueError) as e:
#         # Handle encryption errors
#         if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
#             add_log(f"Encryption error saving credential: {str(e)}")
#             return handle_encryption_error(e)
#         add_log(f"Error saving credential: {str(e)}")
#         return fail(f'Error saving credential: {str(e)}', 500)
#     except Exception as e:
#         add_log(f"Error saving credential: {str(e)}")
#         return fail('An unexpected error occurred while saving the credential. Please try again.', 500)


@acumatica_bp.route('/acumatica/save_credentials', methods=['POST'])
def save_acumatica_credentials():
        """
        Legacy endpoint placeholder. Acumatica credentials are now handled via the unified
        credentials API in api_layer/credentials.py. This route is kept only to avoid 404s.
        """
        add_log("Acumatica save_credentials called but is not supported; use unified credentials API instead")
        return fail('Acumatica save_credentials is not supported. Please use the unified credentials API.', 501)

def _download_pickle_files_from_firebase(user_email: str, target_dir: str) -> List[str]:
    """Download all pickle and CSV files from Firebase Storage path <user_email>/data/acumatica into target_dir."""
    os.makedirs(target_dir, exist_ok=True)
    saved_files: List[str] = []

    try:
        # Try using firebase_admin if available
        try:
            import firebase_admin
            from firebase_admin import storage
            if not firebase_admin._apps:
                raise Exception('Firebase not initialized')
            bucket = storage.bucket()
            prefix = f"{user_email}/data/acumatica/"
            blobs = list(bucket.list_blobs(prefix=prefix))
            add_log(f"Acumatica import: listing via firebase_admin - bucket={bucket.name}, prefix={prefix}")
            for blob in blobs:
                # Download both .pkl and .csv files
                if blob.name.lower().endswith(('.pkl', '.csv')):
                    original = os.path.basename(blob.name)
                    normalized = _normalize_filename_remove_timestamp(original)
                    local_path = os.path.join(target_dir, normalized)
                    blob.download_to_filename(local_path)
                    saved_files.append(local_path)
            if saved_files:
                return saved_files
        except Exception as e:
            add_log(f"Acumatica import: Firebase admin listing failed: {str(e)}")

        # Fallback: use Google Cloud Storage client with explicit key
        try:
            if gcs is None:
                add_log("Acumatica import: google-cloud-storage not available; skipping GCS fallback")
                return saved_files
            creds = None
            if KEY_FILE_PATH and KEY_FILE_PATH.exists():
                creds = service_account.Credentials.from_service_account_file(str(KEY_FILE_PATH.resolve()))
            project_id = os.getenv('GCP_PROJECT') or GCP_PROJECT or (getattr(creds, 'project_id', None) if creds else None)
            gcs_client = gcs.Client(project=project_id, credentials=creds) if creds else gcs.Client(project=project_id)
            bucket_name = constants.get('storage_bucket')
            prefix = f"{user_email}/data/acumatica/"
            try:
                bucket_obj = gcs_client.bucket(bucket_name)
                blobs_iter = gcs_client.list_blobs(bucket_or_name=bucket_obj, prefix=prefix)
                for blob in blobs_iter:
                    # Download both .pkl and .csv files
                    if blob.name.lower().endswith(('.pkl', '.csv')):
                        original = os.path.basename(blob.name)
                        normalized = _normalize_filename_remove_timestamp(original)
                        local_path = os.path.join(target_dir, normalized)
                        blob.download_to_filename(local_path)
                        saved_files.append(local_path)
                if saved_files:
                    add_log(f"Acumatica import: downloaded {len(saved_files)} files from bucket {bucket_name}")
            except Exception as be:
                add_log(f"Acumatica import: GCS listing failed for bucket {bucket_name}: {str(be)}")
            return saved_files
        except Exception as ge:
            add_log(f"Acumatica import: GCS fallback failed: {str(ge)}")
            return saved_files
    except Exception as e:
        add_log(f"Acumatica _download_pickle_files_from_firebase error for {user_email}: {str(e)}")
        raise RuntimeError(f"Failed to download files: {str(e)}")


@acumatica_bp.route('/acumatica/fetch_data_for_subject', methods=['POST'])
def fetch_data_for_subject():
    """Fetch Acumatica data for a specific subject area after OAuth connection."""
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        session_id = _validate_non_empty_string(data.get('session_id'), 'session_id', min_len=8)
        
        # Get subject area information
        subject_area = data.get('subject_area')
        subject_area_name = data.get('subject_area_name')
        subject_area_description = data.get('subject_area_description')
        
        if not subject_area:
            return fail('Subject area is required for data fetching', 400)
        
        # Validate that the subject area is supported
        valid_subject_areas = [sa['id'] for sa in constants.get('acumatica_subject_areas')]
        if subject_area not in valid_subject_areas:
            return fail(f'Subject area "{subject_area}" is not supported. Supported areas: {valid_subject_areas}', 400)
        
        # Cloud function URL
        cloud_function_url = constants.get('acumatica_cloud_function_url')
        
        # Get OAuth tokens from session/request
        oauth_tokens = data.get('oauth_tokens')
        if not oauth_tokens or not oauth_tokens.get('access_token'):
            return fail('Access token is required. Please authenticate with Acumatica first.', 401)
        
        # Fetch credentials from Firestore
        data_manager = get_data_manager()
        credential = data_manager.get_credential(user_email, data.get('connection_id', '').strip())
        if not credential:
            return fail('No Acumatica credentials found. Please save credentials first.', 404)
        


        # Prepare cloud function payload
        cloud_function_payload = {
            "email": user_email,
            "accessToken": oauth_tokens.get('access_token'),
            "subject_area": subject_area,
            "subject_area_name": subject_area_name or subject_area.title(),
            "subject_area_description": subject_area_description or f"{subject_area.title()} data analysis",
            "client_id": credential.get('client_id'),
            "client_secret": credential.get('client_secret'),
            "tenant_url": credential.get('tenant_url')
        }
        
        # Call cloud function to fetch data
        try:
            print("Calling Acumatica cloud function to fetch data", cloud_function_url)
            print("Cloud function payload", cloud_function_payload)
            cf_response = requests.post(cloud_function_url, json=cloud_function_payload, timeout=(10, 3600))
            
            if cf_response.status_code == 200:
                cf_data = cf_response.json()
                
                # Download and save files to session directory
                try:
                    # Sanitize email for storage
                    sanitized_email_for_storage = user_email.replace('@', '_').replace('.', '_')
                    
                    # Clear existing input data before processing new Acumatica data
                    _clear_session_input_data(session_id)
                    
                    # Create session input data directory
                    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                    input_data_dir = os.path.join(base_dir, 'execution_layer', 'input_data', session_id)
                    os.makedirs(input_data_dir, exist_ok=True)
                    
                    # Download files from Firebase Storage
                    downloaded_files = _download_pickle_files_from_firebase(
                        user_email=sanitized_email_for_storage, 
                        target_dir=input_data_dir
                    )

                    print("Downloaded files", downloaded_files)
                    
                    # Get entities for the subject area
                    subject_area_entities = next((area['entities'] for area in constants.get('acumatica_subject_areas') if area['id'] == subject_area), [])
                    
                    # Build saved_files list dynamically from cloud function response storage information
                    # This ensures saved_files reflects what was actually saved in THIS request
                    saved_files = []
                    if cf_data.get('results'):
                        for entity_name, entity_result in cf_data['results'].items():
                            storage_info = entity_result.get('storage', {})
                            if isinstance(storage_info, dict):
                                for storage_key, storage_value in storage_info.items():
                                    if isinstance(storage_value, dict) and storage_value.get('status') == 'success':
                                        file_path = storage_value.get('file_path', '')
                                        if file_path:
                                            # Extract just the filename from the path
                                            filename = os.path.basename(file_path)
                                            # Normalize to remove timestamp (to match downloaded files)
                                            normalized_filename = _normalize_filename_remove_timestamp(filename)
                                            # Add full path to match downloaded_files format
                                            full_path = os.path.join(input_data_dir, normalized_filename)
                                            if full_path not in saved_files:
                                                saved_files.append(full_path)
                    
                    print("Saved files (from cloud function response)", saved_files)
                    
                    add_log(f"Acumatica data fetch: {len(saved_files)} files saved to server for session {session_id}")
                    if job_id:
                        add_log(job_id, f"Acumatica: fetched data for subject '{subject_area}' into session {session_id} ({len(saved_files)} files)")
                    
                    # Extract preview data from cloud function response dynamically
                    preview_data = {}
                    if cf_data.get('results'):
                        for entity_name, entity_result in cf_data['results'].items():
                            if entity_result.get('preview_data'):
                                preview_data_dict = entity_result['preview_data']
                                storage_info = entity_result.get('storage', {})
                                
                                # Build file extension map dynamically from storage information
                                # Maps preview_data keys to their file extensions
                                file_ext_map = {}
                                
                                for storage_key, storage_value in storage_info.items():
                                    if isinstance(storage_value, dict) and storage_value.get('status') != 'empty':
                                        file_path = storage_value.get('file_path', '')
                                        if file_path:
                                            # Extract file extension from storage path
                                            file_ext = '.csv' if file_path.endswith('.csv') else '.pkl'
                                            
                                            # Map storage key to preview_data key
                                            # Examples:
                                            # 'stock_items' -> 'stock_items' -> '.pkl'
                                            # 'warehouse' -> 'warehouse' -> '.pkl'
                                            # 'sales_order_csv' -> 'sales_order' -> '.csv'
                                            # 'details_csv' -> 'details' -> '.csv'
                                            
                                            # Normalize storage key to match preview_data key
                                            # Remove '_csv' or '_pkl' suffix if present
                                            preview_key = storage_key.replace('_csv', '').replace('_pkl', '')
                                            file_ext_map[preview_key] = file_ext
                                
                                # Extract preview data dynamically - iterate through all keys
                                if isinstance(preview_data_dict, dict):
                                    # Check if preview_data has only 'top_records' key (standard format from generate_preview_data)
                                    # In this case, treat it as flat preview data and use entity name
                                    if list(preview_data_dict.keys()) == ['top_records']:
                                        # Determine extension from storage or default
                                        file_ext = None
                                        if storage_info:
                                            # Get first storage entry to determine extension
                                            first_storage = next(iter(storage_info.values()))
                                            if isinstance(first_storage, dict):
                                                file_path = first_storage.get('file_path', '')
                                                if file_path:
                                                    file_ext = '.csv' if file_path.endswith('.csv') else '.pkl'
                                        
                                        if not file_ext:
                                            file_ext = '.csv' if entity_name == 'SalesOrder' else '.pkl'
                                        
                                        # Convert entity name to snake_case to match storage file naming
                                        entity_snake = _camel_to_snake(entity_name)
                                        filename = f"{entity_snake}{file_ext}"
                                        preview_data[filename] = preview_data_dict
                                    else:
                                        # If preview_data is nested (e.g., {'stock_items': {...}, 'warehouse': {...}})
                                        for preview_key, preview_value in preview_data_dict.items():
                                            # Skip 'top_records' key if it appears with other keys
                                            if preview_key == 'top_records':
                                                continue
                                            
                                            # Get file extension from map, or infer from storage key pattern
                                            file_ext = file_ext_map.get(preview_key)
                                            
                                            # If not found in map, try to infer from storage keys
                                            if not file_ext:
                                                # Check if any storage key contains this preview_key
                                                for storage_key in storage_info.keys():
                                                    if preview_key in storage_key.lower():
                                                        storage_value = storage_info.get(storage_key, {})
                                                        if isinstance(storage_value, dict):
                                                            file_path = storage_value.get('file_path', '')
                                                            if file_path:
                                                                file_ext = '.csv' if file_path.endswith('.csv') else '.pkl'
                                                                break
                                            
                                            # Default fallback: use .pkl for most entities, .csv for SalesOrder
                                            if not file_ext:
                                                file_ext = '.csv' if entity_name == 'SalesOrder' else '.pkl'
                                            
                                            # Build filename dynamically
                                            # Handle special naming conventions (e.g., 'details' -> 'sales_order_details')
                                            if preview_key == 'details' and entity_name == 'SalesOrder':
                                                filename = f"sales_order_details{file_ext}"
                                            else:
                                                filename = f"{preview_key}{file_ext}"
                                            
                                            preview_data[filename] = preview_value
                                else:
                                    # If preview_data is flat (not nested), use entity name
                                    # Determine extension from storage or default
                                    file_ext = None
                                    if storage_info:
                                        # Get first storage entry to determine extension
                                        first_storage = next(iter(storage_info.values()))
                                        if isinstance(first_storage, dict):
                                            file_path = first_storage.get('file_path', '')
                                            if file_path:
                                                file_ext = '.csv' if file_path.endswith('.csv') else '.pkl'
                                    
                                    if not file_ext:
                                        file_ext = '.csv' if entity_name == 'SalesOrder' else '.pkl'
                                    
                                    # Convert entity name to snake_case to match storage file naming
                                    entity_snake = _camel_to_snake(entity_name)
                                    filename = f"{entity_snake}{file_ext}"
                                    preview_data[filename] = preview_data_dict
                    
                    # Build saved_files list for response (just filenames, not full paths)
                    saved_files_list = [os.path.basename(p) for p in saved_files] if saved_files else []
                    
                    return jsonify({
                        'status': 'success',
                        'status_code': 200,
                        'message': f'Data fetched successfully for {subject_area_name or subject_area} subject area',
                        'cloud_function_response': cf_data,
                        'session_id': session_id,
                        'saved_files': saved_files_list,
                        'target_dir': input_data_dir,
                        'subject_area': subject_area,
                        'subject_area_name': subject_area_name,
                        'entities_fetched': subject_area_entities,
                        'preview_data': preview_data
                    }), 200
                    
                except Exception as file_error:
                    add_log(f"Acumatica data fetch: Error downloading files: {str(file_error)}")
                    return fail('Data fetched but file download failed', 500)
            else:
                print(f"Acumatica fetch_data_for_subject cloud function response: {cf_response.text}")
                return fail('Failed to fetch data from Acumatica', cf_response.status_code)
                
        except requests.RequestException as cf_error:
            print(f"Acumatica fetch_data_for_subject cloud function error: {str(cf_error)}")
            add_log(f"Acumatica fetch_data_for_subject cloud function error: {str(cf_error)}")
            return fail(f'Failed to call cloud function: {str(cf_error)}', 500)
            
    except ValueError as ve:
        print("Error fetching data: ",ve)
        add_log(f"Acumatica fetch_data_for_subject validation error: {str(ve)}")
        return fail(str(ve), 400)
    except Exception as e:
        print("Error fetching data: ",e)
        add_log(f"Acumatica fetch_data_for_subject unexpected error: {str(e)}")
        return fail(f'Failed to fetch data: {str(e)}', 500)

