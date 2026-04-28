from flask import Blueprint, request, jsonify, g
import os
import json
import re
import requests
import urllib.parse
from typing import Dict, Any, List
from pathlib import Path
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound, AlreadyExists, PermissionDenied
from google.oauth2 import service_account
from google.cloud import storage as gcs
from logger import add_log
from .data import _clear_session_input_data
from utils.env import init_env
from utils.env import init_env, KEY_FILE_PATH, GCP_PROJECT
from utils.env import ENV as CURRENT_ENV
from utils.sanitize import sanitize_email_for_storage
from api_layer.firebase_data_models import get_data_manager, CredentialDocument
from api_layer.credentials import handle_encryption_error

salesforce_bp = Blueprint('salesforce_bp', __name__)

constants = init_env()

def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'Salesforce operation failed'}), status



@salesforce_bp.route('/salesforce/subject_areas', methods=['GET'])
def get_salesforce_subject_areas():
    """Get available Salesforce subject areas."""
    try:
        return jsonify({
            'status': 'success',
            'status_code': 200,
            'subject_areas': constants.get('salesforce_subject_areas')
        }), 200
    except Exception as e:
        add_log(f"Salesforce get_salesforce_subject_areas error: {str(e)}")
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


def _sanitize_secret_id_component(text: str) -> str:
    """Sanitize user-provided text (e.g., email) for Secret Manager ID component."""
    component = (text or '').strip().lower()
    component = re.sub(r'[^a-z0-9_-]+', '-', component)
    component = component.strip('-_') or 'user'
    # Keep it reasonably short to leave room for suffix
    return component[:128]


def _get_secret_from_gcp(secret_id: str) -> str:
    """Retrieve a secret from Google Secret Manager using explicit keyfile credentials, like utils.env."""
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
        secret_value = response.payload.data.decode("utf-8")
        print(f"Successfully retrieved secret: {secret_id}")
        return secret_value.strip()
    except Exception as e:
        print(f"Failed to retrieve secret {secret_id}: {str(e)}")
        add_log(f"Salesforce _get_secret_from_gcp error for {secret_id}: {str(e)}")
        raise RuntimeError(f"Failed to retrieve secret {secret_id}: {str(e)}")
 

def get_secret_from_secret_manager(secret_name):
    """Wrapper function for backward compatibility."""
    try:
        return _get_secret_from_gcp(secret_name)
    except Exception as e:
        print(f"Failed to retrieve secret {secret_name}: {str(e)}")
        add_log(f"Salesforce get_secret_from_secret_manager error for {secret_name}: {str(e)}")
        return None



def _store_secret_in_gcp(secret_id: str, payload: Dict[str, Any]) -> None:
    """Create or update a secret in Google Secret Manager with JSON payload using explicit keyfile."""
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
        add_log(f"Salesforce _store_secret_in_gcp error for {secret_id}: {str(e)}")
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


def _generate_salesforce_oauth_url(client_id: str, redirect_uri: str, scope: str = "api refresh_token openid", environment: str = "sandbox") -> str:
    """Generate Salesforce OAuth authorization URL with direct parameters."""
    # Select base URL based on environment
    if environment.lower() == "production":
        base_url = constants.get('salesforce_base_url_prod')
    else:  # Default to sandbox
        base_url = constants.get('salesforce_base_url_sandbox')
    
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': scope,
        'state': environment,
        'prompt' : 'login consent'  # Pass environment as state parameter
    }
    
    # Build the URL without encoding the redirect_uri and scope (they're already properly formatted)
    query_parts = []
    for key, value in params.items():
        if key in ['redirect_uri', 'scope']:
            # Don't encode redirect_uri and scope as they're already properly formatted
            query_parts.append(f"{key}={value}")
        else:
            query_parts.append(f"{key}={urllib.parse.quote(value)}")
    
    query_string = '&'.join(query_parts)
    return f"{base_url}?{query_string}"


@salesforce_bp.route('/salesforce/oauth_login', methods=['POST'])
def salesforce_oauth_login():
    """Generate Salesforce OAuth authorization URL with direct parameters."""
    try:
        data = request.get_json(silent=True) or {}
        
        # Get environment from request (default to sandbox)
        environment = data.get('environment', 'sandbox').lower()
        if environment not in ['sandbox', 'production']:
            environment = 'sandbox'  # Default to sandbox if invalid
        
        # Get OAuth credentials from Secret Manager
        oauth_credentials_json = get_secret_from_secret_manager('salesforce_oauth_credentials')
        
        if not oauth_credentials_json:
            return jsonify({
                'error': 'Failed to retrieve OAuth credentials from Secret Manager'
            }), 500
        
        # Parse the JSON string to get credentials dictionary
        try:
            oauth_credentials = json.loads(oauth_credentials_json)
        except json.JSONDecodeError as e:
            print(f"Failed to parse OAuth credentials JSON: {str(e)}")
            return jsonify({
                'error': 'Invalid OAuth credentials format in Secret Manager'
            }), 500
        
        # Validate that all required fields are present
        required_fields = ['client_id', 'client_secret']
        missing_fields = [field for field in required_fields if field not in oauth_credentials]
        
        if missing_fields:
            print(f"Missing required OAuth fields: {missing_fields}")
            return jsonify({
                'error': f'Missing required OAuth fields: {missing_fields}'
            }), 500
        
        # Validate client credentials format
        client_id = oauth_credentials['client_id']
        client_secret = oauth_credentials['client_secret']
        
        # Basic validation for Salesforce client ID format
        if not client_id.startswith('3MVG'):
            print("WARNING: Client ID doesn't start with '3MVG' - this might not be a valid Salesforce client ID")
        
        if len(client_secret) < 20:
            print("WARNING: Client secret seems too short - might be invalid")
        
        # Get redirect_uri and scope from environment variables or use defaults
        redirect_uri = constants.get('salesforce_redirect_url')
        # os.getenv('SALESFORCE_REDIRECT_URI', 'http://localhost:3000/salesforce/oauth/callback')
        scope = constants.get('salesforce_oauth_scope')
        # os.getenv('SALESFORCE_OAUTH_SCOPE', 'api refresh_token openid')
        
        # Validate inputs
        if not client_id or not redirect_uri:
            return jsonify({
                'error': 'client_id and redirect_uri are required'
            }), 400
        
        # Generate the OAuth URL with environment selection
        oauth_url = _generate_salesforce_oauth_url(client_id, redirect_uri, scope, environment)
        
        return jsonify({
            'status': 'success',
            'status_code': 200,
            'message': 'OAuth authorization URL generated successfully',
            'oauth_url': oauth_url,
            'client_id': client_id[:20] + '...',  # Masked for security
            'redirect_uri': redirect_uri,
            'scope': scope,
            'environment': environment
        }), 200
        
    except Exception as e:
        add_log(f"Salesforce OAuth login error: {str(e)}")
        return jsonify({
            'error': f'Failed to generate OAuth URL: {str(e)}'
        }), 500


# def salesforce_oauth_login():
#     """
#     Generate OAuth authorization URL using credentials stored in Firestore.
    
#     This allows the backend to fetch the stored OAuth credentials (client_id, 
#     client_secret) and generate the appropriate OAuth URL.
#     """
#     try:
#         data = request.get_json(silent=True) or {}
#         user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
#         user_email = user_email.strip().lower()
        
#         # Get environment from request (default to sandbox)
#         environment = data.get('environment', 'sandbox').lower()
#         if environment not in ['sandbox', 'production']:
#             environment = 'sandbox'  # Default to sandbox if invalid

#         add_log(f"Salesforce OAuth: Fetching credentials from Firestore for user: {user_email}")
        
#         # Get OAuth credentials from Firestore
#         data_manager = get_data_manager()
#         try:
#             salesforce_credentials = data_manager.get_all_credentials(user_email, 'salesforce')
#         except (RuntimeError, ValueError) as e:
#             # Handle encryption errors specifically
#             if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
#                 add_log(f"Encryption error retrieving credentials: {str(e)}")
#                 return handle_encryption_error(e)
#             raise
        
#         if not salesforce_credentials or len(salesforce_credentials) == 0:
#             add_log(f"Salesforce OAuth: No credentials found in Firestore for user: {user_email}")
#             return fail('Failed to retrieve OAuth credentials from Firestore. Please save credentials first.', 404)
        
#         # Use the first credential (maintains backward compatibility when user has only one)
#         # If multiple credentials exist, use the first one
#         credential_doc = salesforce_credentials[0]
#         oauth_credentials = credential_doc.credentials
        
#         # Validate that all required fields are present
#         required_fields = ['client_id', 'client_secret']
#         missing_fields = [field for field in required_fields if field not in oauth_credentials]
        
#         if missing_fields:
#             add_log(f"Salesforce OAuth: Missing required OAuth fields in Firestore: {missing_fields}")
#             return fail(f'Missing required OAuth fields: {missing_fields}', 500)
        
#         # Validate client credentials format
#         client_id = oauth_credentials['client_id']
#         client_secret = oauth_credentials['client_secret']
        
#         # Basic validation for Salesforce client ID format
#         if not client_id.startswith('3MVG'):
#             print("WARNING: Client ID doesn't start with '3MVG' - this might not be a valid Salesforce client ID")
        
#         if len(client_secret) < 20:
#             print("WARNING: Client secret seems too short - might be invalid")
        
#         # Get redirect_uri and scope from environment variables or use defaults
#         redirect_uri = constants.get('salesforce_redirect_url')
#         scope = constants.get('salesforce_oauth_scope')
        
#         # Validate inputs
#         if not client_id or not redirect_uri:
#             return fail('client_id, and redirect_uri are required', 400)
        
#         # Generate the OAuth URL with environment selection
#         oauth_url = _generate_salesforce_oauth_url(client_id, redirect_uri, scope, environment)
        
#         add_log(f"Salesforce OAuth: Generated OAuth URL for user {user_email}")
        
#         return jsonify({
#             'status': 'success',
#             'status_code': 200,
#             'message': 'OAuth authorization URL generated successfully using stored credentials',
#             'oauth_url': oauth_url,
#             'client_id': client_id[:20] + '...',  # Masked for security
#             'redirect_uri': redirect_uri,
#             'scope': scope,
#             'environment': environment
#         }), 200
        
#     except ValueError as ve:
#         return fail(str(ve), 400)
#     except Exception as e:
#         add_log(f"Salesforce OAuth: Error generating OAuth URL: {str(e)}")
#         return fail(f'Failed to generate OAuth URL: {str(e)}', 500)


@salesforce_bp.route('/salesforce/oauth_callback', methods=['GET', 'POST'])
def salesforce_oauth_callback():
    """Handle Salesforce OAuth callback and extract authorization code."""
    try:
        # Get parameters from query string or request data
        if request.method == 'GET':
            authorization_code = request.args.get('code')
            state = request.args.get('state')
            error = request.args.get('error')
            error_description = request.args.get('error_description')
            user_email = request.args.get('email')  # Get email from query params
            session_id = request.args.get('session_id')  # Get session_id from query params
        else:
            data = request.get_json(silent=True) or {}
            authorization_code = data.get('code')
            state = data.get('state')
            error = data.get('error')
            error_description = data.get('error_description')
            user_email = data.get('email')  # Get email from request body
            session_id = data.get('session_id')  # Get session_id from request body
        
        # If email not provided in request, try to get from session or use default
        if not user_email:
            # You can implement session-based user email retrieval here
            # For now, we'll use a default or get from headers
            user_email = request.headers.get('X-User-Email') or 'user@example.com'
            print(f"No email provided in request, using: {user_email}")
        
        # Check for OAuth errors
        if error:
            print(f"OAuth Error: {error}")
            if error_description:
                print(f"Error Description: {error_description}")
            return jsonify({
                'status': 'error',
                'error': error,
                'error_description': error_description,
                'message': 'OAuth authorization failed'
            }), 400
        
        # Validate required parameters
        if not authorization_code:
            return jsonify({
                'error': 'Authorization code is required'
            }), 400
        
        
        # Get OAuth credentials from Secret Manager for token exchange
        oauth_credentials_json = get_secret_from_secret_manager('salesforce_oauth_credentials')
        
        if not oauth_credentials_json:
            return jsonify({
                'status': 'error',
                'message': 'Failed to retrieve OAuth credentials from Secret Manager for token exchange',
                'authorization_code': authorization_code,
                'state': state
            }), 500
        
        # Parse the JSON string to get credentials dictionary
        try:
            oauth_credentials = json.loads(oauth_credentials_json)
        except json.JSONDecodeError as e:
            print(f"Failed to parse OAuth credentials JSON: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Invalid OAuth credentials format in Secret Manager',
                'authorization_code': authorization_code,
                'state': state
            }), 500
        
        # Validate that all required fields are present
        required_fields = ['client_id', 'client_secret']
        missing_fields = [field for field in required_fields if field not in oauth_credentials]
        
        if missing_fields:
            print(f"Missing required OAuth fields: {missing_fields}")
            return jsonify({
                'status': 'error',
                'message': f'Missing required OAuth fields: {missing_fields}',
                'authorization_code': authorization_code,
                'state': state
            }), 500
        
        # Exchange the authorization code for an access token
        # Get environment from state parameter (passed from OAuth URL generation)
        environment = state if state in ['sandbox', 'production'] else 'production'
        
        # Select token URL based on environment
        if environment == 'production':
            token_url = constants.get('salesforce_token_url_prod')
        else:
            token_url = constants.get('salesforce_token_url_sandbox')
            
        redirect_uri = constants.get('salesforce_redirect_url')
        
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
                instance_url = token_data.get('instance_url')
                
                
                # OAuth successful - return success without fetching data
                # Data will be fetched later when user selects subject area
                return jsonify({
                    'status': 'success',
                    'status_code': 200,
                    'message': 'OAuth flow completed successfully. Please select a subject area to fetch data.',
                    'authorization_code': authorization_code,
                    'token_response': token_data,
                    'state': state
                }), 200
            else:
                
                return jsonify({
                    'status': 'error',
                    'status_code': token_response.status_code,
                    'message': 'Failed to exchange authorization code for access token',
                    'authorization_code': authorization_code,
                    'error_response': token_response.text,
                    'state': state
                }), 400
                
        except requests.RequestException as e:
            add_log(f"Salesforce OAuth callback network error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Network error during token exchange: {str(e)}',
                'authorization_code': authorization_code,
                'state': state
            }), 500
        
    except Exception as e:
        add_log(f"Salesforce OAuth callback error: {str(e)}")
        return jsonify({
            'error': f'Failed to handle OAuth callback: {str(e)}'
        }), 500

# def salesforce_oauth_callback():
#     """Handle Salesforce OAuth callback and extract authorization code."""
#     try:
#         # Get parameters from query string or request data
#         if request.method == 'GET':
#             authorization_code = request.args.get('code')
#             state = request.args.get('state')
#             error = request.args.get('error')
#             error_description = request.args.get('error_description')
#             user_email = request.args.get('email')
#             session_id = request.args.get('session_id')
#         else:
#             data = request.get_json(silent=True) or {}
#             authorization_code = data.get('code')
#             state = data.get('state')
#             error = data.get('error')
#             error_description = data.get('error_description')
#             user_email = data.get('email')
#             session_id = data.get('session_id')
#             connection_id = data.get('connection_id')
        
#         # If email not provided in request, try to get from session or use default
#         if not user_email:
#             # You can implement session-based user email retrieval here
#             # For now, we'll use a default or get from headers
#             user_email = request.headers.get('X-User-Email') or 'user@example.com'
#             print(f"No email provided in request, using: {user_email}")
        
#         # Check for OAuth errors
#         if error:
#             print(f"OAuth Error: {error}")
#             if error_description:
#                 print(f"Error Description: {error_description}")
#             return fail(f'OAuth authorization failed: {error_description or error}', 400)
        
#         # Validate required parameters
#         if not authorization_code:
#             return fail('Authorization code is required', 400)
        
#         # Normalize user_email
#         user_email = user_email.strip().lower()
        
#         add_log(f"Salesforce OAuth Callback: Fetching credentials from Firestore for user: {user_email}")
        
#         # Get OAuth credentials from Firestore
#         data_manager = get_data_manager()
#         try:
#             salesforce_credentials = data_manager.get_credential(user_email, connection_id)
#             print("salesforce_credentials: ", salesforce_credentials)
#         except (RuntimeError, ValueError) as e:
#             # Handle encryption errors specifically
#             if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
#                 add_log(f"Encryption error retrieving credentials: {str(e)}")
#                 return handle_encryption_error(e)
#             raise
        
#         # Use the credential document directly (get_credential returns a single document)
#         print("salesforce_credentials: ", salesforce_credentials)
#         credential_doc = salesforce_credentials
#         oauth_credentials = credential_doc.credentials
        
#         # Validate that all required fields are present
#         required_fields = ['client_id', 'client_secret']
#         missing_fields = [field for field in required_fields if field not in oauth_credentials]
        
#         if missing_fields:
#             print(f"Missing required OAuth fields: {missing_fields}")
#             return fail(f'Missing required OAuth fields: {missing_fields}', 500)
        
#         # Exchange the authorization code for an access token
#         # Get environment from state parameter (passed from OAuth URL generation)
#         environment = state if state in ['sandbox', 'production'] else 'production'
        
#         # Select token URL based on environment
#         if environment == 'production':
#             token_url = constants.get('salesforce_token_url_prod')
#         else:
#             token_url = constants.get('salesforce_token_url_sandbox')
            
#         redirect_uri = constants.get('salesforce_redirect_url')
        
#         token_payload = {
#             'grant_type': 'authorization_code',
#             'code': authorization_code,
#             'client_id': oauth_credentials['client_id'],
#             'client_secret': oauth_credentials['client_secret'],
#             'redirect_uri': redirect_uri
#         }
        
#         try:
#             token_response = requests.post(token_url, data=token_payload, timeout=30)
            
#             if token_response.status_code == 200:
#                 token_data = token_response.json()
#                 access_token = token_data.get('access_token')
#                 instance_url = token_data.get('instance_url')
                
#                 add_log(f"Salesforce OAuth Callback: Successfully obtained access token for user {user_email}")
                
#                 # NOTE: We do NOT store tokens in Firestore
#                 # Firestore only stores OAuth credentials (client_id, client_secret)
#                 # Access tokens are returned to frontend and stored in localStorage
#                 # This keeps Firestore clean and avoids overwriting credentials
                
#                 # OAuth successful - return success without fetching data
#                 # Data will be fetched later when user selects subject area
#                 return jsonify({
#                     'status': 'success',
#                     'status_code': 200,
#                     'message': 'OAuth flow completed successfully. Please select a subject area to fetch data.',
#                     'authorization_code': authorization_code,
#                     'token_response': token_data,
#                     'state': state,
#                     'environment': environment,
#                     'user_email': user_email,
#                     'session_id': session_id
#                 }), 200
#             else:
#                 return fail('Failed to exchange authorization code for access token', 400)
                
#         except requests.RequestException as e:
#             return fail(f'Network error during token exchange: {str(e)}', 500)
        
#     except Exception as e:
#         return fail(f'Failed to handle OAuth callback: {str(e)}', 500)


@salesforce_bp.route('/salesforce/check_credentials', methods=['POST'])
def check_salesforce_credentials():
    try:
        data = request.get_json(silent=True) or {}

        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)

        # Build secret id from sanitized email (do not modify original for downloads/CF)
        original_user_email = user_email
        sanitized_email_for_secret = original_user_email.replace('@', '_').replace('.', '_')
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_salesforce"

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
                # 'credentials': creds_payload

        # Secret does not exist
        return fail('Credentials not found for user', 404)
    except ValueError as ve:
        add_log(f"Salesforce check_credentials validation error: {str(ve)}")
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"Salesforce check_credentials error: {str(e)}")
        return fail(f'Failed to save credentials: {str(e)}', 500)


@salesforce_bp.route('/salesforce/save_credentials', methods=['POST'])
def save_salesforce_credentials():
    try:
        data = request.get_json(silent=True) or {}

        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)

        original_user_email = user_email
        sanitized_email_for_secret = original_user_email.replace('@', '_').replace('.', '_')
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_salesforce"

        client_id = _validate_non_empty_string(data.get('client_id'), 'client_id')
        client_secret = _validate_non_empty_string(data.get('client_secret'), 'client_secret')
        username = _validate_non_empty_string(data.get('username'), 'username')
        password = _validate_non_empty_string(data.get('password'), 'password')
        security_key = _validate_non_empty_string(data.get('security_key'), 'security_key', min_len=10)

        secret_payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password + security_key,
            'environment': data.get('environment', 'sandbox'),  # Store environment preference
        }
        _store_secret_in_gcp(secret_id, secret_payload)

        add_log(f"Salesforce: saved new secrets for {user_email}")

        return jsonify({
            'status': 'success',
            'status_code': 201,
            'message': 'Credentials saved',
            'secret_id': secret_id
        }), 201
    except ValueError as ve:
        add_log(f"Salesforce save_credentials validation error: {str(ve)}")
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        add_log(f"Salesforce save_credentials error: {str(e)}")
        return jsonify({'error': f'Failed to save credentials: {str(e)}'}), 500


def _download_pickle_files_from_firebase(user_email: str, target_dir: str) -> List[str]:
    """Download all pickle files from Firebase Storage path <user_email>/data/salesforce into target_dir.

    Note: Using Firebase Admin Storage SDK listing would be ideal, but if not configured,
    attempt HTTPS download for known .pkl files when an index is provided by a Cloud Function.
    """
    os.makedirs(target_dir, exist_ok=True)
    saved_files: List[str] = []

    try:
        # We expect the Cloud Function to have created files and possibly returned their names.
        # As a fallback, list using firebase_admin if available.
        try:
            import firebase_admin
            from firebase_admin import storage
            if not firebase_admin._apps:
                # If Firebase not initialized elsewhere, attempt default init via env
                raise Exception('Firebase not initialized')
            bucket = storage.bucket()
            prefix = f"{user_email}/data/salesforce/"
            # List blobs under the prefix
            blobs = list(bucket.list_blobs(prefix=prefix))
            add_log(f"Salesforce import: listing via firebase_admin - bucket={bucket.name}, prefix={prefix}")
            for blob in blobs:
                if blob.name.lower().endswith('.pkl'):
                    original = os.path.basename(blob.name)
                    normalized = _normalize_filename_remove_timestamp(original)
                    local_path = os.path.join(target_dir, normalized)
                    blob.download_to_filename(local_path)
                    saved_files.append(local_path)
            if saved_files:
                return saved_files
        except Exception as e:
            add_log(f"Salesforce import: Firebase admin listing failed: {str(e)}")

        # Fallback: use Google Cloud Storage client with explicit key
        try:
            if gcs is None:
                add_log("Salesforce import: google-cloud-storage not available; skipping GCS fallback")
                return saved_files
            creds = None
            if KEY_FILE_PATH and KEY_FILE_PATH.exists():
                creds = service_account.Credentials.from_service_account_file(str(KEY_FILE_PATH.resolve()))
            project_id = os.getenv('GCP_PROJECT') or GCP_PROJECT or (getattr(creds, 'project_id', None) if creds else None)

            gcs_client = gcs.Client(project=project_id, credentials=creds) if creds else gcs.Client(project=project_id)
            bucket_name = constants.get('storage_bucket')
            prefix = f"{user_email}/data/salesforce/"
            try:
                bucket_obj = gcs_client.bucket(bucket_name)
                blobs_iter = gcs_client.list_blobs(bucket_or_name=bucket_obj, prefix=prefix)
                for blob in blobs_iter:
                    if blob.name.lower().endswith('.pkl'):
                        original = os.path.basename(blob.name)
                        normalized = _normalize_filename_remove_timestamp(original)
                        local_path = os.path.join(target_dir, normalized)
                        blob.download_to_filename(local_path)
                        saved_files.append(local_path)
                if saved_files:
                    add_log(f"Salesforce import: downloaded {len(saved_files)} files from bucket {bucket_name}")
            except Exception as be:
                add_log(f"Salesforce import: GCS listing failed for bucket {bucket_name}: {str(be)}")
            return saved_files
        except Exception as ge:
            add_log(f"Salesforce import: GCS fallback failed: {str(ge)}")
            return saved_files
    except Exception as e:
        add_log(f"Salesforce _download_pickle_files_from_firebase error for {user_email}: {str(e)}")
        raise RuntimeError(f"Failed to download pickle files: {str(e)}")


# @salesforce_bp.route('/salesforce/import_user_data', methods=['POST'])
# New endpoint: separate data retrieval using existing credentials
@salesforce_bp.route('/salesforce/get_data', methods=['POST'])
def get_salesforce_data():
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''

        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        session_id = _validate_non_empty_string(data.get('session_id'), 'session_id', min_len=8)

        # Build secret id and sanitized email for storage
        original_user_email = user_email
        sanitized_email_for_secret = original_user_email.replace('@', '_').replace('.', '_')
        sanitized_email_for_storage = sanitized_email_for_secret
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_salesforce"

        # Ensure credentials exist before proceeding
        secret_exists = False
        try:
            project_id = os.getenv('GCP_PROJECT') 
            credentials_obj = None
            if not project_id:
                config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
                service_key_path = None
                try:
                    if os.path.isdir(config_dir):
                        for fname in os.listdir(config_dir):
                            if fname.endswith('.json'):
                                service_key_path = os.path.join(config_dir, fname)
                                break
                except Exception:
                    service_key_path = None
                if service_key_path and os.path.exists(service_key_path):
                    credentials_obj = service_account.Credentials.from_service_account_file(service_key_path)
                    try:
                        with open(service_key_path, 'r', encoding='utf-8') as f:
                            info = json.load(f)
                            project_id = info.get('project_id') or project_id
                    except Exception:
                        pass
            if not project_id:
                raise RuntimeError("GCP project not configured. Set GCP_PROJECT or GOOGLE_CLOUD_PROJECT env var.")
            client = secretmanager.SecretManagerServiceClient(credentials=credentials_obj) if credentials_obj else secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}"
            try:
                client.get_secret(name=secret_name)
                secret_exists = True
            except NotFound:
                secret_exists = False
            except PermissionDenied:
                secret_exists = True
        except Exception:
            secret_exists = False

        if not secret_exists:
            return fail('Credentials not found for user', 404)

        # Trigger cloud function using sanitized email
        fn_url = constants.get('salesforce_cloud_function_url')
        try:
            resp = requests.post(
                fn_url,
                json={
                    'isOAuthLogin': False,
                    'isCredentialLogin': True,
                    'email': user_email
                },
                timeout=360
            )
            cf_message = resp.text
            add_log(f"Salesforce: cloud function triggered for {sanitized_email_for_storage}")
            try:
                cf_json = json.loads(cf_message)
                if cf_json.get('status') == 'success':
                    add_log("Salesforce: data saved to GCP (cloud function reported success)")
            except Exception:
                pass
        except Exception as e:
            cf_message = f"Cloud Function call failed: {str(e)}"
            add_log(f"Salesforce: cloud function failed for {user_email}: {str(e)}")

        # Clear existing input data before processing new Salesforce data
        print(f"Clearing existing input data for session {session_id} before Salesforce credentials data")
        if not _clear_session_input_data(session_id):
            print(f"Warning: Failed to clear some input data for session {session_id}")
            # Continue anyway - don't fail the Salesforce data import
        
        # Prepare local directory and download files
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        input_data_dir = os.path.join(base_dir, 'execution_layer', 'input_data', session_id)
        os.makedirs(input_data_dir, exist_ok=True)
        saved_files = _download_pickle_files_from_firebase(user_email=sanitized_email_for_storage, target_dir=input_data_dir)
        add_log(f"Salesforce: {len(saved_files)} files saved to server for session {session_id}")
        if job_id:
            add_log(job_id, f"Salesforce: imported {len(saved_files)} files into session {session_id}")

        return jsonify({
            'status': 'success',
            'status_code': 200,
            'message': 'Data imported successfully',
            'secret_id': secret_id,
            'cloud_function_response': cf_message,
            'saved_files': [os.path.basename(p) for p in saved_files],
            'target_dir': input_data_dir,
            # 'domain_dictionary_result': domain_result
        }), 200
    except ValueError as ve:
        add_log(f"Salesforce get_salesforce_data validation error: {str(ve)}")
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"Salesforce get_salesforce_data unexpected error: {str(e)}")
        return fail(f'Failed to import data: {str(e)}', 500)

@salesforce_bp.route('/salesforce/validate_credentials', methods=['POST'])
def validate_salesforce_credentials():
    """Test stored credentials by attempting authentication with Salesforce."""
    try:
        data = request.get_json(silent=True) or {}
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        
        # Get stored credentials
        sanitized_email_for_secret = user_email.replace('@', '_').replace('.', '_')
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_salesforce"
        
        try:
            secret_value = get_secret_from_secret_manager(secret_id)
            if not secret_value:
                return fail('No credentials found. Please save credentials first.', 404)
            
            credentials = json.loads(secret_value)
            environment = credentials.get('environment', 'sandbox')
            
            # Test authentication by calling the cloud function
            cloud_function_url = constants.get('salesforce_cloud_function_url')
            # os.getenv('SALESFORCE_CLOUD_FUNCTION_URL', 'https://us-central1-insightbot-467305.cloudfunctions.net/zingworks_salesforce_connector')
            
            test_payload = {
                "isOAuthLogin": False,
                "isCredentialLogin": True,
                "email": user_email,
                "environment": environment,
                "subject_area": "sales", # Always test with 'sales' for now
                "subject_area_name": "Sales",
                "subject_area_description": "Sales data analysis"
            }
            
            # Make a test call to validate credentials
            cf_response = requests.post(cloud_function_url, json=test_payload, timeout=30)
            
            if cf_response.status_code == 200:
                cf_data = cf_response.json()
                if cf_data.get('status') == 'success':
                    return jsonify({
                        'status': 'success',
                        'status_code': 200,
                        'message': 'Credentials are valid and working',
                        'environment': environment
                    }), 200
                else:
                    return jsonify({
                        'status': 'error',
                        'status_code': 401,
                        'message': cf_data.get('message', 'Credentials validation failed'),
                        'cloud_function_error': cf_data
                    }), 401
            else:
                return fail('Failed to validate credentials', cf_response.status_code)
                
        except Exception as e:
            add_log(f"Salesforce validate_credentials error: {str(e)}")
            return fail(f'Failed to validate credentials: {str(e)}', 500)
            
    except ValueError as ve:
        add_log(f"Salesforce validate_credentials validation error: {str(ve)}")
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"Salesforce validate_credentials unexpected error: {str(e)}")
        return fail(f'Failed to validate credentials: {str(e)}', 500)


@salesforce_bp.route('/salesforce/fetch_data_for_subject', methods=['POST'])
def fetch_data_for_subject():
    """Fetch Salesforce data for a specific subject area after OAuth or credentials connection."""
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        session_id = _validate_non_empty_string(data.get('session_id'), 'session_id', min_len=8)
        
        # Get subject area information (required for data fetching)
        subject_area = data.get('subject_area')
        subject_area_name = data.get('subject_area_name')
        subject_area_description = data.get('subject_area_description')
        custom_domain = data.get('custom_domain')
        
        if not subject_area:
            return fail('Subject area is required for data fetching', 400)
        
        # Validate that the subject area is supported (only 'sales' is supported)
        # if subject_area != 'sales':
        #     return fail(f'Subject area "{subject_area}" is not supported. Only "sales" is currently available.', 400)
        
        # Determine authentication method and prepare cloud function payload
        cloud_function_url = constants.get('salesforce_cloud_function_url')
        
        # Check if we have OAuth tokens in session (from OAuth callback)
        oauth_tokens = data.get('oauth_tokens')
        if oauth_tokens and oauth_tokens.get('access_token') and oauth_tokens.get('instance_url'):
            # Use OAuth login
            cloud_function_payload = {
                "isOAuthLogin": True,
                "isCredentialLogin": False,
                "instanceURL": oauth_tokens.get('instance_url'),
                "accessToken": oauth_tokens.get('access_token'),
                "email": user_email,
                "environment": oauth_tokens.get('environment', 'sandbox'),
                "subject_area": subject_area,
                "subject_area_name": subject_area_name,
                "subject_area_description": subject_area_description
            }
            print("Using OAuth login:", cloud_function_payload)
        else:
            # Check if we have stored credentials for this user
            sanitized_email_for_secret = user_email.replace('@', '_').replace('.', '_')
            secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_salesforce"
            data_manager = get_data_manager()
            credential = data_manager.get_credential(user_email, data.get('connection_id', '').strip())
            try:
                # secret_value = get_secret_from_secret_manager(secret_id)
                if credential:
                    credentials = credential
                # Combine password + security_key
                    credentials["password"] = credentials["password"] + credentials["security_key"]
                    # Remove the field
                    credentials.pop("security_key", None)
                    print("credentials:", credentials)
                    # Use credential login
                    cloud_function_payload = {
                        "isOAuthLogin": False,
                        "isCredentialLogin": True,
                        "email": user_email,
                        "environment": credentials.get('environment', 'sandbox'),
                        "subject_area": subject_area,
                        "subject_area_name": subject_area_name,
                        "subject_area_description": subject_area_description,
                        "credentials": credentials
                    }
                    print("Using credential login:", cloud_function_payload)
                else:
                    return fail('No Salesforce credentials found. Please connect to Salesforce first.', 404)
            except Exception as e:
                print("Exception: ",e)
                return fail('No Salesforce credentials found. Please connect to Salesforce first.', 404)
        
        # Call cloud function to fetch data (always fetches Account and Opportunity)
        try:
            print("Calling cloud function to fetch data", cloud_function_url)
            print("Cloud function payload", cloud_function_payload)
            cf_response = requests.post(cloud_function_url, json=cloud_function_payload)
            print("Cloud function response", cf_response)
            if cf_response.status_code == 200:
                cf_data = cf_response.json()
                
                # Download files from Firebase Storage and upload to GCS (source of truth)
                try:
                    # Sanitize email for storage using helper
                    sanitized_email_for_storage = sanitize_email_for_storage(user_email)
                    
                    # Clear existing input data before processing new Salesforce data
                    if not _clear_session_input_data(session_id, user_email):
                        # Proceed even if partial clear; keep non-fatal
                        add_log(f"Salesforce fetch_data_for_subject: Warning - partial clear for session {session_id}")
                    
                    # Upload to GCS bucket (source of truth for Cloud Run mode)
                    bucket_name = constants.get('storage_bucket') or os.getenv('GCS_BUCKET', 'insightbot-dev-474509.firebasestorage.app')
                    if not bucket_name:
                        add_log("Salesforce fetch_data_for_subject error: Storage bucket not configured")
                        return fail('Storage bucket not configured', 500)
                    
                    if gcs is None:
                        add_log("Salesforce fetch_data_for_subject error: Google Cloud Storage library not available")
                        return fail('Google Cloud Storage library not available', 500)
                    
                    # Download files from Firebase Storage to temp directory
                    import tempfile
                    with tempfile.TemporaryDirectory(prefix="tmp_salesforce_fetch_") as temp_dir:
                        # Download pickle files from Firebase Storage
                        downloaded_files = _download_pickle_files_from_firebase(
                            user_email=sanitized_email_for_storage, 
                            target_dir=temp_dir
                        )
                        
                        if not downloaded_files:
                            add_log("Salesforce fetch_data_for_subject error: No files downloaded from Firebase Storage")
                            return fail('No files downloaded from Firebase Storage', 404)
                        
                        # Create GCS client for uploads
                        client = gcs.Client()
                        bucket = client.bucket(bucket_name)
                        
                        # Check for domain_directory.json in Firebase Storage
                        domain_file_path = None
                        try:
                            # Try to download domain_directory.json from Firebase Storage
                            domain_prefix = f"{sanitized_email_for_storage}/data/salesforce/"
                            
                            domain_blob_name = domain_prefix + 'domain_directory.json'
                            domain_blob = bucket.blob(domain_blob_name)
                            if domain_blob.exists(client):
                                domain_file_path = os.path.join(temp_dir, 'domain_directory.json')
                                domain_blob.download_to_filename(domain_file_path)
                                add_log(f"Salesforce fetch_data_for_subject: Found and downloaded domain_directory.json")
                        except Exception as domain_err:
                            add_log(f"Salesforce fetch_data_for_subject: Could not download domain file (non-fatal): {str(domain_err)}")
                            # Continue without domain file - it can be created later
                        base_prefix = f"{sanitized_email_for_storage}/{session_id}/input_data/"
                        
                        uploaded: Dict[str, str] = {}
                        uploaded_filenames: List[str] = []
                        
                        # Prepare upload list
                        uploads: List[Dict[str, str]] = []
                        for local_file_path in downloaded_files:
                            file_path = Path(local_file_path)
                            if file_path.exists() and file_path.is_file():
                                uploads.append({
                                    'local_path': str(file_path),
                                    'blob_name': file_path.name
                                })
                        
                        # Add domain file if found
                        if domain_file_path and os.path.exists(domain_file_path):
                            uploads.append({
                                'local_path': domain_file_path,
                                'blob_name': 'domain_directory.json'
                            })
                        
                        # Upload files with timeout and retry handling (same as copy_sample_data)
                        timeout_seconds = 600  # 10 minutes timeout for large files
                        for item in uploads:
                            file_path = Path(item['local_path'])
                            if not file_path.exists():
                                add_log(f"Salesforce fetch_data_for_subject: Skipping missing file {item['blob_name']}")
                                continue
                            
                            file_size = file_path.stat().st_size
                            blob_name = base_prefix + item['blob_name']
                            blob = bucket.blob(blob_name)
                            
                            # Check if file already exists to avoid duplicate uploads
                            if blob.exists(client):
                                add_log(f"Salesforce fetch_data_for_subject: File {item['blob_name']} already exists in GCS, skipping upload")
                                uploaded[item['blob_name']] = f"gs://{bucket_name}/{blob_name}"
                                uploaded_filenames.append(item['blob_name'])
                                continue
                            
                            # Use resumable upload for files larger than 5MB, with timeout
                            if file_size > 5 * 1024 * 1024:  # 5MB threshold
                                add_log(f"Salesforce fetch_data_for_subject: Uploading large file {item['blob_name']} ({file_size / (1024*1024):.2f} MB) using resumable upload")
                                blob.chunk_size = 8 * 1024 * 1024  # 8MB chunks
                                try:
                                    try:
                                        from google.cloud.storage import retry as storage_retry
                                        blob.upload_from_filename(
                                            item['local_path'],
                                            timeout=timeout_seconds,
                                            retry=storage_retry.DEFAULT_RETRY
                                        )
                                    except (ImportError, AttributeError):
                                        blob.upload_from_filename(
                                            item['local_path'],
                                            timeout=timeout_seconds
                                        )
                                except Exception as upload_err:
                                    add_log(f"Salesforce fetch_data_for_subject: Upload failed for {item['blob_name']}: {str(upload_err)}")
                                    raise
                            else:
                                add_log(f"Salesforce fetch_data_for_subject: Uploading file {item['blob_name']} ({file_size / 1024:.2f} KB)")
                                blob.upload_from_filename(
                                    item['local_path'],
                                    timeout=timeout_seconds
                                )
                            
                            uploaded[item['blob_name']] = f"gs://{bucket_name}/{blob_name}"
                            uploaded_filenames.append(item['blob_name'])
                            add_log(f"Salesforce fetch_data_for_subject: Successfully uploaded {item['blob_name']}")
                        
                        add_log(f"Salesforce fetch_data_for_subject success for session {session_id}: uploaded {len(uploaded_filenames)} files to GCS {base_prefix}")
                        
                        # Get objects for the subject area
                        subject_area_objects = next((area['sObjects'] for area in constants.get('salesforce_subject_areas') if area['id'] == subject_area), [])
                        
                        # Extract preview data from cloud function response
                        preview_data = {}
                        if cf_data.get('results'):
                            for obj_name, obj_result in cf_data['results'].items():
                                if obj_result.get('preview_data'):
                                    filename = f"{obj_name}.pkl"
                                    preview_data[filename] = obj_result['preview_data']
                        
                        return jsonify({
                            'status': 'success',
                            'status_code': 200,
                            'message': f'Data fetched successfully for {subject_area_name} subject area',
                            'cloud_function_response': cf_data,
                            'session_id': session_id,
                            'gcs': uploaded,
                            'files': {'data': [f for f in uploaded_filenames if f.endswith('.pkl')], 'domain': 'domain_directory.json' if 'domain_directory.json' in uploaded_filenames else None},
                            'saved_files': uploaded_filenames,
                            'subject_area': subject_area,
                            'subject_area_name': subject_area_name,
                            'objects_fetched': subject_area_objects,
                            'preview_data': preview_data
                        }), 200
                    
                except Exception as file_error:
                    add_log(f"Salesforce data fetch: Error downloading files: {str(file_error)}")
                    
                    return jsonify({
                        'status': 'error',
                        'status_code': 500,
                        'message': 'Data fetched but file download failed',
                        'cloud_function_response': cf_data,
                        'file_download_error': str(file_error)
                    }), 500
            else:
                return fail(f'{cf_response.text}', cf_response.status_code)
                
        except requests.RequestException as cf_error:
            add_log(f"Salesforce fetch_data_for_subject cloud function error: {str(cf_error)}")
            return fail(f'Failed to call cloud function: {str(cf_error)}', 500)
            
    except ValueError as ve:
        add_log(f"Salesforce fetch_data_for_subject validation error: {str(ve)}")
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"Salesforce fetch_data_for_subject unexpected error: {str(e)}")
        return fail(f'Failed to fetch data: {str(e)}', 500)

