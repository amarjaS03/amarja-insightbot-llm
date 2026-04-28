from flask import Blueprint, request, jsonify, g
from functools import wraps
from logger import add_log
from api_layer.firebase_data_models import get_data_manager, CredentialDocument
from utils.encryption import generate_encryption_key, clear_encryption_cache
from utils.env import KEY_FILE_PATH, GCP_PROJECT
import uuid
from datetime import datetime
from typing import Dict, Any
import os

credentials_bp = Blueprint('credentials', __name__, url_prefix='/credentials')

def fail(msg: str, status: int = 400, error_code: str = None):
    """Return a standardized error response"""
    response = {
        'result': 'fail',
        'status_code': status,
        'message': msg
    }
    if error_code:
        response['error_code'] = error_code
    return jsonify(response), status

def handle_encryption_error(e: Exception) -> tuple:
    """
    Handle encryption-related errors and return user-friendly API responses.
    
    Args:
        e: Exception raised during encryption/decryption
        
    Returns:
        tuple: (jsonify response, status_code)
    """
    error_msg = str(e)
    
    # Check for specific encryption error types
    if 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
        if 'not found' in error_msg.lower() or 'does not exist' in error_msg.lower():
            return fail(
                'Encryption service configuration error: Encryption key not found in Secret Manager. '
                'Please contact your system administrator.',
                503,
                'ENCRYPTION_KEY_NOT_FOUND'
            )
        elif 'permission' in error_msg.lower() or 'access' in error_msg.lower() or 'role' in error_msg.lower():
            return fail(
                'Encryption service configuration error: Insufficient permissions to access encryption key. '
                'Please contact your system administrator.',
                503,
                'ENCRYPTION_KEY_ACCESS_DENIED'
            )
        elif 'key file' in error_msg.lower() or 'service account' in error_msg.lower():
            return fail(
                'Encryption service configuration error: Service account credentials not configured. '
                'Please contact your system administrator.',
                503,
                'ENCRYPTION_SERVICE_ACCOUNT_ERROR'
            )
        else:
            return fail(
                'Encryption service error: Unable to access encryption key from Secret Manager. '
                'Please contact your system administrator.',
                503,
                'ENCRYPTION_SERVICE_ERROR'
            )
    elif 'Invalid encryption key' in error_msg or 'invalid' in error_msg.lower() and 'key' in error_msg.lower():
        return fail(
            'Encryption service error: Invalid encryption key format. '
            'Please contact your system administrator.',
            503,
            'ENCRYPTION_KEY_INVALID'
        )
    elif 'Failed to encrypt' in error_msg or 'Failed to decrypt' in error_msg:
        return fail(
            'Unable to process credentials securely. Please try again or contact support if the issue persists.',
            500,
            'ENCRYPTION_PROCESSING_ERROR'
        )
    else:
        # Generic encryption error
        return fail(
            'Credential encryption service error. Please contact your system administrator.',
            503,
            'ENCRYPTION_ERROR'
        )

def get_current_user_email() -> str:
    """Extract user email from Flask context"""
    user_info = g.get('user', {})
    return user_info.get('email', '').lower()

def admin_required(f):
    """Decorator to require admin role for endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = getattr(g, 'user', None)
        if not user:
            return fail('Authentication required', 401)
        
        if user.get('role') != 'admin':
            return fail('Admin access required', 403)
            
        return f(*args, **kwargs)
    return decorated_function

def _store_string_secret_in_gcp(secret_id: str, secret_value: str) -> None:
    """
    Store a string secret in Google Secret Manager.
    
    Args:
        secret_id: The secret ID/name in Secret Manager
        secret_value: The string value to store
        
    Raises:
        RuntimeError: If secret cannot be stored
    """
    try:
        from google.cloud import secretmanager
        from google.oauth2 import service_account
        from google.api_core.exceptions import AlreadyExists, PermissionDenied
    except ImportError:
        raise RuntimeError(
            "google-cloud-secret-manager is required. "
            "Install it with: pip install google-cloud-secret-manager"
        )
    
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            raise RuntimeError(
                f"Service account key file not found at {keyfile_path}. "
                "Set KEY_FILE_PATH environment variable to your GCP service account key file."
            )
        
        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        effective_project = os.getenv('GCP_PROJECT') or GCP_PROJECT or getattr(creds, 'project_id', None)
        if not effective_project:
            raise RuntimeError(
                "GCP project ID not found. Set GCP_PROJECT environment variable or ensure "
                "service account key file contains project_id."
            )
        
        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        
        parent = f"projects/{effective_project}"
        secret_name = f"projects/{effective_project}/secrets/{secret_id}"
        
        # Create secret if it doesn't exist
        try:
            client.create_secret(
                parent=parent,
                secret_id=secret_id,
                secret={"replication": {"automatic": {}}},
            )
        except AlreadyExists:
            # Secret already exists, that's fine
            pass
        except PermissionDenied as e:
            raise RuntimeError(
                f"Insufficient permissions to create secret '{secret_id}' in Secret Manager. "
                f"Ensure your service account has 'roles/secretmanager.admin' role. Error: {str(e)}"
            )
        
        # Add new version with the secret value
        secret_value_bytes = secret_value.encode('utf-8')
        try:
            client.add_secret_version(
                parent=secret_name,
                payload={"data": secret_value_bytes},
            )
        except PermissionDenied as e:
            raise RuntimeError(
                f"Insufficient permissions to add secret version for '{secret_id}' in Secret Manager. "
                f"Ensure your service account has 'roles/secretmanager.secretAccessor' role. Error: {str(e)}"
            )
    except RuntimeError:
        # Re-raise RuntimeErrors as-is
        raise
    except Exception as e:
        raise RuntimeError(
            f"Failed to store secret '{secret_id}' in Secret Manager: {str(e)}"
        )

# ==================== CREDENTIAL CRUD OPERATIONS ====================

@credentials_bp.route('/<connector_type>/save', methods=['POST'])
def save_credential(connector_type):
    """
    Save a new credential for a connector type
    
    Path: userCollection/{userEmail}/connections/{connectionId}
    """
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if not data:
            return fail('Missing request body', 400)
        
        user_email = data.get('user_email', '').strip().lower()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        connection_name = data.get('connection_name', '').strip()
        credentials_data = data.get('credentials', {})
        
        if not connection_name:
            return fail('connection_name is required', 400)
        
        if not credentials_data:
            return fail('credentials object is required', 400)
        
        # Generate unique connection ID
        connection_id = str(uuid.uuid4())
        
        # Create credential document
        credential = CredentialDocument(
            connection_id=connection_id,
            connector_type=connector_type,
            connection_name=connection_name,
            credentials=credentials_data,
            is_valid=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Check for duplicate connection_name before saving
        data_manager = get_data_manager()
        
        # Check if connection_name already exists for this connector_type
        if data_manager.credential_exists_by_name(user_email, connector_type, connection_name):
            return fail(f'A credential with the name "{connection_name}" already exists for connector type "{connector_type}"', 409)
        
        # Save to Firestore
        try:
            success = data_manager.save_credential(user_email, credential)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error saving credential: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        if success:
            add_log(f"Credential saved: {connector_type}/{connection_id} for {user_email}")
            if job_id:
                add_log(job_id, f"Credential saved for {connector_type} as {connection_name}")
            return jsonify({
                'status': 'success',
                'message': f'{connector_type} credential saved successfully',
                'connection_id': connection_id,
                'connector_type': connector_type
            }), 201
        else:
            return fail('Failed to save credential. Please try again.', 500)
    
    except (RuntimeError, ValueError) as e:
        # Handle encryption errors
        if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
            add_log(f"Encryption error saving credential: {str(e)}")
            return handle_encryption_error(e)
        add_log(f"Error saving credential: {str(e)}")
        return fail(f'Error saving credential: {str(e)}', 500)
    except Exception as e:
        add_log(f"Error saving credential: {str(e)}")
        return fail('An unexpected error occurred while saving the credential. Please try again.', 500)

@credentials_bp.route('/<connector_type>/<connection_id>', methods=['POST'])
def get_credential(connector_type, connection_id):
    """Get a specific credential"""
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if not data:
            return fail('Missing request body', 400)

        user_email = data.get('user_email', '').strip().lower()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        data_manager = get_data_manager()
        try:
            credential = data_manager.get_credential(user_email, connector_type, connection_id)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error retrieving credential: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        if not credential:
            return fail('Credential not found', 404)
        
        response = jsonify({
            'status': 'success',
            'data': {
                'connection_id': credential.connection_id,
                'connector_type': credential.connector_type,
                'connection_name': credential.connection_name,
                'credentials': credential.credentials,
                'updated_at': credential.updated_at.isoformat() if credential.updated_at else None
            }
        }), 200
        if job_id:
            add_log(job_id, f"Credential fetched: {connector_type}/{connection_id}")
        return response
    
    except (RuntimeError, ValueError) as e:
        # Handle encryption errors
        if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
            add_log(f"Encryption error retrieving credential: {str(e)}")
            return handle_encryption_error(e)
        add_log(f"Error retrieving credential: {str(e)}")
        return fail(f'Error retrieving credential: {str(e)}', 500)
    except Exception as e:
        add_log(f"Error retrieving credential: {str(e)}")
        return fail('An unexpected error occurred while retrieving the credential. Please try again.', 500)

@credentials_bp.route('/<connector_type>', methods=['POST'])
def get_connector_credentials(connector_type):
    """Get all credentials for a connector type"""
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if not data:
            return fail('Missing request body', 400)
        
        user_email = data.get('user_email', '').strip().lower()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        data_manager = get_data_manager()
        try:
            credentials = data_manager.get_all_credentials(user_email, connector_type)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error retrieving credentials: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        credentials_list = []
        for cred in credentials:
            credentials_list.append({
                'connection_id': cred.connection_id,
                'connector_type': cred.connector_type,
                'connection_name': cred.connection_name,
                'is_valid': cred.is_valid,
                'last_validated': cred.last_validated.isoformat() if cred.last_validated else None,
                'created_at': cred.created_at.isoformat() if cred.created_at else None
            })
        
        response = jsonify({
            'status': 'success',
            'connector_type': connector_type,
            'credentials': credentials_list,
            'total': len(credentials_list)
        }), 200
        if job_id:
            add_log(job_id, f"Connector credentials listed for {connector_type}")
        return response
    
    except (RuntimeError, ValueError) as e:
        # Handle encryption errors
        if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
            add_log(f"Encryption error retrieving credentials: {str(e)}")
            return handle_encryption_error(e)
        add_log(f"Error retrieving credentials: {str(e)}")
        return fail(f'Error retrieving credentials: {str(e)}', 500)
    except Exception as e:
        add_log(f"Error retrieving credentials: {str(e)}")
        return fail('An unexpected error occurred while retrieving credentials. Please try again.', 500)

@credentials_bp.route('/all', methods=['POST'])
def get_all_credentials():
    """Get all credentials for all connector types"""
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if not data:
            return fail('Missing request body', 400)
        
        user_email = data.get('user_email', '').strip().lower()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        data_manager = get_data_manager()
        try:
            all_credentials = data_manager.get_all_connector_credentials(user_email)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error retrieving all credentials: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        result = {}
        for connector_type, creds in all_credentials.items():
            result[connector_type] = [
                {
                    'connection_id': cred.connection_id,
                    'connection_name': cred.connection_name,
                    'credentials': cred.credentials,
                    'is_valid': cred.is_valid,
                    'last_validated': cred.last_validated.isoformat() if cred.last_validated else None,
                    'created_at': cred.created_at.isoformat() if cred.created_at else None
                }
                for cred in creds
            ]
        
        response = jsonify({
            'status': 'success',
            'credentials': result
        }), 200
        if job_id:
            add_log(job_id, f"All credentials listed for user {user_email}")
        return response
    
    except (RuntimeError, ValueError) as e:
        # Handle encryption errors
        if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
            add_log(f"Encryption error retrieving all credentials: {str(e)}")
            return handle_encryption_error(e)
        add_log(f"Error retrieving all credentials: {str(e)}")
        return fail(f'Error retrieving credentials: {str(e)}', 500)
    except Exception as e:
        add_log(f"Error retrieving all credentials: {str(e)}")
        return fail('An unexpected error occurred while retrieving credentials. Please try again.', 500)

@credentials_bp.route('/<connection_id>/update', methods=['post'])
def update_credential(connection_id):
    """
    Update credential document fields.
    Only the following fields are allowed to be updated:
      - connection_name
      - credentials  (raw dict or already-encrypted string)
      - updated_at   (will be overridden by server to current time)
    """
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        print("data: ",data)
        if not data:
            return fail('Missing request body', 400)
        
        if not isinstance(data, dict):
            return fail('Request body must be a JSON object', 400)
        
        user_email = data.get('user_email', '').strip().lower()
        connection_id = connection_id.strip()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        # Allowed top-level fields to update
        allowed_fields = {
            'connection_name',
            'credentials',
            'updated_at'
        }
        # Build updates from allowed fields only (ignore user_email and any others)
        updates = {k: v for k, v in data.items() if k in allowed_fields}
        
        data_manager = get_data_manager()
        
        print("updates (requested): ", updates)
        # Verify the credential exists
        try:
            credential_exists = data_manager.get_credential(user_email, connection_id)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error retrieving credential for update: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        if not credential_exists:
            return fail('Credential not found', 404)

        # Perform the update (data layer will set updated_at and encrypt credentials if needed)
        try:
            success = data_manager.update_credential(user_email, connection_id, updates)
            print("success: ", success)
        except (RuntimeError, ValueError) as e:
            # Handle encryption errors specifically
            if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
                add_log(f"Encryption error updating credential: {str(e)}")
                return handle_encryption_error(e)
            raise
        
        if success:
            updated_fields = list(updates.keys())
            add_log(
                f"Updated credential fields: {', '.join(updated_fields)}"
            )
            if job_id:
                add_log(job_id, f"Credential updated: {connection_id} fields: {', '.join(updated_fields)}")
            return jsonify({
                'status': 'success',
                'message': 'Credential updated successfully',
                'updated_fields': updated_fields
            }), 200
        else:
            return fail('Failed to update credential. Please try again.', 500)
    
    except (RuntimeError, ValueError) as e:
        # Handle encryption errors
        if 'encryption' in str(e).lower() or 'Secret Manager' in str(e) or 'secret' in str(e).lower():
            add_log(f"Encryption error updating credential: {str(e)}")
            return handle_encryption_error(e)
        add_log(f"Error updating credential: {str(e)}")
        return fail(f'Error updating credential: {str(e)}', 500)
    except Exception as e:
        add_log(f"Error updating credential: {str(e)}")
        return fail('An unexpected error occurred while updating the credential. Please try again.', 500)

@credentials_bp.route('/<connector_type>/<connection_id>/delete', methods=['POST'])
def delete_credential(connector_type, connection_id):
    """Delete a credential"""
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if not data:
            return fail('Missing request body', 400)
        
        user_email = data.get('user_email', '').strip().lower()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        data_manager = get_data_manager()
        print(connector_type, connection_id)
        print(user_email)
        
        success = data_manager.delete_credential(user_email, connector_type, connection_id)
        
        if success:
            add_log(f"Credential deleted: {connector_type}/{connection_id} for {user_email}")
            if job_id:
                add_log(job_id, f"Credential deleted: {connector_type}/{connection_id}")
            return jsonify({
                'status': 'success',
                'message': 'Credential deleted successfully'
            }), 200
        else:
            return fail('Failed to delete credential', 500)
    
    except Exception as e:
        add_log(f"Error deleting credential: {str(e)}")
        return fail(f'Error: {str(e)}', 500)

@credentials_bp.route('/<connector_type>/<connection_id>/validate', methods=['POST'])
def validate_credential(connector_type, connection_id):
    """Validate a credential (mark as validated with current timestamp)"""
    try:
        data = request.get_json()
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if not data:
            return fail('Missing request body', 400)
        
        user_email = data.get('user_email', '').strip().lower()
        if not user_email:
            return fail('user_email is required in request body', 400)
        
        data_manager = get_data_manager()
        success = data_manager.validate_credential(user_email, connector_type, connection_id)
        
        if success:
            add_log(f"Credential validated: {connector_type}/{connection_id} for {user_email}")
            if job_id:
                add_log(job_id, f"Credential validated: {connector_type}/{connection_id}")
            return jsonify({
                'status': 'success',
                'message': 'Credential validated successfully'
            }), 200
        else:
            return fail('Failed to validate credential', 500)
    
    except Exception as e:
        add_log(f"Error validating credential: {str(e)}")
        return fail(f'Error: {str(e)}', 500)

# ==================== ADMIN OPERATIONS ====================

@credentials_bp.route('/admin/create_encryption_key', methods=['POST'])
@admin_required
def create_encryption_key():
    """
    Generate a new encryption key and store it in Google Secret Manager.
    
    Admin-only endpoint. This will:
    1. Generate a new Fernet encryption key
    2. Store it in Google Secret Manager as "credentials-encryption-key"
    
    WARNING: This will overwrite any existing encryption key. 
    Existing encrypted credentials will become unreadable if the key is changed.
    """
    try:
        user_email = get_current_user_email()
        if not user_email:
            return fail('User not authenticated', 401)
        
        # Generate new encryption key
        try:
            encryption_key = generate_encryption_key()
        except Exception as e:
            add_log(f"Error generating encryption key: {str(e)}")
            return fail(
                'Failed to generate encryption key. Please try again.',
                500,
                'ENCRYPTION_KEY_GENERATION_ERROR'
            )
        
        # Store in Google Secret Manager
        secret_name = os.getenv('CREDENTIALS_ENCRYPTION_KEY_SECRET_NAME', 'credentials-encryption-key')
        
        try:
            _store_string_secret_in_gcp(secret_name, encryption_key)
        except RuntimeError as e:
            error_msg = str(e)
            add_log(f"Error storing encryption key in Secret Manager: {error_msg}")
            
            # Provide specific error messages based on the error type
            if 'permission' in error_msg.lower() or 'access' in error_msg.lower() or 'role' in error_msg.lower():
                return fail(
                    'Insufficient permissions to store encryption key in Secret Manager. '
                    'Ensure your service account has the required Secret Manager roles.',
                    403,
                    'SECRET_MANAGER_PERMISSION_ERROR'
                )
            elif 'key file' in error_msg.lower() or 'service account' in error_msg.lower():
                return fail(
                    'Service account credentials not configured. '
                    'Set KEY_FILE_PATH environment variable to your GCP service account key file.',
                    500,
                    'SERVICE_ACCOUNT_ERROR'
                )
            elif 'project' in error_msg.lower():
                return fail(
                    'GCP project ID not found. Set GCP_PROJECT environment variable.',
                    500,
                    'GCP_PROJECT_ERROR'
                )
            else:
                return fail(
                    f'Failed to store encryption key in Secret Manager: {error_msg}',
                    500,
                    'SECRET_STORAGE_ERROR'
                )
        except Exception as e:
            add_log(f"Unexpected error storing encryption key: {str(e)}")
            return fail(
                'An unexpected error occurred while storing the encryption key. Please try again.',
                500,
                'UNEXPECTED_ERROR'
            )
        
        # Invalidate cached Fernet instance so new key is used
        try:
            clear_encryption_cache()
            add_log("Cached encryption key instance cleared - new key will be loaded on next use")
        except Exception as e:
            add_log(f"Warning: Could not clear cached encryption key instance: {str(e)}")
            # Continue anyway - the cache will expire naturally
        
        # Success
        add_log(f"Encryption key created and stored in Secret Manager by admin: {user_email}")
        return jsonify({
            'status': 'success',
            'message': f'Encryption key generated and stored successfully in Secret Manager as "{secret_name}"',
            'secret_name': secret_name,
            'key_length': len(encryption_key),
            'warning': 'Existing encrypted credentials may become unreadable if a different key was previously used. '
                       'The encryption cache has been cleared and the new key will be used for future operations.'
        }), 201
    
    except Exception as e:
        add_log(f"Error creating encryption key: {str(e)}")
        return fail('An unexpected error occurred while creating the encryption key. Please try again.', 500)