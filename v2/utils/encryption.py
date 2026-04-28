"""
Encryption utility for encrypting credentials before storing in Firestore.

Production-grade implementation using Fernet (AES-128 in CBC mode) symmetric encryption.
The encryption key MUST be stored in Google Secret Manager (secret name: "credentials-encryption-key").

Setup:
1. Generate a new encryption key:
    from v2.utils.encryption import generate_encryption_key
    print(generate_encryption_key())
    
2. Store it in Google Secret Manager:
    echo "<generated_key>" | gcloud secrets create credentials-encryption-key --data-file=-
    
3. Ensure your service account has Secret Manager Secret Accessor role:
    gcloud secrets add-iam-policy-binding credentials-encryption-key \
        --member="serviceAccount:YOUR_SERVICE_ACCOUNT@PROJECT_ID.iam.gserviceaccount.com" \
        --role="roles/secretmanager.secretAccessor"

The encryption key is retrieved from Secret Manager at runtime and cached in memory.
"""
import os
import json
import base64
from pathlib import Path
from typing import Dict, Any
from cryptography.fernet import Fernet


def generate_encryption_key() -> str:
    """
    Generate a new Fernet encryption key for production use.
    
    This generates a cryptographically secure AES-256 key suitable for storing in Google Secret Manager.
    
    To store in Google Secret Manager:
        echo "<generated_key>" | gcloud secrets create credentials-encryption-key --data-file=-
    
    Returns:
        str: Base64-encoded Fernet key (44 characters) suitable for Secret Manager storage
    """
    key = Fernet.generate_key()
    return key.decode('utf-8')


def _get_secret_from_gcp(secret_id: str) -> str:
    """
    Retrieve a secret from Google Secret Manager.
    
    Production-grade implementation that requires Secret Manager access.
    Raises explicit errors if the secret cannot be retrieved.
    
    Args:
        secret_id: The secret ID/name in Secret Manager
        
    Returns:
        str: The secret value
        
    Raises:
        ImportError: If google-cloud-secret-manager is not installed
        RuntimeError: If secret cannot be retrieved (missing credentials, secret not found, etc.)
    """
    try:
        from google.cloud import secretmanager
        from google.oauth2 import service_account
    except ImportError:
        raise RuntimeError(
            "google-cloud-secret-manager is required for credential encryption. "
            "Install it with: pip install google-cloud-secret-manager"
        )
    
    try:
        from v2.utils.env import KEY_FILE_PATH, GCP_PROJECT
        
        keyfile_path = KEY_FILE_PATH
        # Resolve relative paths from project root if not found from cwd
        if keyfile_path and not keyfile_path.exists():
            project_root = Path(__file__).resolve().parents[2]  # v2/utils/encryption.py -> project root
            candidate = project_root / str(keyfile_path)
            if candidate.exists():
                keyfile_path = candidate
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
        secret_name = f"projects/{effective_project}/secrets/{secret_id}/versions/latest"
        
        try:
            response = client.access_secret_version(request={"name": secret_name})
            secret_value = response.payload.data.decode("UTF-8")
            return secret_value.strip()
        except Exception as secret_error:
            raise RuntimeError(
                f"Failed to retrieve secret '{secret_id}' from Secret Manager: {str(secret_error)}. "
                f"Ensure the secret exists and your service account has 'roles/secretmanager.secretAccessor' role. "
                f"Secret path: {secret_name}"
            )
    except RuntimeError:
        # Re-raise RuntimeErrors as-is
        raise
    except Exception as e:
        raise RuntimeError(
            f"Unexpected error retrieving secret '{secret_id}' from Secret Manager: {str(e)}"
        )


def _get_encryption_key() -> bytes:
    """
    Get encryption key from Google Secret Manager (production-only).
    
    This is a production-grade implementation that requires the encryption key
    to be stored in Google Secret Manager. No fallbacks are provided for security.
    
    Returns:
        bytes: Fernet encryption key
        
    Raises:
        RuntimeError: If the encryption key cannot be retrieved from Secret Manager
        ValueError: If the retrieved key is not a valid Fernet key format
    """
    # Get secret name from environment or use default
    secret_name = os.getenv('CREDENTIALS_ENCRYPTION_KEY_SECRET_NAME', 'credentials-encryption-key')
    
    # Retrieve from Secret Manager (will raise RuntimeError if fails)
    secret_key = _get_secret_from_gcp(secret_name)
    
    if not secret_key or not secret_key.strip():
        raise RuntimeError(
            f"Encryption key retrieved from Secret Manager '{secret_name}' is empty. "
            "Ensure the secret contains a valid Fernet key."
        )
    
    # Validate it's a valid Fernet key format
    try:
        Fernet(secret_key.encode())
        return secret_key.encode()
    except Exception as e:
        raise ValueError(
            f"Invalid encryption key format retrieved from Secret Manager '{secret_name}': {str(e)}. "
            "The key must be a valid base64-encoded Fernet key (44 characters). "
            "Generate a new key using: from v2.utils.encryption import generate_encryption_key"
        )


# Cache the Fernet instance
_fernet_instance: Fernet | None = None


def clear_encryption_cache():
    """
    Clear the cached Fernet instance.
    
    This forces the system to reload the encryption key from Secret Manager
    on the next encryption/decryption operation.
    """
    global _fernet_instance
    _fernet_instance = None


def _get_fernet() -> Fernet:
    """Get or create Fernet instance (cached)"""
    global _fernet_instance
    if _fernet_instance is None:
        key = _get_encryption_key()
        _fernet_instance = Fernet(key)
    return _fernet_instance


def encrypt_credentials(credentials: Dict[str, Any]) -> str:
    """
    Encrypt credentials dictionary to a base64-encoded string.
    
    Args:
        credentials: Dictionary containing credential data
        
    Returns:
        str: Base64-encoded encrypted string
        
    Raises:
        Exception: If encryption fails
    """
    try:
        # Convert credentials dict to JSON string
        credentials_json = json.dumps(credentials, ensure_ascii=False)
        credentials_bytes = credentials_json.encode('utf-8')
        
        # Encrypt using Fernet
        fernet = _get_fernet()
        encrypted_bytes = fernet.encrypt(credentials_bytes)
        
        # Return as base64-encoded string for safe storage in Firestore
        return base64.urlsafe_b64encode(encrypted_bytes).decode('utf-8')
        
    except Exception as e:
        print(f"❌ Failed to encrypt credentials: {str(e)}")
        raise


def decrypt_credentials(encrypted_credentials: str) -> Dict[str, Any]:
    """
    Decrypt base64-encoded encrypted credentials string to dictionary.
    
    Args:
        encrypted_credentials: Base64-encoded encrypted string from Firestore
        
    Returns:
        Dict[str, Any]: Decrypted credentials dictionary
        
    Raises:
        Exception: If decryption fails
    """
    try:
        # Decode from base64
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_credentials.encode('utf-8'))
        
        # Decrypt using Fernet
        fernet = _get_fernet()
        decrypted_bytes = fernet.decrypt(encrypted_bytes)
        
        # Convert JSON string back to dictionary
        credentials_json = decrypted_bytes.decode('utf-8')
        credentials = json.loads(credentials_json)
        
        return credentials
        
    except Exception as e:
        print(f"❌ Failed to decrypt credentials: {str(e)}")
        raise


def is_encrypted(credentials_data: Any) -> bool:
    """
    Check if credentials data is already encrypted.
    
    Args:
        credentials_data: Credentials data (can be dict or encrypted string)
        
    Returns:
        bool: True if data appears to be encrypted, False otherwise
    """
    # If it's a string and looks like base64-encoded encrypted data, assume it's encrypted
    if isinstance(credentials_data, str):
        # Encrypted data is base64-encoded and typically longer than plain JSON
        # Simple heuristic: if it's a string and not a valid JSON object, it's likely encrypted
        try:
            json.loads(credentials_data)
            return False  # Valid JSON, probably not encrypted
        except (json.JSONDecodeError, ValueError):
            # Not valid JSON, might be encrypted
            # Additional check: encrypted strings are base64 and typically longer
            if len(credentials_data) > 50:  # Encrypted data is usually longer
                try:
                    base64.urlsafe_b64decode(credentials_data.encode('utf-8'))
                    return True  # Valid base64, likely encrypted
                except Exception:
                    pass
            return False
    
    # If it's a dict, it's not encrypted
    if isinstance(credentials_data, dict):
        return False
    
    return False

