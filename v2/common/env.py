from __future__ import annotations
import os
import threading
from typing import Dict, Any, List
from pathlib import Path
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore


# Global bootstrap state
_bootstrap_lock = threading.Lock()
_initialized = False
_constants: Dict[str, Any] = {}


# Load base env
load_dotenv()
def _normalize_env(value: str) -> str:
    v = (value or '').strip().lower()
    if v in {'local', 'localhost'}:
        return 'local'
    if v in {'dev', 'development'}:
        return 'dev'
    if v in {'prod', 'production'}:
        return 'prod'
    return v or 'local'

ENV = _normalize_env(os.getenv('ENV', 'local'))
GCP_PROJECT = os.getenv('GCP_PROJECT', '')
KEY_FILE_PATH_STR = (os.getenv('KEY_FILE_PATH', '') or '').strip()
KEY_FILE_PATH = Path(KEY_FILE_PATH_STR) if KEY_FILE_PATH_STR else None
USE_SECRET_MANAGER = (os.getenv('USE_SECRET_MANAGER', 'false').strip().lower() in {'1', 'true', 'yes', 'on'})

def _resolve_key_file_path() -> Path | None:
    """
    Resolve service account key file path.
    Priority: KEY_FILE_PATH env var > GOOGLE_APPLICATION_CREDENTIALS > None (use ADC)
    """
    global KEY_FILE_PATH
    
    # First priority: Explicit KEY_FILE_PATH from environment
    if KEY_FILE_PATH and KEY_FILE_PATH.exists():
        return KEY_FILE_PATH
    
    # Second priority: GOOGLE_APPLICATION_CREDENTIALS (standard GCP env var)
    google_app_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '').strip()
    if google_app_creds:
        candidate = Path(google_app_creds)
        if candidate.exists():
            KEY_FILE_PATH = candidate
            return candidate
    
    # No explicit key file - will use Application Default Credentials (ADC)
    return None

def _ensure_firebase_initialized() -> None:
    """Initialize Firebase Admin SDK using ADC or explicit credentials."""
    # Skip firebase init for local since we read constants from file
    if ENV == 'local':
        return
    
    # Check if already initialized
    if getattr(firebase_admin, "_apps", None):
        try:
            if firebase_admin._apps:
                return
        except Exception:
            pass

    try:
        keyfile_path = _resolve_key_file_path()
        
        if keyfile_path:
            # Use explicit key file if provided
            if not keyfile_path.exists():
                raise FileNotFoundError(f"Key file not found at: {keyfile_path}")
            if not keyfile_path.is_file():
                raise ValueError(f"Path exists but is not a file: {keyfile_path}")
            cred = credentials.Certificate(str(keyfile_path.resolve()))
            firebase_admin.initialize_app(cred)
            print(f"Firebase initialized with key file: {keyfile_path}")
        else:
            # Use Application Default Credentials (ADC)
            # This works in GCP environments (Cloud Run, GCE, etc.) or with gcloud auth
            firebase_admin.initialize_app()
            print("Firebase initialized with Application Default Credentials (ADC)")
        
        return
    except Exception as e:
        print(f"Firebase initialization failed: {str(e)}")
        raise


def _load_constants() -> Dict[str, Any]:
    # Local: read from utils/constants.json
    if ENV == 'local':
        try:
            project_root = Path(__file__).resolve().parent.parent
            local_constants_path = project_root / 'utils' / 'constants.json'
            if local_constants_path.exists():
                import json
                with open(local_constants_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
        return {}
    # Dev/Prod: read from Firestore
    try:
        _ensure_firebase_initialized()
        db = firestore.client()
        # Use 'dev' doc for ENV=dev and 'prod' doc for ENV=prod
        doc_id = 'dev' if ENV == 'dev' else 'prod'
        doc = db.collection('constants').document(doc_id).get()
        if doc.exists:
            data = doc.to_dict() or {}
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _set_named_secrets_if_missing(project_id: str, secret_names: List[str]) -> None:
    """Set environment variables from GCP Secret Manager using ADC or explicit credentials."""
    if not secret_names:
        return
    
    # Lazy imports to avoid hard dependency at module import time
    try:
        from google.cloud import secretmanager
        from google.auth import default as google_auth_default
        from google.oauth2 import service_account
    except Exception as e:
        print("Failed to import Secret Manager libraries", str(e))
        return

    try:
        # Try to use explicit key file if available, otherwise use ADC
        keyfile_path = KEY_FILE_PATH
        if keyfile_path and keyfile_path.exists():
            # Use explicit key file
            creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
            client = secretmanager.SecretManagerServiceClient(credentials=creds)
            effective_project = project_id or getattr(creds, 'project_id', None)
        else:
            # Use Application Default Credentials (ADC)
            creds, effective_project_adc = google_auth_default()
            client = secretmanager.SecretManagerServiceClient(credentials=creds)
            effective_project = project_id or effective_project_adc
        
        if not effective_project:
            raise RuntimeError("GCP project not provided and could not be inferred from credentials")
        
        for env_key in secret_names:
            try:
                name = f"projects/{effective_project}/secrets/{env_key}/versions/latest"
                response = client.access_secret_version(request={'name': name})
                value = response.payload.data.decode('utf-8')
                if value is not None:
                    print(f"Setting {env_key} to {value[:5]}{'*' * (len(value) - 5)}")
                    os.environ[env_key] = value
            except Exception as e:
                print(f"Failed to set {env_key} from Secret Manager: {name} — {str(e)}")
                # Ignore and continue; missing or permission errors shouldn't crash startup
                continue
    except Exception as e:
        print(f"Failed to create Secret Manager client: {str(e)}")
        return


def init_env() -> Dict[str, Any]:
    """Initialize env and constants once; returns constants dict."""
    global _initialized, _constants
    if _initialized:
        return _constants
    _constants = _load_constants()
    # Only fetch secrets from Secret Manager if explicitly enabled and not local
    try:
        if ENV != 'local' and USE_SECRET_MANAGER:
            gcp_secrets = _constants.get('gcp_secrets') or []
            if isinstance(gcp_secrets, list) and gcp_secrets:
                _set_named_secrets_if_missing(GCP_PROJECT, [str(s) for s in gcp_secrets])
        else:
            # For local, rely on .env variables already loaded above
            pass
    except Exception:
        pass
    _initialized = True
    return _constants


def get_constants() -> Dict[str, Any]:
    return _constants
