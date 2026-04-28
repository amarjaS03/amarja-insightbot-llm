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
MODEL_NAME = (os.getenv('MODEL_NAME') or '').strip()
KEY_FILE_PATH_STR = (os.getenv('KEY_FILE_PATH', '') or '').strip()
KEY_FILE_PATH = Path(KEY_FILE_PATH_STR) if KEY_FILE_PATH_STR else None
USE_SECRET_MANAGER = (os.getenv('USE_SECRET_MANAGER', 'false').strip().lower() in {'1', 'true', 'yes', 'on'})

# Common auth/api envs (kept as simple exported constants for easy import across modules)
GOOGLE_CLIENT_ID = (os.getenv('GOOGLE_CLIENT_ID') or '').strip()
GOOGLE_CLIENT_SECRET = (os.getenv('GOOGLE_CLIENT_SECRET') or '').strip()
OPENAI_API_KEY = (os.getenv('OPENAI_API_KEY') or '').strip()
API_VERSION = (os.getenv('API_VERSION') or '').strip()
API_KEY = (os.getenv('API_KEY') or '').strip()

def _resolve_key_file_path() -> Path | None:
    """Resolve service account key file path based on ENV or explicit KEY_FILE_PATH."""
    global KEY_FILE_PATH
    if KEY_FILE_PATH and KEY_FILE_PATH.exists():
        return KEY_FILE_PATH
    # Project root: utils/.. -> project
    project_root = Path(__file__).resolve().parent.parent
    if ENV == 'dev':
        candidate = project_root / 'api_layer' / 'config' / 'key-dev.json'
    elif ENV == 'prod':
        candidate = project_root / 'api_layer' / 'config' / 'key-prod.json'
    else:
        return None
    if candidate.exists():
        KEY_FILE_PATH = candidate
        return candidate
    return None

def _ensure_firebase_initialized() -> None:
    # Skip firebase init for local since we read constants from file
    if ENV == 'local':
        return
    if getattr(firebase_admin, "_apps", None):
        try:
            if firebase_admin._apps:
                return
        except Exception:
            pass

    try:
        keyfile_path = _resolve_key_file_path()
        if not keyfile_path:
            raise ValueError("Service account key file path not resolved for this environment")
        if not keyfile_path.exists():
            raise FileNotFoundError(f"Key file not found at: {keyfile_path}")
        if not keyfile_path.is_file():
            raise ValueError(f"Path exists but is not a file: {keyfile_path}")
        cred = credentials.Certificate(str(keyfile_path.resolve()))
        firebase_admin.initialize_app(cred)
        print("Firebase successfully initialized")
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
    # print(f"Setting named secrets from Secret Manager for project")
    if not secret_names:
        return
    # Lazy imports to avoid hard dependency at module import time
    try:
        from google.cloud import secretmanager
        from google.oauth2 import service_account
    except Exception as e:
        print("Failed to import Secret Manager libraries", str(e))
        return

    # Build credentials strictly from the provided service account keyfile
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            raise RuntimeError("KEY_FILE_PATH is not set or file does not exist; cannot access Secret Manager without ADC")
        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        effective_project = project_id or getattr(creds, 'project_id', None)
        if not effective_project:
            raise RuntimeError("GCP project not provided and could not be inferred from keyfile")
    except Exception as e:
        print("Failed to create Secret Manager client", str(e))
        return
    for env_key in secret_names:
        try:
            # if os.getenv(env_key) and os.getenv(env_key) != 'OPENAI_API_KEY':
            #     continue
            # print(f"Setting {env_key}")
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
