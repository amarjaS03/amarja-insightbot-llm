from flask import Blueprint, request, jsonify
import os
import math
import numbers
from typing import Dict, List, Any
import pandas as pd
import tempfile
from utils.sanitize import sanitize_email_for_storage

# Optional GCS (source of truth for session input data)
try:
    from google.cloud import storage  # type: ignore
except Exception:
    storage = None


pseudonymize_bp = Blueprint('pseudonymize_bp', __name__)


# Character mapping for pseudonymization
MAPPER: Dict[str, str] = {
    'A': 'Q', 'B': 'W', 'C': 'E', 'D': 'R', 'E': 'T',
    'F': 'Y', 'G': 'U', 'H': 'I', 'I': 'O', 'J': 'P',
    'K': 'A', 'L': 'S', 'M': 'D', 'N': 'F', 'O': 'G',
    'P': 'H', 'Q': 'J', 'R': 'K', 'S': 'L', 'T': 'Z',
    'U': 'X', 'V': 'C', 'W': 'V', 'X': 'B', 'Y': 'N', 'Z': 'M',
    '0': '5', '1': '6', '2': '7', '3': '8', '4': '9',
    '5': '0', '6': '1', '7': '2', '8': '3', '9': '4',
    ' ': '_', '"': '!', "'": '@',
}

# Build a translation table that also includes lowercase keys for A-Z mappings for robustness.
# (We still uppercase string inputs per requirements; unmapped characters remain unchanged.)
_MAPPER_EXTENDED: Dict[str, str] = {
    **MAPPER,
    **{k.lower(): v for k, v in MAPPER.items() if isinstance(k, str) and len(k) == 1 and k.isalpha()},
}
_TRANSLATION_TABLE = str.maketrans(_MAPPER_EXTENDED)


def _pseudonymize_value(value: Any) -> Any:
    if value is None:
        return None
    # Preserve pandas nulls (NaN/NA) as-is instead of turning them into "NAN" -> mapped letters.
    try:
        if pd.isna(value):
            return value
    except Exception:
        pass
    # Avoid mutating booleans (bool is a subclass of int).
    if isinstance(value, bool):
        return value

    def _translate_text(s: str) -> str:
        # Normalize casing first per requirements, then translate.
        s2 = s.upper()
        if s2 == "":
            return s2
        return s2.translate(_TRANSLATION_TABLE)

    # Strings: uppercase then translate.
    if isinstance(value, str):
        return _translate_text(value)

    # Integers (including numpy ints): translate digits and cast back to int when possible.
    if isinstance(value, numbers.Integral):
        mapped = _translate_text(str(value))
        try:
            return int(mapped)
        except Exception:
            # Fallback: if casting fails for any reason, return the mapped representation.
            return mapped

    # Floats (including numpy floats): translate digits in repr and cast back to float when possible.
    if isinstance(value, numbers.Real):
        try:
            if not math.isfinite(float(value)):
                return value
        except Exception:
            pass
        mapped = _translate_text(repr(value))
        try:
            return float(mapped)
        except Exception:
            return mapped

    # Decimal, timestamps, datetimes, and other objects: translate their string form.
    # We do not attempt to reconstruct the original type because translation can make the
    # value invalid for that type (e.g., dates). Returning a safe string avoids crashes.
    try:
        return _translate_text(str(value))
    except Exception:
        return value


def pseudonymize_apply(session_id: str, columns_map: Dict[str, List[str]]) -> Dict[str, Any]:
    """Apply pseudonymization to specified columns across files for a session.

    Returns a dict with keys: updated_files, preview_data, warnings.
    """
    # Source of truth: GCS input_data/<pkl> under <user>/<session_id>/
    # Best-effort: only works when google-cloud-storage is available and a bucket is configured.
    if storage is None:
        raise RuntimeError("google-cloud-storage not available; cannot pseudonymize in bucket-only mode")
    try:
        from flask import current_app, g
        constants = (getattr(current_app, "config", {}) or {}).get("CONSTANTS", {}) or {}
        bucket_name = (os.getenv("GCS_BUCKET") or constants.get("storage_bucket") or "").strip()
    except Exception:
        bucket_name = (os.getenv("GCS_BUCKET") or "").strip()
    if not bucket_name:
        raise RuntimeError("GCS_BUCKET not configured; cannot pseudonymize in bucket-only mode")
    try:
        from flask import g as flask_g
        token_payload = flask_g.get('user', {}) or {}
        user_email = (token_payload.get('email') or '').lower()
    except Exception:
        user_email = ""
    if not user_email:
        raise RuntimeError("User email not available; cannot pseudonymize user-scoped session files")
    user_email_sanitized = sanitize_email_for_storage(user_email)
    prefix_root = f"{user_email_sanitized}/{session_id}/input_data/"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    previews: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    updated_files: List[str] = []
    warnings: List[str] = []

    for file_key, columns in columns_map.items():
        if not isinstance(columns, list):
            warnings.append(f"Skipping {file_key}: columns must be a list")
            continue

        key = str(file_key).strip()
        if key.lower().endswith('.pkl'):
            pkl_filename = key
        elif key.lower().endswith('.csv'):
            pkl_filename = os.path.splitext(key)[0] + '.pkl'
        else:
            pkl_filename = key + '.pkl'

        blob_name = f"{prefix_root}{pkl_filename}"
        with tempfile.TemporaryDirectory(prefix="tmp_pseudo_") as tdir:
            pkl_path = os.path.join(tdir, pkl_filename)
            blob = bucket.blob(blob_name)
            if not blob.exists(client):
                raise FileNotFoundError(f"File not found in bucket: {pkl_filename}. Validate/upload first.")
            blob.download_to_filename(pkl_path)
            df = pd.read_pickle(pkl_path)

        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            warnings.append(f"{pkl_filename}: missing columns {missing_cols}")

        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(_pseudonymize_value)

            df.to_pickle(pkl_path)
            # Upload back (overwrite)
            blob.upload_from_filename(pkl_path)
            updated_files.append(pkl_filename)

        top_records: List[Dict[str, str]] = []
        try:
            preview = df.head(10).to_dict('records')
            for rec in preview:
                str_rec = {str(k): ("null" if v is None else str(v)) for k, v in rec.items()}
                top_records.append(str_rec)
        except Exception:
            pass

            previews[pkl_filename] = {"top_records": top_records}

    return {
        'updated_files': updated_files,
        'preview_data': previews,
        'warnings': warnings
    }


@pseudonymize_bp.route('/pseudonymize/pseudonymize_columns', methods=['POST'])
def pseudonymize_columns():
    try:
        data = request.get_json(silent=True) or {}
        session_id = (data.get('session_id') or '').strip()
        columns_map = data.get('columns_map') or data.get('files') or data.get('map') or {}

        if not session_id:
            return jsonify({'error': 'Missing session_id'}), 400
        if not isinstance(columns_map, dict) or not columns_map:
            return jsonify({'error': 'Missing columns map (file_name -> [columns])'}), 400

        try:
            result = pseudonymize_apply(session_id, columns_map)
        except FileNotFoundError as e:
            return jsonify({'error': str(e)}), 404
        except Exception as e:
            return jsonify({'error': f'Failed to pseudonymize: {str(e)}'}), 500

        response_body: Dict[str, Any] = {
            'status': 'success',
            'status_code': 200,
            'message': 'Pseudonymization applied',
            'session_id': session_id,
            'updated_files': result.get('updated_files', []),
            'preview_data': result.get('preview_data', {})
        }
        if result.get('warnings'):
            response_body['warnings'] = result['warnings']

        return jsonify(response_body), 200
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

