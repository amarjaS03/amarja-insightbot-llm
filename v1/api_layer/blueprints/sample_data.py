from flask import Blueprint, jsonify, request, g
from utils.env import init_env
from pathlib import Path
from typing import Any, Dict, List, Tuple
import os
import shutil
from .data import _clear_session_input_data
from utils.sanitize import sanitize_email_for_storage
import pandas as pd
import json
from logger import add_log

# Optional GCS (for Cloud Run mode)
try:
    from google.cloud import storage  # type: ignore
except Exception:
    storage = None


sample_data_bp = Blueprint('sample_data_bp', __name__)

constants = init_env()


def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'Sample data operation failed'}), status

def _get_suggested_questions_for_filename(filename: str) -> Tuple[List[str], List[str]]:
    """
    Returns a pair (deep_questions, simple_questions) for the given sample filename.

    - deep_questions come from 'suggested_questions'
    - simple_questions come from 'suggested_questions_simple', falling back to deep or generic defaults
    """
    fallback = [
        "Show the first 10 rows.",
        "List all column names.",
        "Show summary statistics for numeric columns.",
        "Count rows and columns in the dataset.",
        "Find top 10 frequent values for a key categorical column."
    ]

    try:
        sample_info = constants.get('sample_preload_data_info') or []
        for domain in sample_info:
            datasets = domain.get('datasets') or []
            for ds in datasets:
                if str(ds.get('filename', '')).strip() == str(filename).strip():
                    deep_raw = ds.get('suggested_questions') or []
                    simple_raw = ds.get('suggested_questions_simple') or []

                    deep = [str(q).strip() for q in deep_raw if str(q).strip()]
                    simple = [str(q).strip() for q in simple_raw if str(q).strip()]

                    if not deep:
                        deep = fallback.copy()
                    if not simple:
                        # For safety, fall back to deep if simple questions are not defined
                        simple = deep.copy()
                    return deep, simple
    except Exception:
        pass

    # Safe generic fallbacks
    return fallback.copy(), fallback.copy()


def _suggested_questions_for_filename(filename: str) -> List[str]:
    """Backward-compatible helper returning only the deep (analysis) questions."""
    deep, _ = _get_suggested_questions_for_filename(filename)
    return deep


def _simple_suggested_questions_for_filename(filename: str) -> List[str]:
    """Helper returning the simple QnA-style questions for the given filename."""
    _, simple = _get_suggested_questions_for_filename(filename)
    return simple


@sample_data_bp.route('/get_sample_data_info', methods=['GET'])
def get_sample_data_info():
    try:
        sample_info: List[Dict[str, Any]] = constants.get('sample_preload_data_info') or []
        if not isinstance(sample_info, list):
            return fail('sample_preload_data_info not found', 404)

        # Resolve project root and sample_data path
        project_root = Path(__file__).resolve().parents[2]
        sample_data_dir = project_root / 'sample_data'
        if not sample_data_dir.exists() or not sample_data_dir.is_dir():
            return fail('sample_data directory not found', 404)

        # Collect all .pkl filenames present under sample_data
        existing_pkl_filenames = {p.name for p in sample_data_dir.rglob('*.pkl')}

        # Filter datasets by presence in existing_pkl_filenames
        filtered_domains: List[Dict[str, Any]] = []
        for domain in sample_info:
            datasets = domain.get('datasets') or []
            filtered_datasets = [d for d in datasets if str(d.get('filename', '')).strip() in existing_pkl_filenames]
            if filtered_datasets:
                filtered_domain = {**domain, 'datasets': filtered_datasets}
                filtered_domains.append(filtered_domain)

        return jsonify({'result': 'success', 'status_code': 200, 'data': filtered_domains}), 200
    except Exception as e:
        add_log(f"SAMPLE_DATA get_sample_data_info error: {str(e)}")
        return fail(str(e), 500)


@sample_data_bp.route('/copy_sample_data', methods=['POST'])
def copy_sample_data():
    try:
        data = request.get_json(silent=True) or {}
        session_id = (data.get('session_id') or '').strip()
        filename = (data.get('filename') or '').strip()
        user_email = (g.get('user') or {}).get('email', 'anonymous')

        if not session_id:
            add_log("SAMPLE_DATA copy_sample_data error: Missing field: session_id")
            return fail('Missing field: session_id', 400)
        if not filename:
            add_log("SAMPLE_DATA copy_sample_data error: Missing field: filename")
            return fail('Missing field: filename', 400)

        # Locate the file under sample_data and ensure domain dictionary exists in same folder
        project_root = Path(__file__).resolve().parents[2]
        sample_data_dir = project_root / 'sample_data'
        if not sample_data_dir.exists() or not sample_data_dir.is_dir():
            add_log("SAMPLE_DATA copy_sample_data error: sample_data directory not found")
            return fail('sample_data directory not found', 404)

        candidates = list(sample_data_dir.rglob(filename))
        if not candidates:
            add_log(f"SAMPLE_DATA copy_sample_data error: File not found in sample_data: {filename}")
            return fail(f'File not found in sample_data: {filename}', 404)

        # Prefer first match (filenames are unique per design)
        src_file = candidates[0]
        if not src_file.is_file():
            add_log(f"SAMPLE_DATA copy_sample_data error: Not a file: {filename}")
            return fail(f'Not a file: {filename}', 404)

        src_dir = src_file.parent
        domain_file = src_dir / 'domain_directory.json'
        if not domain_file.exists() or not domain_file.is_file():
            add_log("SAMPLE_DATA copy_sample_data error: Domain dictionary not found beside the file")
            return fail('Domain dictionary not found beside the file', 404)

        # Clear existing input files for the session
        if not _clear_session_input_data(session_id, user_email):
            # Proceed even if partial clear; keep non-fatal
            pass

        # Upload to GCS bucket (source of truth for Cloud Run mode)
        bucket_name = constants.get('storage_bucket') or os.getenv('GCS_BUCKET', 'insightbot-dev-474509.firebasestorage.app')
        if not bucket_name:
            add_log("SAMPLE_DATA copy_sample_data error: Storage bucket not configured")
            return fail('Storage bucket not configured', 500)

        if storage is None:
            add_log("SAMPLE_DATA copy_sample_data error: Google Cloud Storage library not available")
            return fail('Google Cloud Storage library not available', 500)

        uploaded: Dict[str, str] = {}
        try:
            # Create client with timeout configuration
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            user_email_sanitized = sanitize_email_for_storage(user_email)
            base_prefix = f"{user_email_sanitized}/{session_id}/input_data/"

            uploads: List[Dict[str, str]] = [
                {'local_path': str(src_file), 'blob_name': src_file.name},
                {'local_path': str(domain_file), 'blob_name': 'domain_directory.json'},
            ]

            # Upload files with timeout and retry handling
            timeout_seconds = 600  # 10 minutes timeout for large files
            for item in uploads:
                file_path = Path(item['local_path'])
                file_size = file_path.stat().st_size
                blob_name = base_prefix + item['blob_name']
                blob = bucket.blob(blob_name)
                
                # Use resumable upload for files larger than 5MB, with timeout
                if file_size > 5 * 1024 * 1024:  # 5MB threshold
                    add_log(f"SAMPLE_DATA copy_sample_data: Uploading large file {item['blob_name']} ({file_size / (1024*1024):.2f} MB) using resumable upload")
                    # For large files, use resumable upload with chunk size
                    blob.chunk_size = 8 * 1024 * 1024  # 8MB chunks
                    try:
                        # Try with retry if available
                        try:
                            from google.cloud.storage import retry as storage_retry
                            blob.upload_from_filename(
                                item['local_path'],
                                timeout=timeout_seconds,
                                retry=storage_retry.DEFAULT_RETRY
                            )
                        except (ImportError, AttributeError):
                            # Fallback without retry parameter
                            blob.upload_from_filename(
                                item['local_path'],
                                timeout=timeout_seconds
                            )
                    except Exception as upload_err:
                        add_log(f"SAMPLE_DATA copy_sample_data: Upload failed for {item['blob_name']}: {str(upload_err)}")
                        raise
                else:
                    add_log(f"SAMPLE_DATA copy_sample_data: Uploading file {item['blob_name']} ({file_size / 1024:.2f} KB)")
                    blob.upload_from_filename(
                        item['local_path'],
                        timeout=timeout_seconds
                    )
                
                uploaded[item['blob_name']] = f"gs://{bucket_name}/{blob_name}"
                add_log(f"SAMPLE_DATA copy_sample_data: Successfully uploaded {item['blob_name']}")

            add_log(f"SAMPLE_DATA copy_sample_data success for session {session_id}: uploaded to GCS {base_prefix}")
            
            # Deep analysis-style suggested questions
            suggestions = _suggested_questions_for_filename(src_file.name)
            # Simple QnA-style suggested questions
            simple_suggestions = _simple_suggested_questions_for_filename(src_file.name)

            return jsonify({
                'result': 'success',
                'status_code': 200,
                'message': 'Sample data uploaded to cloud storage',
                'gcs': uploaded,
                'files': {'data': src_file.name, 'domain': 'domain_directory.json'},
                'suggested_questions': suggestions,
                'suggested_questions_simple': simple_suggestions
            }), 200

        except Exception as e:
            add_log(f"SAMPLE_DATA copy_sample_data GCS upload error: {str(e)}")
            return fail(f"GCS upload failed: {str(e)}", 500)

    except Exception as e:
        add_log(f"SAMPLE_DATA copy_sample_data error: {str(e)}")
        return fail(str(e), 500)



@sample_data_bp.route('/preview_sample_data_and_domain_dictionary', methods=['POST'])
def preview_sample_data_and_domain_dictionary():
    try:
        data = request.get_json(silent=True) or {}
        filename = (data.get('filename') or '').strip()

        if not filename:
            add_log("SAMPLE_DATA preview_sample_data_and_domain_dictionary error: Missing field: filename")
            return fail('Missing field: filename', 400)

        # Resolve project root and sample_data path
        project_root = Path(__file__).resolve().parents[2]
        sample_data_dir = project_root / 'sample_data'
        if not sample_data_dir.exists() or not sample_data_dir.is_dir():
            add_log("SAMPLE_DATA preview_sample_data_and_domain_dictionary error: sample_data directory not found")
            return fail('sample_data directory not found', 404)

        # Find the requested file under sample_data
        candidates = list(sample_data_dir.rglob(filename))
        if not candidates:
            add_log(f"SAMPLE_DATA preview_sample_data_and_domain_dictionary error: File not found in sample_data: {filename}")
            return fail(f'File not found in sample_data: {filename}', 404)

        src_file = candidates[0]
        # print(src_file)
        if not src_file.is_file():
            add_log(f"SAMPLE_DATA preview_sample_data_and_domain_dictionary error: Not a file: {filename}")
            return fail(f'Not a file: {filename}', 404)

        # Determine domain directory file alongside the data file
        src_dir = src_file.parent
        domain_file = src_dir / 'domain_directory.json'
        if not domain_file.exists() or not domain_file.is_file():
            add_log("SAMPLE_DATA preview_sample_data_and_domain_dictionary error: Domain dictionary not found beside the file")
            return fail('Domain dictionary not found beside the file', 404)

        # Number of rows to preview from constants
        num_rows = constants.get('sample_data_rows')
        if not isinstance(num_rows, int) or num_rows <= 0:
            num_rows = 5

        # Read data and prepare preview with JSON-safe types
        df = pd.read_pickle(str(src_file))
        # print(df.columns)
        df_preview = df.head(num_rows)
        df_preview = df_preview.where(pd.notnull(df_preview), None)
        preview_rows = json.loads(df_preview.to_json(orient='records', date_format='iso'))

        # Read domain dictionary JSON
        with open(domain_file, 'r', encoding='utf-8') as f:
            domain_dict = json.load(f)

        add_log(f"SAMPLE_DATA preview_sample_data_and_domain_dictionary success for {src_file.name}")
        return jsonify({
            'result': 'success',
            'status_code': 200,
            'filename': src_file.name,
            'data': preview_rows,
            'domain': domain_dict
        }), 200
    except Exception as e:
        add_log(f"SAMPLE_DATA preview_sample_data_and_domain_dictionary error: {str(e)}")
        return fail(str(e), 500)


@sample_data_bp.route('/get_sample_qna_suggestions', methods=['GET'])
def get_sample_qna_suggestions():
    try:
        # Either pass ?filename=<pkl> or ?session_id=<uuid> to auto-detect
        filename = (request.args.get('filename') or '').strip()
        session_id = (request.args.get('session_id') or '').strip()
        user_email = (g.get('user') or {}).get('email', 'anonymous')

        project_root = Path(__file__).resolve().parents[2]

        if not filename and session_id:
            # Try GCS first (Cloud Run mode)
            bucket_name = constants.get('storage_bucket') or os.getenv('GCS_BUCKET', 'insightbot-dev-474509.firebasestorage.app')
            if storage is not None and bucket_name:
                try:
                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    user_email_sanitized = sanitize_email_for_storage(user_email)
                    prefix = f"{user_email_sanitized}/{session_id}/input_data/"
                    blobs = list(bucket.list_blobs(prefix=prefix))
                    pkl_files = [b.name.split('/')[-1] for b in blobs if b.name.endswith('.pkl')]
                    if pkl_files:
                        filename = pkl_files[0]
                        add_log(f"SAMPLE_DATA get_sample_qna_suggestions: Found {filename} in GCS for session {session_id}")
                except Exception as e:
                    add_log(f"SAMPLE_DATA get_sample_qna_suggestions GCS check error: {str(e)}")
            
            # Fallback to local disk if not found in GCS
            if not filename:
                input_dir = project_root / 'execution_layer' / 'input_data' / session_id
                if input_dir.exists() and input_dir.is_dir():
                    pkl_files = [p.name for p in input_dir.glob('*.pkl')]
                    if pkl_files:
                        filename = pkl_files[0]
                        add_log(f"SAMPLE_DATA get_sample_qna_suggestions: Found {filename} in local disk for session {session_id}")
            
            if not filename:
                add_log("SAMPLE_DATA get_sample_qna_suggestions error: No .pkl file found in session input_data")
                return fail('No .pkl file found in session input_data', 404)

        if not filename:
            add_log("SAMPLE_DATA get_sample_qna_suggestions error: Provide either session_id or filename")
            return fail('Provide either session_id or filename', 400)

        # Deep analysis-style suggested questions
        deep_suggestions = _suggested_questions_for_filename(filename)
        # Simple QnA-style suggested questions
        simple_suggestions = _simple_suggested_questions_for_filename(filename)
        return jsonify({
            'result': 'success',
            'status_code': 200,
            'filename': filename,
            'suggested_questions': deep_suggestions,
            'suggested_questions_simple': simple_suggestions
        }), 200
    except Exception as e:
        add_log(f"SAMPLE_DATA get_sample_qna_suggestions error: {str(e)}")
        return fail(str(e), 500)
