from flask import Blueprint, request, jsonify, g, current_app
import os
import tempfile
from typing import List, Dict, Any
from werkzeug.utils import secure_filename
import json
import re
from dotenv import load_dotenv
from api_layer.blueprints.pseudonymize import pseudonymize_apply
from logger import add_log
from utils.sanitize import sanitize_email_for_storage
from execution_layer.utils.llm_core import CALLER_CONFIG, call_llm_sync

# Optional GCS (for Cloud Run mode)
try:
    from google.cloud import storage  # type: ignore
except Exception:
    storage = None

# Ensure environment variables (e.g., OPENAI_API_KEY) are loaded for this module
load_dotenv()


data_bp = Blueprint('data_bp', __name__)

def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'Data operation failed'}), status

def _get_gcs_bucket():
    """Best-effort: return (client, bucket) if configured; else (None, None)."""
    try:
        if storage is None:
            return None, None
        constants = (getattr(current_app, "config", {}) or {}).get("CONSTANTS", {}) or {}
        bucket_name = (os.getenv("GCS_BUCKET") or constants.get("storage_bucket") or "").strip()
        if not bucket_name:
            return None, None
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        return client, bucket
    except Exception:
        return None, None

def _upload_file_to_gcs(local_path: str, bucket, blob_name: str) -> bool:
    try:
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        return True
    except Exception as e:
        add_log(f"DATA GCS upload error: {str(e)}")
        return False

def _download_file_from_gcs(bucket, blob_name: str, local_path: str) -> bool:
    try:
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return False
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        blob.download_to_filename(local_path)
        return True
    except Exception as e:
        add_log(f"DATA GCS download error: {str(e)}")
        return False

def _delete_prefix_from_gcs(client, bucket, prefix: str) -> bool:
    """Delete all blobs under a prefix (best-effort)."""
    try:
        for b in client.list_blobs(bucket, prefix=prefix):
            try:
                b.delete()
            except Exception:
                pass
        return True
    except Exception as e:
        add_log(f"DATA GCS delete prefix error: {str(e)}")
        return False

def _user_session_prefix(user_email: str, session_id: str) -> str:
    user_email_sanitized = sanitize_email_for_storage(user_email)
    return f"{user_email_sanitized}/{session_id}/"

def _clear_session_input_data(session_id: str, user_email: str | None = None) -> bool:
    """Clear all input data for a session to prepare for new file uploads.

    Prefer clearing GCS user/session prefix when configured; fallback to local disk for legacy.
    """
    try:
        # Prefer GCS when possible (source of truth)
        try:
            token_payload = g.get('user', {}) or {}
            email = (user_email or (token_payload.get('email') or '')).lower()
            client, bucket = _get_gcs_bucket()
            if email and client is not None and bucket is not None:
                prefix = _user_session_prefix(email, session_id) + "input_data/"
                return _delete_prefix_from_gcs(client, bucket, prefix)
        except Exception:
            pass

        # Legacy fallback: local disk cleanup
        input_data_dir = os.path.join('execution_layer', 'input_data', session_id)
        if not os.path.exists(input_data_dir):
            return True  # Directory doesn't exist, nothing to clear
        
        # List all files in the input directory
        files_to_remove = []
        for filename in os.listdir(input_data_dir):
            file_path = os.path.join(input_data_dir, filename)
            if os.path.isfile(file_path):
                files_to_remove.append(file_path)
        
        # Remove all files
        for file_path in files_to_remove:
            try:
                os.remove(file_path)
                print(f"Cleared input file: {file_path}")
            except Exception as e:
                add_log(f"DATA _clear_session_input_data error: Failed to remove file {file_path}: {str(e)}")
                return False
        
        print(f"Successfully cleared {len(files_to_remove)} input files for session {session_id}")
        return True
        
    except Exception as e:
        add_log(f"DATA _clear_session_input_data error: Failed to clear input data for session {session_id}: {str(e)}")
        print(f"Error clearing input data for session {session_id}: {str(e)}")
        return False


@data_bp.route('/validate_data', methods=['POST'])
def validate_data():
    try:
        allowed_ext = {'.csv', '.xlsx', '.xls'}

        def fail(msg: str, status: int = 400):
            try:
                add_log(f"DATA validate_data error: {msg}")
            except Exception:
                pass
            return jsonify({'valid': False, 'status_code': status, 'message': msg, 'error': 'validation failed'})

        if 'file' not in request.files:
            return fail('Missing file in request', 400)

        session_id = request.form.get('session_id')
        if not session_id:
            return fail('Missing session_id in request', 400)

        # Clear existing input data before processing new file
        print(f"Clearing existing input data for session {session_id} before new file upload")
        if not _clear_session_input_data(session_id):
            print(f"Warning: Failed to clear some input data for session {session_id}")
            # Continue anyway - don't fail the upload

        f = request.files['file']
        filename = f.filename or ''
        if not filename:
            return fail('Empty filename', 400)

        # Removed content-length size check per new requirements

        ext = '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        if ext not in allowed_ext:
            return fail('Invalid file type. Only csv and excel files are allowed.', 415)

        safe_name = secure_filename(filename)
        with tempfile.NamedTemporaryFile(prefix='upload_', suffix=ext, delete=False) as tmp:
            tmp_path = tmp.name
            f.save(tmp)

        # Removed file size check after saving temp file per new requirements

        import pandas as pd

        errors: List[str] = []
        warnings: List[str] = []
        info: Dict[str, Any] = {}

        try:
            if ext == '.csv':
                df = pd.read_csv(tmp_path, header=0, encoding_errors='replace')
            else:
                try:
                    df = pd.read_excel(tmp_path, header=0, engine='openpyxl')
                except Exception:
                    df = pd.read_excel(tmp_path, header=0)
        except UnicodeDecodeError as e:
            os.remove(tmp_path)
            return fail(f'Encoding error: {str(e)}', 422)
        except ValueError as e:
            os.remove(tmp_path)
            return fail(f'Value error while reading file: {str(e)}', 422)
        except Exception as e:
            add_log(f"DATA validate_data error: Failed to read file: {str(e)}")
            os.remove(tmp_path)
            return fail(f'Failed to read file: {str(e)}', 422)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

        col_names = [str(c) for c in df.columns.tolist()]

        if any((c.strip() == '' or c.lower().startswith('unnamed')) for c in col_names):
            errors.append('Header row has empty or Unnamed columns. Ensure the first row contains proper column names.')

        lowered = [c.lower() for c in col_names]
        if len(set(lowered)) != len(lowered):
            errors.append('Duplicate column names detected (case-insensitive). Column names must be unique.')

        def is_numeric_like(s: str) -> bool:
            try:
                float(s.replace(',', ''))
                return True
            except Exception:
                return False

        numeric_like_count = sum(1 for c in col_names if is_numeric_like(c))
        if df.shape[1] > 0 and numeric_like_count >= max(1, int(0.6 * df.shape[1])):
            errors.append('First row appears to be data, not headers. Please include a header row as the first line.')

        rows, cols = df.shape
        # Removed columns/rows limit checks per new requirements

        base64_regex = re.compile(r'^[A-Za-z0-9+/=\r\n]+$')

        def looks_like_json(text: str) -> bool:
            text = text.strip()
            if not text or (text[0] not in '{['):
                return False
            if len(text) > 20000:
                return True
            try:
                json.loads(text)
                return True
            except Exception:
                return False

        def looks_like_base64_blob(text: str) -> bool:
            if len(text) < 1000:
                return False
            compact = ''.join(ch for ch in text if not ch.isspace())
            if not base64_regex.match(compact):
                return False
            if len(compact) > 5000:
                return True
            return False

        try:
            object_cols = [c for c in df.columns if str(df[c].dtype) == 'object']
            sample_size = min(int(rows), 1000)
            for col in object_cols:
                series = df[col].dropna().astype(str).head(sample_size)
                if series.empty:
                    continue
                very_long = (series.str.len() > 50000).any()
                json_like_ratio = series.apply(looks_like_json).mean() if len(series) > 0 else 0
                b64_like_ratio = series.apply(looks_like_base64_blob).mean() if len(series) > 0 else 0
                if very_long or json_like_ratio >= 0.2 or b64_like_ratio >= 0.2:
                    errors.append(f"Column '{col}' appears to contain non-text payloads (JSON/blob/image/base64). These are not allowed.")
        except Exception:
            pass

        valid = len(errors) == 0

        if valid:
            try:
                base_filename = filename.rsplit('.', 1)[0] if '.' in filename else filename
                pkl_filename = f"{base_filename}.pkl"
                # Source of truth: GCS (use temp file locally, then upload)
                token_payload = g.get('user', {}) or {}
                user_email = (token_payload.get('email') or '').lower()
                client, bucket = _get_gcs_bucket()
                if not user_email or bucket is None:
                    return fail('GCS not configured or user not authenticated; cannot save dataset', 500)
                gcs_key = _user_session_prefix(user_email, session_id) + f"input_data/{pkl_filename}"

                with tempfile.TemporaryDirectory(prefix="tmp_upload_pkl_") as tdir:
                    pkl_path = os.path.join(tdir, pkl_filename)
                    df.to_pickle(pkl_path)
                    if not _upload_file_to_gcs(pkl_path, bucket, gcs_key):
                        return fail('Failed to upload dataset to bucket', 500)
                
                # Generate preview data with top 10 records (similar to Salesforce implementation)
                preview_data = {
                    "top_records": []
                }
                
                # Convert top records to strings
                top_records = df.head(10).to_dict('records')
                for record in top_records:
                    string_record = {}
                    for key, value in record.items():
                        if value is None:
                            string_record[str(key)] = "null"
                        elif isinstance(value, (dict, list)):
                            string_record[str(key)] = str(value)
                        else:
                            string_record[str(key)] = str(value)
                    preview_data["top_records"].append(string_record)
                
                return jsonify({
                    'valid': True, 
                    'message': 'validation successful', 
                    'saved_file': pkl_filename,
                    'gcs_path': gcs_key,
                    'preview_data': preview_data,
                }), 200
            except Exception as e:
                add_log(f"DATA validate_data error: Failed to save file: {str(e)}")
                return jsonify({'valid': False, 'message': 'validation failed', 'error': f'Failed to save file: {str(e)}'}), 500
        else:
            primary_error = errors[0] if errors else 'Validation failed'
            return fail(primary_error, 400)
    except Exception as e:
        import traceback
        traceback.print_exc()
        add_log(f"DATA validate_data unexpected error: {str(e)}")
        return jsonify({'valid': False, 'message': 'validation failed', 'error': f'Unexpected error during validation: {str(e)}'}), 500


@data_bp.route('/generate_domain_dictionary', methods=['POST'])
def generate_domain_dictionary():
    try:
        def fail(msg: str, status: int = 400):
            try:
                add_log(f"DATA generate_domain_dictionary error: {msg}")
            except Exception:
                pass
            return jsonify({'valid': False, 'status_code': status, 'message': msg, 'error': 'Domain generation failed'})

        data = request.get_json()
        if not data:
            return fail('Missing request data', 400)

        # Inputs (support both legacy and new payload shapes)
        domain_desc = (data.get('domain') or '').strip()
        session_id = (data.get('session_id') or '').strip()
        filename_single = (data.get('filename') or '').strip()
        filenames_list = data.get('filenames') or []
        file_info_single = (data.get('file_info') or '').strip()
        file_info_map = data.get('file_info_map') or {}
        underlying_csv = (data.get('underlying_conditions_about_dataset') or '').strip()
        existing_dict = data.get('existing_domain_dictionary') or None

        # New shape: data_set_files mapping. session_id must be at root
        data_set_files_payload = data.get('data_set_files') if isinstance(data.get('data_set_files'), dict) else None

        if not session_id:
            return fail('Missing field: session_id', 400)
        if not domain_desc and not existing_dict:
            return fail('Missing field: domain', 400)

        # Determine files to process
        filenames: List[str] = []
        underlying_map: Dict[str, List[str]] = {}
        pseudonymized_cols_map: Dict[str, List[str]] = {}

        if data_set_files_payload:
            # Extract filenames from keys
            for key, val in data_set_files_payload.items():
                if not isinstance(val, dict):
                    continue
                filenames.append(key)
                # Per-file file_info and underlying conditions
                finfo = (val.get('file_info') or '').strip()
                if finfo:
                    file_info_map[key] = finfo
                ulist = val.get('underlying_conditions_about_dataset')
                if isinstance(ulist, list):
                    underlying_map[key] = [str(x).strip() for x in ulist if str(x).strip()]
                # Per-file pseudonymized columns (aka anom_cols)
                anom_cols = val.get('anom_cols')
                if isinstance(anom_cols, list):
                    cleaned = []
                    for c in anom_cols:
                        cs = str(c).strip()
                        if cs:
                            cleaned.append(cs)
                    # de-dup while preserving order
                    seen = set()
                    pseudonymized_cols_map[key] = [x for x in cleaned if not (x in seen or seen.add(x))]
        elif isinstance(filenames_list, list) and filenames_list:
            filenames = [str(x).strip() for x in filenames_list if str(x).strip()]
        elif filename_single:
            filenames = [filename_single]
        else:
            return fail('Missing field: filename/filenames or data_set_files', 400)

        # Prepare existing dictionary (from request or GCS)
        if existing_dict is None:
            try:
                token_payload = g.get('user', {}) or {}
                user_email = (token_payload.get('email') or '').lower()
                client_gcs, bucket = _get_gcs_bucket()
                if user_email and bucket is not None:
                    prefix = _user_session_prefix(user_email, session_id) + "input_data/"
                    with tempfile.TemporaryDirectory(prefix="tmp_domain_load_") as tdir:
                        fname = "domain_directory.json"
                        tmp = os.path.join(tdir, fname)
                        if _download_file_from_gcs(bucket, prefix + fname, tmp):
                            try:
                                with open(tmp, 'r', encoding='utf-8') as f:
                                    existing_dict = json.load(f)
                            except Exception:
                                existing_dict = None
            except Exception:
                existing_dict = None

        # Initialize result structure
        if existing_dict and isinstance(existing_dict, dict):
            result = existing_dict
            if 'data_set_files' not in result or not isinstance(result['data_set_files'], dict):
                result['data_set_files'] = {}
            if 'domain' not in result and domain_desc:
                result['domain'] = domain_desc
        else:
            result = {
                'domain': domain_desc,
                'data_set_files': {}
            }

        # Utility to parse JSON robustly
        def parse_json_strict_or_fallback(text: str) -> Dict[str, Any]:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                fenced_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
                candidate = None
                if fenced_match:
                    candidate = fenced_match.group(1).strip()
                else:
                    start = text.find('{')
                    end = text.rfind('}')
                    if start != -1 and end != -1 and end > start:
                        candidate = text[start:end+1]
                if candidate is None:
                    raise json.JSONDecodeError("No JSON object found in response content", text, 0)
                return json.loads(candidate)

        # Iterate files
        # Create a persistent temp directory for all files (will be cleaned up at end)
        import tempfile
        import shutil
        persistent_temp_dir = tempfile.mkdtemp(prefix="tmp_domain_all_")
        files_to_save_back: List[Dict[str, Any]] = []

        # Helper function to pseudonymize locally (without uploading to GCS)
        def _pseudonymize_dataframe_locally(df, columns_to_pseudonymize):
            """Pseudonymize specified columns in a DataFrame locally."""
            from api_layer.blueprints.pseudonymize import _pseudonymize_value
            df_copy = df.copy()
            for col in columns_to_pseudonymize:
                if col in df_copy.columns:
                    df_copy[col] = df_copy[col].apply(_pseudonymize_value)
            return df_copy
        
        try:
            for idx, filename in enumerate(filenames):
                base_filename = filename.rsplit('.', 1)[0] if '.' in filename else filename
                pkl_filename = f"{base_filename}.pkl"
                # Source of truth: read dataset from GCS
                token_payload = g.get('user', {}) or {}
                user_email = (token_payload.get('email') or '').lower()
                client_gcs, bucket = _get_gcs_bucket()
                if not user_email or bucket is None:
                    return fail('GCS not configured or user not authenticated; cannot load dataset', 500)
                gcs_key = _user_session_prefix(user_email, session_id) + f"input_data/{pkl_filename}"
                
                # Step 1: Download file from GCS to persistent temp directory
                pkl_path = os.path.join(persistent_temp_dir, pkl_filename)
                if not _download_file_from_gcs(bucket, gcs_key, pkl_path):
                    return fail(f'Saved file not found in bucket: {pkl_filename}. Please validate/upload first.', 404)
                
                try:
                    import pandas as pd
                    df = pd.read_pickle(pkl_path)
                except Exception as e:
                    add_log(f"DATA generate_domain_dictionary error: Failed to load saved file {pkl_filename}: {str(e)}")
                    return fail(f'Failed to load saved file {pkl_filename}: {str(e)}', 500)
                
                # Step 2: Pseudonymize locally if anom_cols provided
                pseudonymized_df = df
                needs_pseudonymization = False
                if data_set_files_payload and isinstance(data_set_files_payload.get(filename), dict):
                    maybe_anom_cols = data_set_files_payload[filename].get('anom_cols')
                    if isinstance(maybe_anom_cols, list) and len(maybe_anom_cols) > 0:
                        try:
                            pseudonymized_df = _pseudonymize_dataframe_locally(df, maybe_anom_cols)
                            needs_pseudonymization = True
                            add_log(f"DATA generate_domain_dictionary: Applied local pseudonymization to {pkl_filename}")
                            
                            # Save pseudonymized DataFrame to persistent temp file for later upload
                            pseudonymized_pkl_path = os.path.join(persistent_temp_dir, f"pseudonymized_{pkl_filename}")
                            pseudonymized_df.to_pickle(pseudonymized_pkl_path)
                            
                            # Track for parallel upload later
                            files_to_save_back.append({
                                'local_path': pseudonymized_pkl_path,
                                'gcs_key': gcs_key,
                                'filename': pkl_filename
                            })
                        except Exception as e:
                            add_log(f"DATA generate_domain_dictionary: Local pseudonymization failed for {pkl_filename}: {str(e)}")
                            # Continue with original DataFrame
                
                # Step 3: Take required info from pseudonymized files to generate domain dictionary
                # Use pseudonymized_df for analysis
                df_for_analysis = pseudonymized_df
                rows, cols = df_for_analysis.shape
                columns_info = []
                for col in df_for_analysis.columns:
                    dtype_str = str(df_for_analysis[col].dtype)
                    unique_values = df_for_analysis[col].dropna().unique()
                    if len(unique_values) > 10:
                        unique_sample = unique_values[:10].tolist()
                        unique_count = len(unique_values)
                    else:
                        unique_sample = unique_values.tolist()
                        unique_count = len(unique_values)
                    sample_values = df_for_analysis[col].dropna().head(3).tolist()
                    columns_info.append({
                        'name': str(col),
                        'dtype': dtype_str,
                        'sample_values': [str(v) for v in sample_values],
                        'unique_values': [str(v) for v in unique_sample],
                        'unique_count': unique_count,
                        'null_count': int(df_for_analysis[col].isnull().sum())
                    })

                # Select file-specific metadata
                this_file_info = (file_info_map.get(filename) or file_info_single or '').strip()
                # Prefer per-file underlying list; fallback to legacy CSV for all files
                if filename in underlying_map:
                    underlying_list = underlying_map[filename]
                else:
                    underlying_list = [c.strip() for c in underlying_csv.split(',') if c.strip()] if underlying_csv else []

                is_first = (len(result.get('data_set_files', {})) == 0)

                if is_first:
                    prompt = f"""You are a senior data analyst. Produce a high-quality, business-friendly domain dictionary from the dataset below.
                        Domain: {domain_desc}
                        File: {filename}
                        File Info: {this_file_info}
                        Shape: {rows} rows, {cols} columns
 
                        Detailed Column Analysis (JSON):
                        {json.dumps(columns_info, indent=2)}
 
                        Business Rules (list): {underlying_list}
 
                        Authoring guidelines (critical):
                        - Write specific, business-meaningful descriptions. Do NOT start with 'Column' or repeat the column name verbatim.
                        - Use evidence from dtype, unique_values, sample_values and null_count to infer meaning.
                        - IDs: state the entity the ID refers to and whether it is unique per row.
                        - Categorical fields: mention representative categories (3–8) from unique_values and what they indicate.
                        - Dates/times: state which event they mark (e.g., creation, pickup, drop), observed format, and timezone if inferable.
                        - Numeric measures: include unit/currency/scale (e.g., km, minutes, INR/USD, 1–5).
                        - If uncertain, use cautious wording like 'appears to' based on data; avoid speculation beyond provided context.
                        - Keep each column description 1–2 sentences.
 
                        Return ONLY a JSON object with EXACTLY this structure:
                        {{
                        "domain": "1–2 sentence dataset-level description using the domain context",
                        "data_set_files": {{
                            "{filename}": {{
                            "description": "1–2 sentence file-level description summarizing the contents and purpose",
                            "columns": [
                                {{"name": "column_name", "description": "precise, context-aware description (no 'Column …')", "dtype": "data_type"}}
                            ],
                            "underlying_conditions_about_dataset": ["rule1", "rule2"]
                            }}
                        }}
                        }}"""
                else:
                    prompt = f"""You are a senior data analyst. Generate ONLY the JSON for this file's dictionary entry.
                        File: {filename}
                        File Info: {this_file_info}
                        Shape: {rows} rows, {cols} columns
 
                        Detailed Column Analysis (JSON):
                        {json.dumps(columns_info, indent=2)}
 
                        Business Rules (list): {underlying_list}
 
                        Authoring guidelines (critical):
                        - Write specific, business-meaningful descriptions. Do NOT start with 'Column' or repeat the column name verbatim.
                        - Use evidence from dtype, unique_values, sample_values and null_count to infer meaning.
                        - IDs: state the entity the ID refers to and whether it is unique per row.
                        - Categorical fields: mention representative categories (3–8) from unique_values and what they indicate.
                        - Dates/times: state which event they mark, observed format, and timezone if inferable.
                        - Numeric measures: include unit/currency/scale; ratings must include scale bounds.
                        - If uncertain, use cautious wording; avoid speculation beyond provided context.
                        - Keep each column description 1–2 sentences.
 
                        Return ONLY a JSON object with EXACTLY this structure:
                        {{
                        "file": "{filename}",
                        "description": "1–2 sentence file-level description summarizing contents and purpose",
                        "columns": [
                            {{"name": "column_name", "description": "precise, context-aware description (no 'Column …')", "dtype": "data_type"}}
                        ],
                        "underlying_conditions_about_dataset": ["rule1", "rule2"]
                        }}"""

                # Step 4: Generate domain dictionary
                content = call_llm_sync(
                    SYS_PROMPT="You are a senior data analyst. Create concise, accurate domain dictionaries.",
                    USER_ANALYSIS_PROMPT="Domain dictionary request",
                    USER_PROMPT=prompt,
                    json_output=True,
                    api_type="chat",
                    temperature=CALLER_CONFIG["V1_DOMAIN_GEN"]["temperature"],
                    max_tokens=CALLER_CONFIG["V1_DOMAIN_GEN"]["max_tokens"],
                )
                if not content:
                    content = "{}"
                ai_json = parse_json_strict_or_fallback(content)

                if is_first:
                    # Expect top-level structure with domain and data_set_files mapping
                    new_domain = ai_json.get('domain') or domain_desc
                    files_map = ai_json.get('data_set_files') or {}
                    # Validate and merge
                    if isinstance(files_map, dict):
                        result['domain'] = new_domain
                        # Ensure each file entry is an object with description/columns/underlying
                        for fname, entry in files_map.items():
                            if isinstance(entry, str):
                                entry_obj = {
                                    'description': entry,
                                    'columns': [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info],
                                    'underlying_conditions_about_dataset': underlying_list
                                }
                            else:
                                entry_obj = entry
                            # Attach pseudonymized columns (anom_cols) per dataset/file.
                            # If request didn't specify, preserve any existing value from disk (if present).
                            if fname in pseudonymized_cols_map:
                                entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(fname, [])
                            elif isinstance(result.get('data_set_files', {}).get(fname), dict) and 'pseudonymized_columns' in result['data_set_files'][fname]:
                                entry_obj['pseudonymized_columns'] = result['data_set_files'][fname].get('pseudonymized_columns')
                            result['data_set_files'][fname] = entry_obj
                    else:
                        # If the model returned a flat file entry by mistake, wrap it
                        entry_obj = ai_json if 'description' in ai_json else {
                            'description': this_file_info,
                            'columns': [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info],
                            'underlying_conditions_about_dataset': underlying_list
                        }
                        if filename in pseudonymized_cols_map:
                            entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(filename, [])
                        elif isinstance(result.get('data_set_files', {}).get(filename), dict) and 'pseudonymized_columns' in result['data_set_files'][filename]:
                            entry_obj['pseudonymized_columns'] = result['data_set_files'][filename].get('pseudonymized_columns')
                        result['data_set_files'][filename] = entry_obj
                else:
                    # Expect only file-specific object
                    if 'file' in ai_json and isinstance(ai_json.get('file'), str):
                        fname_key = ai_json['file']
                        entry_obj = {
                            'description': ai_json.get('description', this_file_info),
                            'columns': ai_json.get('columns', [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info]),
                            'underlying_conditions_about_dataset': ai_json.get('underlying_conditions_about_dataset', underlying_list)
                        }
                        if fname_key in pseudonymized_cols_map:
                            entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(fname_key, [])
                        elif isinstance(result.get('data_set_files', {}).get(fname_key), dict) and 'pseudonymized_columns' in result['data_set_files'][fname_key]:
                            entry_obj['pseudonymized_columns'] = result['data_set_files'][fname_key].get('pseudonymized_columns')
                        result['data_set_files'][fname_key] = entry_obj
                    else:
                        # Fallback: assume ai_json is directly the entry
                        entry_obj = {
                            'description': ai_json.get('description', this_file_info),
                            'columns': ai_json.get('columns', [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info]),
                            'underlying_conditions_about_dataset': ai_json.get('underlying_conditions_about_dataset', underlying_list)
                        }
                        if filename in pseudonymized_cols_map:
                            entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(filename, [])
                        elif isinstance(result.get('data_set_files', {}).get(filename), dict) and 'pseudonymized_columns' in result['data_set_files'][filename]:
                            entry_obj['pseudonymized_columns'] = result['data_set_files'][filename].get('pseudonymized_columns')
                        result['data_set_files'][filename] = entry_obj
            
            # Step 5: Parallel upload pseudonymized files back to GCS (after all files processed)
            if files_to_save_back:
                import threading
                from concurrent.futures import ThreadPoolExecutor, as_completed
                
                def upload_file_to_gcs(file_info):
                    """Upload a single file to GCS."""
                    try:
                        blob = bucket.blob(file_info['gcs_key'])
                        blob.upload_from_filename(file_info['local_path'])
                        add_log(f"DATA generate_domain_dictionary: Uploaded pseudonymized file {file_info['filename']} to GCS")
                        return {'success': True, 'filename': file_info['filename']}
                    except Exception as e:
                        add_log(f"DATA generate_domain_dictionary: Failed to upload {file_info['filename']}: {str(e)}")
                        return {'success': False, 'filename': file_info['filename'], 'error': str(e)}
                
                # Upload files in parallel
                add_log(f"DATA generate_domain_dictionary: Starting parallel upload of {len(files_to_save_back)} pseudonymized files")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    upload_futures = {executor.submit(upload_file_to_gcs, file_info): file_info for file_info in files_to_save_back}
                    upload_results = []
                    for future in as_completed(upload_futures):
                        result_upload = future.result()
                        upload_results.append(result_upload)
                
                successful_uploads = sum(1 for r in upload_results if r.get('success'))
                add_log(f"DATA generate_domain_dictionary: Completed parallel upload: {successful_uploads}/{len(upload_results)} files uploaded successfully")
        
        finally:
            # Clean up persistent temp directory
            try:
                shutil.rmtree(persistent_temp_dir)
                add_log(f"DATA generate_domain_dictionary: Cleaned up temp directory {persistent_temp_dir}")
            except Exception as e:
                add_log(f"DATA generate_domain_dictionary: Warning - failed to clean up temp directory: {str(e)}")

        return jsonify({'message': 'generated', 'domain_dictionary': result}), 200
    except json.JSONDecodeError:
        add_log("DATA generate_domain_dictionary error: Failed to parse LLM response as JSON")
        return jsonify({'error': 'Failed to parse LLM response as JSON'}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        add_log(f"DATA generate_domain_dictionary unexpected error: {str(e)}")
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@data_bp.route('/save_domain_dictionary', methods=['POST'])
def save_domain_dictionary():
    try:
        def fail(msg: str, status: int = 400):
            try:
                add_log(f"DATA save_domain_dictionary error: {msg}")
            except Exception:
                pass
            return jsonify({'valid': False,'status_code': status, 'message': msg, 'error': 'Domain dictionary saving failed'})

        data = request.get_json()
        if not data:
            return fail('Missing request data', 400)
        domain_dictionary = data.get('domain_dictionary')
        if not domain_dictionary:
            return fail('Missing domain_dictionary in request', 400)

        # Source of truth: save to GCS (use temp file only)
        token_payload = g.get('user', {}) or {}
        user_email = (token_payload.get('email') or '').lower()
        session_id = (data.get('session_id') or '').strip()
        client, bucket = _get_gcs_bucket()
        if not user_email or bucket is None or not session_id:
            return jsonify({'error': 'GCS not configured or user not authenticated; cannot save domain dictionary'}), 500

        gcs_key = _user_session_prefix(user_email, session_id) + "input_data/domain_directory.json"
        with tempfile.TemporaryDirectory(prefix="tmp_domain_save_") as tdir:
            domain_file_path = os.path.join(tdir, 'domain_directory.json')
            with open(domain_file_path, 'w', encoding='utf-8') as f:
                json.dump(domain_dictionary, f, indent=2, ensure_ascii=False)
            if not _upload_file_to_gcs(domain_file_path, bucket, gcs_key):
                return jsonify({'error': 'Failed to upload domain dictionary to bucket'}), 500

        add_log("DATA save_domain_dictionary success")
        return jsonify({'message': 'Domain dictionary saved successfully', 'gcs_path': gcs_key}), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        add_log(f"DATA save_domain_dictionary error: {str(e)}")
        return jsonify({'error': f'Failed to save domain dictionary: {str(e)}'}), 500




@data_bp.route('/check_domain_exists', methods=['POST'])
def check_domain_exists():
    try:
        data = request.get_json()
        if not data:
            add_log("DATA check_domain_exists error: Missing request data")
            return fail('Missing request data', 400)

        session_id = (data.get('session_id') or '').strip()
        if not session_id:
            add_log("DATA check_domain_exists error: Missing field: session_id")
            return fail('Missing field: session_id', 400)

        # MOCKED: Return true for now (as requested)
        add_log(f"DATA check_domain_exists: Mocked to return true for session {session_id}")
        return jsonify({'result': 'success', 'status_code': 200, 'message': 'Domain exists', 'domain': 'mocked_domain'})

        # Original implementation (commented out for mocking):
        # # Source of truth: read from GCS
        # token_payload = g.get('user', {}) or {}
        # user_email = (token_payload.get('email') or '').lower()
        # client, bucket = _get_gcs_bucket()
        # if not user_email or bucket is None:
        #     return jsonify({'result': 'error', 'status_code': 500, 'message': 'GCS not configured or user not authenticated'}), 500

        # domain_payload = None
        # prefix = _user_session_prefix(user_email, session_id) + "input_data/"
        # with tempfile.TemporaryDirectory(prefix="tmp_domain_exists_") as tdir:
        #     for fname in ("domain_dictionary.json", "domain_directory.json"):
        #         tmp = os.path.join(tdir, fname)
        #         if _download_file_from_gcs(bucket, prefix + fname, tmp):
        #             try:
        #                 with open(tmp, 'r', encoding='utf-8') as f:
        #                     domain_payload = json.load(f)
        #                 break
        #             except Exception:
        #                 domain_payload = None

        # if domain_payload is None:
        #     add_log("DATA check_domain_exists error: Domain knowledge missing for session")
        #     return jsonify({'result': 'fail', 'status_code': 404, 'message': 'Domain knowledge missing. Please re-upload or reconnect your data source'})

        # domain_value = domain_payload.get('domain') if isinstance(domain_payload, dict) else None
        # if isinstance(domain_value, str) and domain_value.strip():
        #     return jsonify({'result': 'success', 'status_code': 200, 'message': 'Domain exists', 'domain': domain_value.strip()})

        # add_log("DATA check_domain_exists error: Domain field missing or empty")
        # return jsonify({'result': 'fail', 'status_code': 404, 'message': 'Domain field missing or empty'})
    except Exception as e:
        import traceback
        traceback.print_exc()
        add_log(f"DATA check_domain_exists unexpected error: {str(e)}")
        return jsonify({'result': 'error', 'status_code': 500, 'message': f'Unexpected error: {str(e)}'})