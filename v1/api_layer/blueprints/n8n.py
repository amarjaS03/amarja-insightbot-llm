from flask import Blueprint, current_app, request, jsonify
import os
import io
import requests
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote
import re
from logger import add_log
import traceback
from .data import _clear_session_input_data


n8n_bp = Blueprint('n8n_bp', __name__)

_session_id = None

def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'n8n operation failed'}), status

# @n8n_bp.route('/n8n/session', methods=['POST'])
# def create_n8n_session():
#     try:
#         global _session_id
#         data = request.get_json(silent=True) or {}
#         provided_session_id = (data.get('session_id') or '').strip()

#         # If a session_id is provided, store it. Otherwise create a new one.
#         if provided_session_id:
#             _session_id = provided_session_id
#             add_log(f"[n8n] Stored provided session: {_session_id}")
#             return jsonify({'status': 'success', 'status_code': 200,'message': 'Session id stored'}), 200
#         else:
#             return jsonify({'status': 'error','status_code': 400,'message': 'Session id missing'}), 400
#     except Exception as e:
#         add_log(f"[n8n] Error creating/storing session: {str(e)}")
#         return jsonify({'error': f'Failed to create/store session: {str(e)}', 'status': 'error'}), 500


@n8n_bp.route('/n8n/download_csv', methods=['POST'])
def n8n_download_csv_to_pkl():
    try:
        global _session_id
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        url = (data.get('url') or '').strip()
        _session_id = (data.get('session_id') or '').strip()

        if not url:
            return fail('Missing url in request body', 400)
        if not _session_id:
            return fail('Missing session_id. Call /n8n/session first or provide session_id', 400)

        # Clear existing files for this session before saving new pickle
        try:
            if not _clear_session_input_data(_session_id):
                add_log(f"[n8n] Warning: Failed to fully clear input data for session {_session_id}")
        except Exception as e:
            add_log(f"[n8n] Error clearing input data for session {_session_id}: {str(e)} | traceback: {traceback.format_exc()}")

        # Fetch CSV bytes
        http = current_app.requests_session if hasattr(current_app, 'requests_session') else requests
        try:
            resp = http.get(url, timeout=60)
            resp.raise_for_status()
            csv_bytes = resp.content
        except requests.RequestException as re:
            add_log(f"[n8n] Failed to download CSV from {url}: {str(re)} | traceback: {traceback.format_exc()}")
            return fail(f'Failed to download CSV: {str(re)}', 502)

        # Parse CSV via pandas
        try:
            df = pd.read_csv(io.BytesIO(csv_bytes),low_memory=False)
        except Exception as pe:
            add_log(f"[n8n] Failed reading CSV: {str(pe)} | traceback: {traceback.format_exc()}")
            return fail(f'Failed to read CSV: {str(pe)}', 422)

        # Save PKL under execution_layer/input_data/<session_id>/
        input_data_dir = os.path.join('execution_layer', 'input_data', _session_id)
        os.makedirs(input_data_dir, exist_ok=True)
        parsed = urlparse(url)
        candidate = os.path.basename(parsed.path)
        candidate = unquote(candidate)

        if not candidate.lower().endswith('.csv') or not candidate:
            qs = parse_qs(parsed.query)
            found = None
            for key in ('filename', 'file', 'name', 'download', 'attachment', 'export'):
                for value in qs.get(key, []):
                    if '.csv' in value.lower():
                        found = value
                        break
                if found:
                    break
            if not found:
                m = re.search(r'([^/?#]+\.csv)(?:[?#]|$)', unquote(url), flags=re.IGNORECASE)
                candidate = m.group(1) if m else 'download.csv'
            else:
                candidate = found

        # Sanitize for filesystem (Windows-safe)
        unsafe_chars = '<>:"/\\|?*'
        safe_base, _ = os.path.splitext(candidate)
        for ch in unsafe_chars:
            safe_base = safe_base.replace(ch, '_')
        safe_base = safe_base.strip(' .') or 'download'

        pkl_filename = f"{safe_base}.pkl"
        pkl_path = os.path.join(input_data_dir, pkl_filename)

        try:
            df.to_pickle(pkl_path)
        except Exception as se:
            add_log(f"[n8n] Failed saving PKL: {str(se)} | traceback: {traceback.format_exc()}")
            return fail(f'Failed to save PKL: {str(se)}', 500)

        # Optional preview
        top_records = []
        try:
            preview = df.head(10).to_dict('records')
            for rec in preview:
                str_rec = {str(k): ("null" if v is None else str(v)) for k, v in rec.items()}
                top_records.append(str_rec)
        except Exception:
            pass

        add_log(f"[n8n] Saved PKL {pkl_filename} for session {_session_id}")
        if job_id:
            try:
                add_log(job_id, f"[n8n] Saved PKL {pkl_filename} for session {_session_id}")
            except Exception:
                pass
        return jsonify({
            'status': 'success',
            'status_code': 200,
            'message': 'PKL saved successfully',
            'session_id': _session_id,
            'saved_file': pkl_filename,
            'preview_data': {"top_records":top_records}
        })
    except Exception as e:
        add_log(f"[n8n] Unexpected error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Unexpected error: {str(e)}', 500)


