import base64
import mimetypes
from pathlib import Path
import re
from typing import Dict, List
from bs4 import BeautifulSoup
from flask import Blueprint, current_app, request, jsonify, Response, g
import os
import json
import time
import requests
import tempfile
from logger import add_log
import traceback
from ..job_manager import JobStatus
import asyncio
import pandas as pd
from utils.sanitize import sanitize_email_for_storage

# Optional GCS (for Cloud Run mode)
try:
    from google.cloud import storage  # type: ignore
except Exception:
    storage = None


sessions_bp = Blueprint('sessions_bp', __name__)


def _check_session_has_input_data(session_id: str) -> bool:
    try:
        input_data_dir = os.path.join('execution_layer', 'input_data', session_id)
        if not os.path.exists(input_data_dir):
            return False

        # Domain considered present only if domain_directory.json exists with non-empty 'domain'
        domain_path = os.path.join(input_data_dir, 'domain_directory.json')
        domain_payload = None
        try:
            if os.path.exists(domain_path):
                with open(domain_path, 'r', encoding='utf-8') as f:
                    domain_payload = json.load(f)
        except Exception:
            pass

        if not domain_payload:
            return False

        domain_value = domain_payload.get('domain') if isinstance(domain_payload, dict) else None
        return bool(isinstance(domain_value, str) and domain_value.strip())
    except Exception as e:
        add_log(f"Error checking input data for session {session_id}: {str(e)}")
        return False

def build_pseudonymized_columns_map(domain_directory: dict) -> Dict[str, List[str]]:
    """
    Build a domain-agnostic pseudonymization map from domain_directory.json:
      { "data_set_name.pkl": ["col1","col2"], "data_set_name2.pkl": ["col3"] }
    """
    out: Dict[str, List[str]] = {}
    dsf = domain_directory.get("data_set_files", {}) if isinstance(domain_directory, dict) else {}
    if not isinstance(dsf, dict):
        return out
    for ds_name, meta in dsf.items():
        if not isinstance(meta, dict):
            continue
        cols = meta.get("pseudonymized_columns")
        if not isinstance(cols, list):
            cols = []
        cleaned: List[str] = []
        seen = set()
        for c in cols:
            cs = str(c).strip()
            if cs and cs.lower() not in seen:
                seen.add(cs.lower())
                cleaned.append(cs)
        out[str(ds_name)] = cleaned
    return out

def convert_images_to_base64_for_report(*, html_content: str, output_dir: str | Path | None = None) -> str:
    """
    Convert <img src="..."> in an HTML string to base64 data URIs.
    Intended for final report packaging so the HTML becomes self-contained.
    """
    soup = BeautifulSoup(html_content or "", "html.parser")

    out_dir = Path(output_dir) if output_dir else None
    script_dir = Path(__file__).resolve().parent

    for img_tag in soup.find_all("img"):
        src = (img_tag.get("src") or "").strip()
        if not src:
            continue

        # Skip already-inlined or remote images
        if src.startswith("data:") or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", src):
            continue

        # Strip querystring/fragment and normalize
        src_clean = src.split("?", 1)[0].split("#", 1)[0]
        if src_clean.startswith("./"):
            src_clean = src_clean[2:]

        src_path = Path(src_clean)
        candidates: List[Path] = []
        if src_path.is_absolute():
            candidates.append(src_path)
        else:
            if out_dir:
                candidates.append(out_dir / src_path)
            candidates.append(Path.cwd() / src_path)
            candidates.append(script_dir / src_path)

        img_path = next((p for p in candidates if p.exists()), None)
        if not img_path:
            add_log.warning(f"[base64] Image not found for src='{src}' (tried: {candidates})")
            continue

        mime_type, _ = mimetypes.guess_type(str(img_path))
        if not mime_type:
            mime_type = "image/png"

        try:
            encoded = base64.b64encode(img_path.read_bytes()).decode("utf-8")
        except Exception as e:
            add_log.warning(f"[base64] Failed reading '{img_path}': {e}")
            continue

        base64_src = f"data:{mime_type};base64,{encoded}"

        new_img_tag = soup.new_tag("img")
        new_img_tag["src"] = base64_src
        for attr_name, attr_value in img_tag.attrs.items():
            if attr_name != "src":
                new_img_tag[attr_name] = attr_value
        img_tag.replace_with(new_img_tag)

    return str(soup)



@sessions_bp.route('/session_id')
def session_id():
    try:
        token_payload = g.get('user', {})
        session_manager = current_app.session_manager
        session_id, container_id = session_manager.create_session(user_info=token_payload)
        # Cloud Run: ensure user/session prefixes exist in the configured GCS bucket (idempotent, already called in create_session)
        try:
            email = (token_payload.get('email') or '').lower()
            if email:
                session_manager.ensure_user_session_prefixes(email, session_id)
        except Exception:
            pass
        # Cloud Run only mode
        # # Log appropriately for Cloud Run vs Docker mode
        # if session_manager.cloud_run_enabled:
        add_log(f"Session created: {session_id} (Cloud Run mode, no Docker container) for user: {token_payload.get('email', 'unknown')}")
        # else:
        #     add_log(f"Session created: {session_id} with container: {container_id[:12]} for user: {token_payload.get('email', 'unknown')}")
        return jsonify({'session_id': session_id, 'container_id': container_id, 'status': 'success'})
    except Exception as e:
        add_log(f"Error creating session: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to create session: {str(e)}', 'status': 'error'}), 500


@sessions_bp.route('/validate_session/<session_id>')
def validate_session(session_id):
    try:
        session_manager = current_app.session_manager
        http = current_app.requests_session
        session_info = session_manager.get_session_container(session_id)
        if session_info:
            has_input_data = _check_session_has_input_data(session_id)
            # Cloud Run only mode: skip container health check (no container exists)
            # # Cloud Run mode: skip container health check (no container exists)
            # if session_manager.cloud_run_enabled:
            add_log(f"Session {session_id} validation success (Cloud Run mode, no container health check)")
            return jsonify({'session_id': session_id, 'container_id': session_info.get('container_id', ''), 'status': 'active', 'valid': True, 'has_input_data': has_input_data})
            
            # Docker mode commented out - Cloud Run only
            # # Docker mode: check container health
            # container_port = session_info.get('container_port')
            # if container_port:
            #     try:
            #         resp = http.get(f"http://localhost:{container_port}/health", timeout=5)
            #         if resp.status_code == 200:
            #             add_log(f"Session {session_id} validation success (container healthy)")
            #             return jsonify({'session_id': session_id, 'container_id': session_info['container_id'], 'status': 'active', 'valid': True, 'has_input_data': has_input_data})
            #     except requests.RequestException as e:
            #         add_log(f"Container for session {session_id} is not responding: {str(e)} | traceback: {traceback.format_exc()}")
            # add_log(f"Session {session_id} validation success (active)")
            # return jsonify({'session_id': session_id, 'container_id': session_info.get('container_id', ''), 'status': 'active', 'valid': True, 'has_input_data': has_input_data})
        else:
            has_input_data = _check_session_has_input_data(session_id)
            add_log(f"Session {session_id} validation failed - not found or inactive")
            return jsonify({'session_id': session_id, 'status': 'not_found', 'valid': False, 'has_input_data': has_input_data})
    except Exception as e:
        add_log(f"Error validating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to validate session: {str(e)}', 'status': 'error', 'valid': False, 'has_input_data': False}), 500


@sessions_bp.route('/session_status/<session_id>')
def session_status(session_id):
    try:
        status = current_app.session_manager.get_session_status(session_id)
        add_log(f"Session status fetched for {session_id}: {status.get('status')}")
        return jsonify(status)
    except Exception as e:
        add_log(f"Error getting session status: {str(e)}")
        return jsonify({'error': f'Failed to get session status: {str(e)}', 'status': 'error'}), 500


@sessions_bp.route('/restart_session/<session_id>', methods=['POST'])
def restart_user_session(session_id):
    try:
        success = current_app.session_manager.restart_session(session_id)
        if success:
            add_log(f"Session {session_id} restarted successfully")
            return jsonify({'message': f'Session {session_id} restarted successfully', 'status': 'success'})
        else:
            return jsonify({'error': f'Failed to restart session {session_id}', 'status': 'error'}), 404
    except Exception as e:
        add_log(f"Error restarting session: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to restart session: {str(e)}', 'status': 'error'}), 500


@sessions_bp.route('/cleanup_session/<session_id>', methods=['POST'])
def cleanup_session(session_id):
    try:
        # Check if session exists before attempting cleanup
        session_manager = current_app.session_manager
        session_info = session_manager.get_session_container(session_id)
        
        if not session_info:
            add_log(f"Session {session_id} not found for cleanup")
            return jsonify({'message': f'Session {session_id} not found or already cleaned up', 'status': 'success'}), 200
        
        success = session_manager.cleanup_session(session_id)
        if success:
            add_log(f"Session {session_id} cleanup success")
            return jsonify({'message': f'Session {session_id} cleaned up successfully', 'status': 'success'})
        else:
            return jsonify({'error': f'Failed to cleanup session {session_id}', 'status': 'error'}), 404
    except Exception as e:
        add_log(f"Error cleaning up session: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to cleanup session: {str(e)}', 'status': 'error'}), 500


@sessions_bp.route('/create_job', methods=['POST'])
def create_job():
    token_payload = g.get('user', {})
    data = request.get_json()
    if not data or 'query' not in data:
        add_log("Error: Missing analysis query")
        return jsonify({'error': 'Missing analysis query'}), 400

    session_id = data.get('session_id')
    if not session_id:
        add_log("Error: Missing session ID")
        return jsonify({'error': 'Missing session ID'}), 400

    session_info = current_app.session_manager.get_session_container(session_id)
    if not session_info:
        # Check if session exists but container is missing
        session_status = current_app.session_manager.get_session_status(session_id)
        if session_status and session_status.get('status') == 'inactive':
            add_log(f"Error: Session {session_id} exists but container is missing or inactive")
            return jsonify({
                'error': f'Session container is no longer available. Please create a new session.',
                'error_code': 'SESSION_CONTAINER_MISSING',
                'session_id': session_id
            }), 400
        else:
            add_log(f"Error: Invalid or inactive session: {session_id}")
            return jsonify({
                'error': f'Invalid or inactive session: {session_id}',
                'error_code': 'SESSION_NOT_FOUND',
                'session_id': session_id
            }), 400

    user_query = data['query']
    model_name = data.get('model', "gpt-5.4")  # Default to gpt-5.4

    try:
        job_id, job_info = current_app.job_manager.create_job(
            session_id=session_id,
            query=user_query,
            model=model_name,
            session_info=session_info,
            user_info=token_payload
        )

        success = current_app.job_manager.start_job_execution(job_id, current_app.session_manager)
        if not success:
            add_log(job_id, f"Failed to start execution for job {job_id} in session {session_id}")
            return jsonify({'error': 'Failed to start job execution'}), 500

        response_data = {'status': 'success', 'job_id': job_id, 'message': 'Job created and started successfully'}
        print(f"[API LAYER] ✅ Job created and started: {job_id} - Query: {user_query[:50]}...")
        add_log(job_id, f"Job {job_id} created via API and execution started for session {session_id}")
        return jsonify(response_data)
    except Exception as e:
        error_msg = f"Job creation failed: {str(e)} | traceback: {traceback.format_exc()}"
        add_log(error_msg, job_id=job_id if 'job_id' in locals() else None)
        if 'job_id' in locals():
            add_log(job_id, f"Job creation failed at API layer: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@sessions_bp.route('/simpleqna/query', methods=['POST'])
def simpleqna_query():
    """Simple QnA endpoint - works directly with sessions, no jobs required"""
    token_payload = g.get('user', {})
    data = request.get_json()
    if not data or 'query' not in data:
        add_log("Error: Missing query")
        return jsonify({'error': 'Missing query'}), 400

    session_id = data.get('session_id')
    if not session_id:
        add_log("Error: Missing session ID")
        return jsonify({'error': 'Missing session ID'}), 400

    session_info = current_app.session_manager.get_session_container(session_id)
    if not session_info:
        # Check if session exists but container is missing
        session_status = current_app.session_manager.get_session_status(session_id)
        if session_status and session_status.get('status') == 'inactive':
            add_log(f"Error: Session {session_id} exists but container is missing or inactive")
            return jsonify({
                'error': f'Session container is no longer available. Please create a new session.',
                'error_code': 'SESSION_CONTAINER_MISSING',
                'session_id': session_id
            }), 400
        else:
            add_log(f"Error: Invalid or inactive session: {session_id}")
            return jsonify({
                'error': f'Invalid or inactive session: {session_id}',
                'error_code': 'SESSION_NOT_FOUND',
                'session_id': session_id
            }), 400

    # Cloud Run only mode: simpleqna_query requires a container, which doesn't exist in Cloud Run mode
    # Use /create_job endpoint instead, which will deploy a Cloud Run service per job
    # if current_app.session_manager.cloud_run_enabled:
    return jsonify({
        'error': 'Simple QnA endpoint not available in Cloud Run mode. Please use /create_job endpoint instead.',
        'error_code': 'CLOUD_RUN_MODE',
        'session_id': session_id
    }), 400
    
    # Docker mode code commented out - Cloud Run only
    # user_query = data['query']
    # model_name = data.get('model', "gpt-5.4-mini")
    # container_port = session_info.get('container_port')
    # 
    # if not container_port:
    #     return jsonify({'error': 'Cannot determine container port for session'}), 400
    #
    # # Preflight: ensure session has input data and a domain dictionary
    # try:
    #     if not _check_session_has_input_data(session_id):
    #         input_dir = os.path.join('execution_layer', 'input_data', session_id)
    #         existing = []
    #         try:
    #             if os.path.exists(input_dir):
    #                 existing = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
    #         except Exception:
    #             existing = []
    #         add_log(f"❌ Simple QnA preflight failed for session {session_id}: missing domain or data files. Existing files: {existing}")
    #         return jsonify({
    #             'error': 'INPUT_DATA_MISSING',
    #             'message': 'No dataset/domain found for this session. Please upload data or select sample data.',
    #             'files': existing,
    #             'session_id': session_id
    #         }), 400
    # except Exception:
    #     # Do not block QnA due to preflight error; container will still validate paths internally
    #     pass
    #
    # try:
    #     # Call container endpoint directly (no job creation)
    #     import requests
    #     container_url = f"http://localhost:{container_port}/simpleqna/query"
    #     
    #     response = requests.post(
    #         container_url,
    #         json={
    #             'session_id': session_id,
    #             'query': user_query,
    #             'model': model_name,
    #             'user_email': token_payload.get('email', '')
    #         },
    #         timeout=300  # 5 minute timeout for QnA
    #     )
    #     
    #     if response.status_code == 200:
    #         try:
    #             result = response.json()
    #         except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
    #             # Response is not valid JSON
    #             response_text = response.text[:500] if response.text else "Empty response"
    #             error_msg = f"Container returned invalid JSON response: {response_text}"
    #             add_log(f"Simple QnA query failed: {error_msg}")
    #             return jsonify({'error': error_msg, 'raw_response': response_text}), 500
    #         
    #         # Return the complete response from container
    #         response_data = {
    #             'status': 'success',
    #             'response': result.get('response', ''),
    #             'html': result.get('html', ''),
    #             'metrics': result.get('metrics', {}),
    #             'costs': result.get('costs', {}),
    #             'session_id': session_id
    #         }
    #         add_log(f"✅ Simple QnA query successful for session {session_id}")
    #         return jsonify(response_data)
    #     else:
    #         # Handle error response
    #         error_msg = f'Container returned status {response.status_code}'
    #         try:
    #             if response.content:
    #                 error_data = response.json()
    #                 error_msg = error_data.get('error', error_msg)
    #         except (ValueError, requests.exceptions.JSONDecodeError):
    #             # Not JSON, use text response
    #             if response.text:
    #                 error_msg = f'Container error: {response.text[:500]}'
    #             else:
    #                 error_msg = f'Container returned status {response.status_code} with empty response'
    #         
    #         add_log(f"Simple QnA query failed: {error_msg}")
    #         return jsonify({'error': error_msg}), response.status_code
    #         
    # except requests.RequestException as e:
    #     error_msg = f"Error communicating with container: {str(e)}"
    #     add_log(f"{error_msg} | traceback: {traceback.format_exc()}")
    #     return jsonify({'error': error_msg}), 500
    # except Exception as e:
    #     error_msg = f"Simple QnA query failed: {str(e)}"
    #     add_log(f"{error_msg} | traceback: {traceback.format_exc()}")
    #     return jsonify({'error': str(e)}), 500


@sessions_bp.route('/create_simpleqna_job', methods=['POST'])
def create_simpleqna_job():
    """Legacy endpoint - kept for backward compatibility, redirects to session-only flow"""
    # Redirect to new session-only endpoint
    return simpleqna_query()


@sessions_bp.route('/cancel', methods=['POST', 'OPTIONS'])
def cancel_processing():
    """Cancel ongoing processing for a session.
    
    This endpoint proxies the cancel request to the container and marks
    the session for cancellation. Running agents will check this flag
    and exit gracefully at the next checkpoint.
    
    Request body:
        - session_id: The session to cancel
        - job_id: Optional job ID to cancel (for job-based flow)
    """
    # Handle CORS preflight request
    if request.method == 'OPTIONS':
        response = jsonify({})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
        response.headers.add('Access-Control-Max-Age', '3600')
        return response
    
    token_payload = g.get('user', {})
    data = request.get_json() or {}
    session_id = data.get('session_id')
    job_id = data.get('job_id')
    
    if not session_id:
        return jsonify({'error': 'Missing session_id'}), 400
    
    # Get session info to find container port
    session_info = current_app.session_manager.get_session_container(session_id)
    if not session_info:
        add_log(f"Cancel request for invalid/inactive session: {session_id}")
        response = jsonify({
            'error': f'Session not found or inactive: {session_id}',
            'status': 'not_found'
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 404
    
    # Cloud Run only mode: cancellation handled at job level (Cloud Run service deletion)
    # if current_app.session_manager.cloud_run_enabled:
    add_log(f"🛑 Cancel request for Cloud Run mode - session {session_id}, job {job_id} (handled at job level)")
    result = jsonify({
        'status': 'cancelled',
        'session_id': session_id,
        'job_id': job_id,
        'message': 'Cancellation signal sent. Job service will be terminated.',
        'container_signaled': False  # No container in Cloud Run mode
    })
    result.headers.add('Access-Control-Allow-Origin', '*')
    return result
    
    # Docker mode code commented out - Cloud Run only
    # container_port = session_info.get('container_port')
    # if not container_port:
    #     response = jsonify({'error': 'Cannot determine container port for session'})
    #     response.headers.add('Access-Control-Allow-Origin', '*')
    #     return response, 400
    # 
    # try:
    #     # Proxy cancel request to the container
    #     container_url = f'http://localhost:{container_port}/cancel'
    #     cancel_payload = {'session_id': session_id}
    #     if job_id:
    #         cancel_payload['job_id'] = job_id
    #     
    #     # Best-effort: try to cancel at container level (non-blocking)
    #     try:
    #         response = requests.post(
    #             container_url,
    #             json=cancel_payload,
    #             timeout=2  # Short timeout - don't block
    #         )
    #         container_result = response.json() if response.status_code == 200 else None
    #     except Exception as container_error:
    #         # Container cancel failed - but we still mark as cancelled locally
    #         add_log(f"⚠️ Container cancel call failed (non-critical): {container_error}")
    #         container_result = None
    #     
    #     add_log(f"🛑 Cancel request sent for session {session_id}, job {job_id}")
    #     
    #     # Always return success - cancellation is best-effort
    #     result = jsonify({
    #         'status': 'cancelled',
    #         'session_id': session_id,
    #         'job_id': job_id,
    #         'message': 'Cancellation signal sent. Processing will stop at next checkpoint.',
    #         'container_signaled': container_result is not None
    #     })
    #     result.headers.add('Access-Control-Allow-Origin', '*')
    #     return result
    #     
    # except Exception as e:
    #     add_log(f"Error sending cancel request: {str(e)} | traceback: {traceback.format_exc()}")
    #     # Even on error, return success - cancellation is best-effort
    #     result = jsonify({
    #         'status': 'cancelled',
    #         'session_id': session_id,
    #         'message': 'Cancellation signal sent (best-effort)'
    #     })
    #     result.headers.add('Access-Control-Allow-Origin', '*')
    #     return result


@sessions_bp.route('/job_status/<job_id>')
def get_job_status(job_id):
    try:
        job_info = current_app.job_manager.get_job(job_id)
        if not job_info:
            return jsonify({'error': 'Job not found'}), 404
        status = job_info['status']
        status_str = getattr(status, 'value', status)
        response_data = {
            'job_id': job_info['job_id'],
            'status': status_str,
            'created_at': job_info['created_at'],
            'started_at': job_info.get('started_at'),
            'completed_at': job_info.get('completed_at'),
            'error': job_info.get('error')
        }
        add_log(job_id, f"Job status fetched via API: {response_data['status']}")
        return jsonify(response_data)
    except Exception as e:
        add_log(f"Error getting job status: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to get job status: {str(e)}'}), 500


@sessions_bp.route('/job_report', methods=['POST'])
def get_job_report():
    """Get job report - accepts POST with payload: {'user_id': 'email', 'session_id': '...', 'job_id': '...', 'get_original': true/false}"""
    try:
        data = request.get_json() or {}
        
        # Get parameters from POST payload
        user_email = (data.get('user_id') or '').lower()
        session_id = data.get('session_id')
        job_id = data.get('job_id')
        get_original = data.get('get_original', False)  # New parameter: if True, return analysis_report_original.html
        
        # Validate required parameters
        if not job_id:
            return jsonify({'error': 'Missing job_id in request payload'}), 400
        if not session_id:
            return jsonify({'error': 'Missing session_id in request payload'}), 400
        if not user_email:
            return jsonify({'error': 'Missing user_id in request payload'}), 400
        
        # Get job info
        job_info = current_app.job_manager.get_job(job_id)
        if not job_info:
            return jsonify({'error': 'Job not found'}), 404
        
        # Verify job belongs to the provided session and user
        job_session_id = job_info.get('session_id')
        job_user_info = job_info.get('user_info', {})
        job_user_email = (job_user_info.get('email') or '').lower()
        
        if job_session_id != session_id:
            return jsonify({'error': 'Job does not belong to the provided session'}), 403
        
        if job_user_email != user_email:
            return jsonify({'error': 'Job does not belong to the provided user'}), 403
        
        status = job_info['status']
        status_str = getattr(status, 'value', status)
        
        # Determine which report file to fetch based on get_original flag
        report_filename = "analysis_report_original.html" if get_original else "analysis_report.html"
        
        # Allow fetching report if it exists, regardless of status (handles cases where status is incorrectly set)
        # Check if report exists first before rejecting based on status
        report_exists = False
        
        # Check GCS first
        if storage is not None:
            try:
                bucket_name = current_app.session_manager.gcs_bucket or os.getenv('GCS_BUCKET', 'insightbot-dev-474509.firebasestorage.app')
                if bucket_name:
                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    user_email_sanitized = sanitize_email_for_storage(user_email)
                    gcs_blob_path = f"{user_email_sanitized}/{session_id}/output_data/{job_id}/{report_filename}"
                    blob = bucket.blob(gcs_blob_path)
                    report_exists = blob.exists()
            except Exception:
                pass
        
        # Check local disk as fallback
        if not report_exists:
            output_dir = job_info.get('output_dir')
            if output_dir:
                report_path = os.path.join(output_dir, report_filename)
                if os.path.exists(report_path):
                    report_exists = True
        
        # Only reject if status is not completed AND report doesn't exist
        if status_str != 'completed' and not report_exists:
            return jsonify({
                'error': f'Job is not completed and report not found. Current status: {status_str}',
                'status': status_str
            }), 400
        
        # If status is not completed but report exists, log a warning but allow fetching
        if status_str != 'completed' and report_exists:
            add_log(f"Warning: Fetching report for job {job_id} with status '{status_str}' - report exists, allowing fetch")
        
        # Cloud Run only mode - always enabled
        # # Check if Cloud Run mode is enabled
        # use_cloud_run = current_app.session_manager.cloud_run_enabled
        
        # Try GCS first (Cloud Run only mode)
        if storage is not None:
            try:
                bucket_name = current_app.session_manager.gcs_bucket or os.getenv('GCS_BUCKET', 'insightbot-dev-474509.firebasestorage.app')
                if bucket_name:
                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    user_email_sanitized = sanitize_email_for_storage(user_email)
                    gcs_blob_path = f"{user_email_sanitized}/{session_id}/output_data/{job_id}/{report_filename}"
                    
                    blob = bucket.blob(gcs_blob_path)
                    if blob.exists():
                        # Download to temp file and read (properly handle Windows file locking)
                        tmp_file_path = None
                        try:
                            # Create temp file with delete=False, then manually delete after closing
                            with tempfile.NamedTemporaryFile(mode='wb', suffix='.html', delete=False) as tmp_file:
                                tmp_file_path = tmp_file.name
                                blob.download_to_filename(tmp_file_path)
                            
                            # Read the file after it's closed
                            with open(tmp_file_path, 'r', encoding='utf-8') as f:
                                html = f.read()
                            
                            # Clean up temp file after reading
                            try:
                                os.unlink(tmp_file_path)
                            except Exception as cleanup_err:
                                add_log(f"Warning: Could not delete temp file {tmp_file_path}: {str(cleanup_err)}")
                            
                            report_type = "original" if get_original else "final"
                            add_log(f"Report ({report_type}) served for job {job_id} from GCS: {gcs_blob_path}")
                            return Response(html, mimetype='text/html')
                        except Exception as tmp_err:
                            # Clean up temp file on error
                            if tmp_file_path and os.path.exists(tmp_file_path):
                                try:
                                    os.unlink(tmp_file_path)
                                except Exception:
                                    pass
                            raise tmp_err
                    else:
                        add_log(f"Report ({report_filename}) not found in GCS for job {job_id} at {gcs_blob_path}, trying local fallback")
            except Exception as gcs_err:
                add_log(f"GCS read error for job {job_id}: {str(gcs_err)}, trying local fallback")
        
        # Fallback to local disk (Docker mode or GCS unavailable)
        output_dir = job_info.get('output_dir')
        if not output_dir:
            add_log(f"Output dir not found for job {job_id}")
            return jsonify({'error': 'Report not found in GCS or local disk', 'output_dir': None}), 404
        
        report_path = os.path.join(output_dir, report_filename)
        if not os.path.exists(report_path):
            add_log(f"Report file ({report_filename}) does not exist at {report_path} for job {job_id}")
            return jsonify({'error': f'Report file ({report_filename}) does not exist at {report_path}'}), 404
        
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                html = f.read()
            
            report_type = "original" if get_original else "final"
            add_log(f"Report ({report_type}) served for job {job_id} from local disk: {report_path}")
            return Response(html, mimetype='text/html')
        except Exception as read_err:
            add_log(f"Error reading report file {report_path}: {str(read_err)}")
            return jsonify({'error': f'Failed to read report file: {str(read_err)}'}), 500
    except Exception as e:
        add_log(f"Error getting job report: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to read report: {str(e)}'}), 500


@sessions_bp.route('/analysis_history', methods=['GET'])
def get_analysis_history():
    try:
        user_info = g.get('user')
        if not user_info:
            return jsonify({'error': 'Authentication required'}), 401
        user_email = user_info.get('email')
        if not user_email:
            return jsonify({'error': 'User email not found'}), 401
        job_history = current_app.job_manager.data_manager.get_user_job_history(user_email, limit=50)
        history_items = []
        for job_data in job_history:
            try:
                created_at = job_data.get('created_at')
                timestamp_str = "Unknown"
                if created_at:
                    try:
                        if hasattr(created_at, 'strftime'):
                            timestamp_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            timestamp_str = str(created_at)
                    except:
                        timestamp_str = str(created_at)
                job_status = job_data.get('job_status', 'unknown')
                if job_status == 'success':
                    frontend_status = 'completed'
                elif job_status == 'failed':
                    frontend_status = 'failed'
                elif job_status == 'running':
                    frontend_status = 'running'
                else:
                    frontend_status = 'failed'
                history_item = {
                    'id': job_data.get('job_id'),
                    'query': job_data.get('question', ''),
                    'timestamp': timestamp_str,
                    'status': frontend_status,
                    'reportUrl': job_data.get('report_url', ''),
                    'sessionId': job_data.get('session_id', ''),
                    'totalTokens': job_data.get('total_token_used', 0),
                    'totalCost': job_data.get('total_cost', 0.0)
                }
                history_items.append(history_item)
            except Exception as e:
                add_log(f"Error processing job data: {e}")
                print(f"⚠️ Error processing job data: {e}")
                continue
        add_log(f"Analysis history fetched for {user_email}: {len(history_items)} items")
        return jsonify({'history': history_items, 'total': len(history_items), 'message': f'Retrieved {len(history_items)} completed analysis records'})
    except Exception as e:
        add_log(f"Error getting analysis history: {str(e)}")
        return jsonify({'error': f'Failed to get analysis history: {str(e)}'}), 500

@sessions_bp.route('/job_report', methods=['POST'])
def get_job_report_post():
    """Get job report - accepts POST with payload: {'user_id': 'email', 'session_id': '...', 'job_id': '...', 'get_original': true/false}"""
    try:
        data = request.get_json() or {}
        
        # Get parameters from POST payload
        user_email = (data.get('user_id') or '').lower()
        session_id = data.get('session_id')
        job_id = data.get('job_id')
        get_original = data.get('get_original', False)  # New parameter: if True, return analysis_report_original.html
        
        # Validate required parameters
        if not job_id:
            return jsonify({'error': 'Missing job_id in request payload'}), 400
        if not session_id:
            return jsonify({'error': 'Missing session_id in request payload'}), 400
        if not user_email:
            return jsonify({'error': 'Missing user_id in request payload'}), 400
        
        # Get job info
        job_info = current_app.job_manager.get_job(job_id)
        if not job_info:
            return jsonify({'error': 'Job not found'}), 404
        
        # Verify job belongs to the provided session and user
        job_session_id = job_info.get('session_id')
        job_user_info = job_info.get('user_info', {})
        job_user_email = (job_user_info.get('email') or '').lower()
        
        if job_session_id != session_id:
            return jsonify({'error': 'Job does not belong to the provided session'}), 403
        
        if job_user_email != user_email:
            return jsonify({'error': 'Job does not belong to the provided user'}), 403
        
        status = job_info['status']
        status_str = getattr(status, 'value', status)

        # Get output directory from job info
        output_dir = job_info.get('output_dir')
        if not output_dir:
            add_log(f"Output dir not found for job {job_id}")
            return jsonify({'error': 'Report not found - output directory not available', 'output_dir': None}), 404

        # Read domain directory to determine pseudonymization status
        pseudonymized_columns_map = {}
        try:
            input_data_dir = os.path.join('execution_layer', 'input_data', session_id)
            domain_path = os.path.join(input_data_dir, 'domain_directory.json')
            domain_payload = None
            try:
                if os.path.exists(domain_path):
                    with open(domain_path, 'r', encoding='utf-8') as f:
                        domain_payload = json.load(f)
            except Exception:
                pass

            if domain_payload:
                pseudonymized_columns_map = build_pseudonymized_columns_map(domain_payload)
                print(f"Pseudonymized columns map: {pseudonymized_columns_map}")
        except Exception as e:
            add_log(f"Error reading domain directory for session {session_id}: {str(e)}")
            # Continue without pseudonymization info - treat as not pseudonymized

        # Determine which report file to fetch and the report type
        if get_original:
            # User explicitly requested original report
            report_filename = "analysis_report_original.html"
            report_type_key = "pseudonymized_report"
        else:
            # Default behavior: check if depseudonymized report exists, otherwise use analysis_report.html
            depseudonymized_report_path = os.path.join(output_dir, "de_pseudonymized_report.html")
            if os.path.exists(depseudonymized_report_path):
                report_filename = "de_pseudonymized_report.html"
                report_type_key = "depseudonymized_report"
            else:
                report_filename = "analysis_report.html"
                if pseudonymized_columns_map:
                    report_type_key = "pseudonymized_report"  # Show pseudonymized version until user depseudonymizes
                else:
                    report_type_key = "analysis_report"  # No pseudonymization was applied

        # Check if report file exists
        report_path = os.path.join(output_dir, report_filename)
        report_exists = os.path.exists(report_path)

        # Only reject if status is not completed AND report doesn't exist
        if status_str != 'completed' and not report_exists:
            return jsonify({
                'error': f'Job is not completed and report not found. Current status: {status_str}',
                'status': status_str
            }), 400

        # If status is not completed but report exists, log a warning but allow fetching
        if status_str != 'completed' and report_exists:
            add_log(f"Warning: Fetching report for job {job_id} with status '{status_str}' - report exists, allowing fetch")

        # Read report from local disk
        if not report_exists:
            add_log(f"Report file ({report_filename}) does not exist at {report_path} for job {job_id}")
            return jsonify({'error': f'Report file ({report_filename}) does not exist at {report_path}'}), 404

        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                html = f.read()

            add_log(f"Report ({report_type_key}) served for job {job_id} from local disk: {report_path}")
            return jsonify({
                'report': html,
                'type': report_type_key,
                'job_id': job_id,
                'session_id': session_id
            })
        except Exception as read_err:
            add_log(f"Error reading report file {report_path}: {str(read_err)}")
            return jsonify({'error': f'Failed to read report file: {str(read_err)}'}), 500
    except Exception as e:
        add_log(f"Error getting job report: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to read report: {str(e)}'}), 500
        
    #     # Get output directory from job info
    #     output_dir = job_info.get('output_dir')
    #     if not output_dir:
    #         add_log(f"Output dir not found for job {job_id}")
    #         return jsonify({'error': 'Report not found - output directory not available', 'output_dir': None}), 404
        
    #     # Check if report file exists
    #     report_path = os.path.join(output_dir, report_filename)
    #     report_exists = os.path.exists(report_path)
        
    #     # Only reject if status is not completed AND report doesn't exist
    #     if status_str != 'completed' and not report_exists:
    #         return jsonify({
    #             'error': f'Job is not completed and report not found. Current status: {status_str}',
    #             'status': status_str
    #         }), 400
        
    #     # If status is not completed but report exists, log a warning but allow fetching
    #     if status_str != 'completed' and report_exists:
    #         add_log(f"Warning: Fetching report for job {job_id} with status '{status_str}' - report exists, allowing fetch")
        
    #     # Read report from local disk
    #     if not report_exists:
    #         add_log(f"Report file ({report_filename}) does not exist at {report_path} for job {job_id}")
    #         return jsonify({'error': f'Report file ({report_filename}) does not exist at {report_path}'}), 404
        
    #     try:
    #         with open(report_path, 'r', encoding='utf-8') as f:
    #             html = f.read()

    #         report_type = "original" if get_original else "analysis_report"
    #         add_log(f"Report ({report_type}) served for job {job_id} from local disk: {report_path}")
    #         return jsonify({
    #             'report': html,
    #             'report_type': report_type,
    #             'job_id': job_id,
    #             'session_id': session_id
    #         })
    #     except Exception as read_err:
    #         add_log(f"Error reading report file {report_path}: {str(read_err)}")
    #         return jsonify({'error': f'Failed to read report file: {str(read_err)}'}), 500
    # except Exception as e:
    #     add_log(f"Error getting job report: {str(e)} | traceback: {traceback.format_exc()}")
    #     return jsonify({'error': f'Failed to read report: {str(e)}'}), 500


@sessions_bp.route('/depseudonymize_report', methods=['POST'])
def depseudonymize_report():
    """Depseudonymize a report by calling execution layer API"""
    try:
        data = request.get_json() or {}
        user_info = g.get('user', {})

        # Validate required parameters
        required_fields = ['session_id', 'job_id']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'error': f'Missing required parameter: {field}'}), 400

        session_id = data['session_id']
        job_id = data['job_id']
        user_email = user_info.get('email', '')

        if not user_email:
            return jsonify({'error': 'User authentication required'}), 401

        # NOTE: Depseudonymization now runs on the host API layer directly
        
        # Call host execution layer depseudonymization API
        try:
            from api_layer.depseudonymization import run_depseudonymization_on_host
            
            result = run_depseudonymization_on_host(session_id, job_id)

            if result.get("status") == "success":
                add_log(f"✅ Report depseudonymized successfully for job {job_id}")
                # Return HTML directly like job_report endpoint
                report_html = result.get("report_html", "")
                if report_html:
                    return Response(report_html, mimetype='text/html')
                else:
                    return jsonify({'error': 'Depseudonymized report HTML not generated'}), 500
            else:
                error_msg = result.get('error') or "Unknown error"
                add_log(f"❌ Report depseudonymization failed: {error_msg}")
                return jsonify({'error': error_msg}), 500

        except Exception as e:
            error_msg = f"Error executing depseudonymization: {str(e)}"
            add_log(f"{error_msg} | traceback: {traceback.format_exc()}")
            return jsonify({'error': error_msg}), 500

    except Exception as e:
        add_log(f"Error in depseudonymize_report: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to depseudonymize report: {str(e)}'}), 500


# @sessions_bp.route('/analysis_report/<job_id>', methods=['GET'])
# def get_analysis_report(job_id):
#     try:
#         user_info = g.get('user')
#         if not user_info:
#             return jsonify({'error': 'Authentication required'}), 401
#         user_email = user_info.get('email')
#         if not user_email:
#             return jsonify({'error': 'User email not found'}), 401
#         job_history = current_app.job_manager.data_manager.get_user_job_history(user_email, limit=500)
#         target_job = None
#         for job_data in job_history:
#             if job_data.get('job_id') == job_id:
#                 target_job = job_data
#                 break
#         if not target_job:
#             return jsonify({'error': 'Analysis report not found or access denied'}), 404
#         report_url = target_job.get('report_url', '')
#         if not report_url:
#             return jsonify({'error': 'Report URL not available'}), 404
#         try:
#             response = requests.get(report_url, timeout=30)
#             response.raise_for_status()
#             response.encoding = 'utf-8'
#             html_content = response.text
            
#             return Response(html_content, mimetype='text/html; charset=utf-8')
#         except requests.RequestException as e:
#             print(f"❌ Failed to fetch report from URL {report_url}: {str(e)}")
#             return jsonify({'error': f'Failed to fetch report: {str(e)}'}), 500
#     except Exception as e:
#         add_log(f"Error getting analysis report: {str(e)}")
#         return jsonify({'error': f'Failed to get analysis report: {str(e)}'}), 500

@sessions_bp.route('/get_analysis_report', methods=['POST'])
def get_analysis_report():
    data = request.get_json()
    report_url = data.get('report_url')
    try:
        try:
            response = requests.get(report_url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            html_content = response.text
            add_log("Analysis report fetched successfully from URL")
            return Response(html_content, mimetype='text/html; charset=utf-8')
        except requests.RequestException as e:
            print(f"❌ Failed to fetch report from URL {report_url}: {str(e)}")
            return jsonify({'error': f'Failed to fetch report: {str(e)}'}), 500
    except Exception as e:
        add_log(f"Error getting analysis report: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to get analysis report: {str(e)}'}), 500

