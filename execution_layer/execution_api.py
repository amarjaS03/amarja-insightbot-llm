from flask import Flask, request, jsonify
import asyncio
from flask_cors import CORS
import socketio  # SocketIO client to connect to API layer
import json
import requests
import time
import os
import sys
import threading
import traceback
import uuid
import shutil
import queue
from pathlib import Path
import re
import firebase_admin
from firebase_admin import firestore as firebase_firestore

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import analysis components
from execution_layer.agents.data_analysis_agent import DataAnalysisAgent
from execution_layer.agents.llm_client import initialize_client
from typing import TypedDict, Dict, Any

from execution_layer.image_utils.image_master import write_image_master_atomic, load_image_master

# Import Simple QnA agent (lazy import to avoid startup errors)
try:
    from execution_layer.agents.simpleqna_data_analysis import DataAnalysisAgent as SimpleQnaDataAnalysisAgent
except ImportError as e:
    print(f"⚠️ Warning: Could not import SimpleQnaDataAnalysisAgent: {e}")
    SimpleQnaDataAnalysisAgent = None

class MetricsState(TypedDict):
    total_tokens: int
    prompt_tokens: int
    completion_tokens: int
    successful_requests: int


ENFORCED_MODEL_NAME = "gpt-5.4"

class ProgressEvent:
    def __init__(self, job_id: str, stage: str, message: str, percentage: int, emoji: str = ""):
        self.job_id = job_id
        self.stage = stage
        self.message = message
        self.percentage = percentage
        self.emoji = emoji
        self.timestamp = time.time()
        self.iso_timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())

    def to_dict(self):
        return {
            'job_id': self.job_id,
            'stage': self.stage,
            'message': self.message,
            'percentage': self.percentage,
            'emoji': self.emoji,
            'timestamp': self.timestamp,
            'iso_timestamp': self.iso_timestamp
        }

class CancellationManager:
    """Thread-safe manager for tracking cancelled sessions/jobs"""
    
    def __init__(self):
        self._cancelled_sessions = set()
        self._cancelled_jobs = set()
        self._lock = threading.Lock()
    
    def cancel_session(self, session_id: str) -> bool:
        """Mark a session as cancelled. Returns True if newly cancelled."""
        with self._lock:
            if session_id not in self._cancelled_sessions:
                self._cancelled_sessions.add(session_id)
                print(f"🛑 [CANCELLATION] Session {session_id} marked for cancellation")
                return True
            return False
    
    def cancel_job(self, job_id: str) -> bool:
        """Mark a job as cancelled. Returns True if newly cancelled."""
        with self._lock:
            if job_id not in self._cancelled_jobs:
                self._cancelled_jobs.add(job_id)
                print(f"🛑 [CANCELLATION] Job {job_id} marked for cancellation")
                return True
            return False
    
    def is_cancelled(self, session_id: str = None, job_id: str = None) -> bool:
        """Check if a session or job is cancelled"""
        with self._lock:
            if session_id and session_id in self._cancelled_sessions:
                return True
            if job_id and job_id in self._cancelled_jobs:
                return True
            return False
    
    def clear_session(self, session_id: str):
        """Clear cancellation flag for a session (for new requests)"""
        with self._lock:
            self._cancelled_sessions.discard(session_id)
    
    def clear_job(self, job_id: str):
        """Clear cancellation flag for a job"""
        with self._lock:
            self._cancelled_jobs.discard(job_id)
    
    def clear_all_for_session(self, session_id: str):
        """Clear all cancellation flags for a session"""
        with self._lock:
            self._cancelled_sessions.discard(session_id)


# Global cancellation manager instance (shared across the Cloud Run service)
cancellation_manager = CancellationManager()


class ExecutionApi:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        
        # SocketIO client to connect to API layer for progress streaming
        # This creates a client connection (not server) to avoid conflicts
        self.sio_client = socketio.SimpleClient()
        self.api_layer_connected = False

        # Cloud Run / GCS FUSE configuration (REQUIRED for v2 - Cloud Run only)
        # - cloud_run_service.yaml sets: GCS_BUCKET, DATA_DIR, api_layer_url
        self.gcs_bucket = (os.getenv("GCS_BUCKET") or "").strip()
        self.data_dir = (os.getenv("DATA_DIR") or "").strip()
        
        # Validate Cloud Run configuration
        if not self.data_dir:
            raise RuntimeError("DATA_DIR environment variable is required (Cloud Run only mode). This execution layer must run in Cloud Run with GCS FUSE mount.")
        if not self.gcs_bucket:
            raise RuntimeError("GCS_BUCKET environment variable is required (Cloud Run only mode).")
        
        # API layer URL for Socket.IO progress streaming (required for progress updates)
        # Read API layer URL from environment (set by Cloud Run service YAML)
        # Check both uppercase and lowercase variants
        self.api_layer_url = (os.getenv("API_LAYER_URL") or os.getenv("api_layer_url") or "").strip().rstrip("/")
        if self.api_layer_url:
            print(f"[EXECUTION LAYER] ✅ API_LAYER_URL configured: {self.api_layer_url}")
        else:
            print("[EXECUTION LAYER] ⚠️ Warning: API_LAYER_URL not set - progress streaming will be disabled")
        
        # Cancellation manager for stop functionality
        self.cancellation_manager = cancellation_manager
                
        # Cloud Run: Use GCS FUSE mounted directory for locks (inside DATA_DIR)
        # Create a writable locks directory inside the mounted data directory
        self.locks_dir = os.path.join(self.data_dir, '.locks')
        try:
            os.makedirs(self.locks_dir, exist_ok=True)
        except Exception as e:
            # Fallback to /tmp if DATA_DIR/.locks is not writable (shouldn't happen in Cloud Run)
            print(f"[EXECUTION LAYER] ⚠️ Could not create locks dir in DATA_DIR, using /tmp: {e}")
            self.locks_dir = '/tmp'
            os.makedirs(self.locks_dir, exist_ok=True)
        
        # File-based run lock path (per Cloud Run instance) - use writable locks dir
        self.run_lock_path = os.path.join(self.locks_dir, '.analysis_running.lock')
        # Cache job_ids that are invalid for milestone API (prevents repetitive 404 spam).
        self._milestone_invalid_jobs = set()
        
        # Progress tracking for jobs - restored streaming capability
        self.active_jobs = set()
        self.progress_lock = threading.Lock()
        
        # Initialize LLM client singleton (routes based on MODEL_NAME / LLM_PROVIDER env vars)
        try:
            initialize_client()
        except Exception as e:
            print(f"[EXECUTION LAYER] ⚠️ Warning: Could not initialize LLM client: {e}")

        # Register routes 
        self.register_routes()
        # Reuse a single SimpleQnA agent instance to preserve warm caches/kernels.
        self.simpleqna_agent = None
        self.simpleqna_agent_lock = threading.Lock()
        if SimpleQnaDataAnalysisAgent is not None:
            try:
                self.simpleqna_agent = SimpleQnaDataAnalysisAgent()
            except Exception as e:
                print(f"⚠️ Warning: Could not initialize shared SimpleQnA agent: {e}")

    @staticmethod
    def _sanitize_email_for_storage(email: str) -> str:
        """Sanitize email for folder/prefix usage (underscores, lowercased)."""
        if not email:
            return "anonymous"
        return re.sub(r"[^a-zA-Z0-9]+", "_", email.lower()).strip("_")

    @staticmethod
    def _resolve_model_name(requested_model: str | None) -> str:
        """Strictly enforce GPT-5.4 across execution endpoints."""
        model = (requested_model or ENFORCED_MODEL_NAME).strip()
        if model != ENFORCED_MODEL_NAME:
            raise ValueError(
                f"Strict model policy violation: expected '{ENFORCED_MODEL_NAME}', got '{model}'"
            )
        return model

    def _get_data_root(self) -> str:
        """Return Cloud Run mounted data dir (always required in v2)."""
        data_root = (self.data_dir or "").strip()
        if not data_root:
            raise RuntimeError("DATA_DIR is required but not set (Cloud Run only mode)")
        return data_root

    def _resolve_paths(
        self,
        *,
        user_email: str | None,
        user_email_sanitized: str | None,
        job_id: str | None,
        session_id: str | None,
        ):
        """
        Resolve input/output directories for Cloud Run (GCS FUSE mount).
        
        Cloud Run paths (DATA_DIR is required):
          /data/<user_id>/<session_id>/input_data
          /data/<user_id>/<session_id>/output_data/<job_id>      (job flow)
          /data/<user_id>/<session_id>/output_data               (session flow)
        
        Note: v2 execution layer is Cloud Run only - no local/dev fallbacks.
        """
        data_root = self._get_data_root()
        safe_user = (user_email_sanitized or "").strip() or self._sanitize_email_for_storage(user_email or "")

        safe_session = (session_id or "").strip()
        # Session folder is required for Cloud Run path layout. Fall back to "no_session" to avoid crashing.
        if not safe_session:
            safe_session = "no_session"

        input_dir = os.path.join(data_root, safe_user, safe_session, "input_data")
        if job_id:
            output_dir = os.path.join(data_root, safe_user, safe_session, "output_data", job_id)
        else:
            output_dir = os.path.join(data_root, safe_user, safe_session, "output_data")
        return input_dir, output_dir

    def _get_session_pseudonymized_flag(self, session_id: str) -> bool:
        """Read `pseudonymized` flag from Firestore session document (best-effort)."""
        if not session_id:
            return False
        try:
            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app()
            db = firebase_firestore.client()
            doc = db.collection("sessions").document(str(session_id)).get()
            if not doc.exists:
                return False
            data = doc.to_dict() or {}
            return bool(data.get("pseudonymized", False))
        except Exception as e:
            print(f"[EXECUTION LAYER] ⚠️ Could not read pseudonymized flag for session {session_id}: {e}")
            return False
    
    def connect_to_api_layer(self, max_retries=5):
        """
        Connect to API layer's SocketIO server for progress streaming with retry logic.
        
        Cloud Run only: Requires API_LAYER_URL/api_layer_url environment variable to be set.
        This URL should point to the v2 API layer (e.g., Cloudflare tunnel URL during development, 
        or App Engine URL in production).
        """
        if not self.api_layer_url:
            # print(f"[EXECUTION LAYER] ⚠️ Cannot connect: API_LAYER_URL not set")
            return False
        
        retry_count = 0
        base_delay = 1  # 1 second
        
        # while retry_count < max_retries:
        #     try:
        #         if not self.api_layer_connected:
        #             # Ensure URL has no trailing slash for SocketIO
        #             # Use custom SocketIO path /socketio for v2 standalone API.
        #             api_url = self.api_layer_url.rstrip('/')
        #             socketio_url = f"{api_url}/socketio"
        #             print(f"[EXECUTION LAYER] 🔗 Connecting to API layer SocketIO at {socketio_url} (attempt {retry_count + 1}/{max_retries})")
                    
        #             # Use longer timeout for tunnel connections and allow both websocket and polling transports
        #             self.sio_client.connect(
        #                 api_url,
        #                 socketio_path='socketio',
        #                 wait_timeout=20,  # Increased timeout for tunnel connections
        #                 transports=['websocket', 'polling']
        #             )
                    
        #             self.api_layer_connected = True
        #             print(f"[EXECUTION LAYER] ✅ Connected to API layer SocketIO at {socketio_url}")
        #             return True
        #         else:
        #             return True
        #     except Exception as e:
        #         retry_count += 1
        #         error_msg = str(e)
        #         # Truncate long error messages for readability
        #         if len(error_msg) > 200:
        #             error_msg = error_msg[:200] + "..."
        #         print(f"[EXECUTION LAYER] ❌ Failed to connect to API layer (attempt {retry_count}/{max_retries}): {error_msg}")
                
                # if retry_count < max_retries:
                #     delay = base_delay * (2 ** (retry_count - 1))  # Exponential backoff
                #     print(f"[EXECUTION LAYER] ⏳ Retrying connection in {delay} seconds...")
                #     time.sleep(delay)
                # else:
                #     print(f"[EXECUTION LAYER] ❌ Max connection retries reached, giving up")
                #     self.api_layer_connected = False
                #     return False
        
        return False

    def _create_milestone(self, job_id: str, name: str, description: str, data: dict = None):
        """Create a milestone via HTTP API. Used for granular progress from execution layer."""
        if not self.api_layer_url:
            return
        if not job_id:
            return
        if job_id in self._milestone_invalid_jobs:
            return
        secret = (os.getenv("EXECUTION_LAYER_SECRET") or "").strip()
        headers = {"Content-Type": "application/json"}
        if secret:
            headers["X-Execution-Layer-Secret"] = secret
        try:
            url = f"{self.api_layer_url.rstrip('/')}/internal/milestones"
            payload = {"job_id": job_id, "name": name, "description": description}
            if data:
                payload["data"] = data
            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=10,
            )
            if resp.status_code != 200:
                # Job framework milestones require a valid Job Framework job_id.
                # SimpleQnA query_id values are not Job IDs, so suppress repeated 404s.
                if resp.status_code == 404 and "not found" in (resp.text or "").lower():
                    self._milestone_invalid_jobs.add(job_id)
                    return
                print(f"[EXECUTION LAYER] ⚠️ Milestone API returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"[EXECUTION LAYER] ⚠️ Milestone API error: {e}")

    def _emit_progress(self, job_id: str, stage: str, message: str, emoji: str = ""):
        """Emit progress event for a job - RESTORED streaming through SocketIO client with reconnection handling"""
        progress_event = ProgressEvent(job_id, stage, message, 0, emoji)  # Set percentage to 0 since we don't need it
        
        # Emit progress to API layer via SocketIO client
        try:
            if self.api_layer_connected:
                # Emit to API layer which will forward to frontend
                self.sio_client.emit('execution_progress', progress_event.to_dict())
                print(f"[EXECUTION LAYER] 📡 Progress streamed: {emoji} {stage}: {message}")
            else:
                # Try to reconnect if not connected
                print(f"[EXECUTION LAYER] 🔄 Connection lost, attempting to reconnect...")
                if self.connect_to_api_layer():
                    self.sio_client.emit('execution_progress', progress_event.to_dict())
                    print(f"[EXECUTION LAYER] 📡 Progress streamed after reconnection: {emoji} {stage}: {message}")
                else:
                    print(f"[EXECUTION LAYER] ⚠️  Progress (no connection): {emoji} {stage}: {message}")
        except Exception as e:
            print(f"[EXECUTION LAYER] ❌ Error emitting progress: {e}")
            # Mark connection as disconnected and try to reconnect
            self.api_layer_connected = False
            try:
                if self.connect_to_api_layer():
                    self.sio_client.emit('execution_progress', progress_event.to_dict())
                    print(f"[EXECUTION LAYER] 📡 Progress streamed after error recovery: {emoji} {stage}: {message}")
                else:
                    print(f"[EXECUTION LAYER] 📝 Progress (fallback): {emoji} {stage}: {message}")
            except Exception as retry_error:
                print(f"[EXECUTION LAYER] ❌ Retry failed: {retry_error}")
                print(f"[EXECUTION LAYER] 📝 Progress (fallback): {emoji} {stage}: {message}")
    
    # REMOVED: register_socketio_events() - no longer needed since we removed the SocketIO instance
    # All WebSocket communication now handled by main API layer
    
    def register_routes(self):
        """Register all API routes"""

        def _is_lock_stale(lock_path: str) -> bool:
            """Best-effort stale lock detection to avoid permanent 409 after crashes."""
            try:
                ttl = int(os.getenv("ANALYSIS_LOCK_TTL_SECS", "1800"))  # default 30 minutes
            except Exception:
                ttl = 1800
            try:
                p = Path(lock_path)
                if not p.exists():
                    return False
                age = time.time() - p.stat().st_mtime
                return age > ttl
            except Exception:
                return False

        def _clear_lock_if_stale(lock_path: str) -> bool:
            try:
                if _is_lock_stale(lock_path):
                    os.remove(lock_path)
                    print(f"[EXECUTION_API] 🧹 Cleared stale lock: {lock_path}")
                    return True
            except Exception:
                pass
            return False
        
        @self.app.route('/health')
        def health():
            """Health check endpoint"""
            try:
                return jsonify({
                    'status': 'healthy', 
                    'service': 'analysis-execution-api',
                    'data_dir': self.data_dir,
                    'gcs_bucket': self.gcs_bucket
                })
            except Exception as e:
                return jsonify({
                    'status': 'error',
                    'service': 'analysis-execution-api',
                    'error': str(e)
                }), 500
        
        @self.app.route('/cancel', methods=['POST', 'OPTIONS'])
        def cancel_processing():
            """Cancel ongoing processing for a session or job.
            
            This endpoint marks the session/job for cancellation.
            Running agents will check this flag and exit gracefully.
            """
            # Handle CORS preflight request
            if request.method == 'OPTIONS':
                response = jsonify({})
                response.headers.add('Access-Control-Allow-Origin', '*')
                response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
                response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
                return response
            
            data = request.get_json() or {}
            session_id = data.get('session_id')
            job_id = data.get('job_id')
            
            if not session_id and not job_id:
                response = jsonify({'error': 'Missing session_id or job_id'})
                response.headers.add('Access-Control-Allow-Origin', '*')
                return response, 400
            
            cancelled = False
            if session_id:
                cancelled = self.cancellation_manager.cancel_session(session_id)
            if job_id:
                cancelled = self.cancellation_manager.cancel_job(job_id) or cancelled
            
            result = jsonify({
                'status': 'cancelled' if cancelled else 'already_cancelled',
                'session_id': session_id,
                'job_id': job_id,
                'message': 'Cancellation signal sent. Processing will stop at next checkpoint.'
            })
            result.headers.add('Access-Control-Allow-Origin', '*')
            return result
        
        @self.app.route('/cancel/clear', methods=['POST'])
        def clear_cancellation():
            """Clear cancellation flag for a session (called before new request)"""
            data = request.get_json() or {}
            session_id = data.get('session_id')
            job_id = data.get('job_id')
            
            if session_id:
                self.cancellation_manager.clear_session(session_id)
            if job_id:
                self.cancellation_manager.clear_job(job_id)
            
            return jsonify({'status': 'cleared', 'session_id': session_id, 'job_id': job_id})

        @self.app.route('/analyze_job', methods=['POST'])
        def analyze_job():
            """Handle job-based analysis with job-specific folders"""
            # Get analysis request from JSON body
            data = request.get_json()
            if not data or 'query' not in data or 'job_id' not in data:
                return jsonify({'error': 'Missing analysis query or job_id'}), 400
            
            job_id = data['job_id']
            print(f"[EXECUTION_API] ▶️ analyze_job received for job_id={job_id}")
            
            # Create job-specific lock path
            job_lock_path = os.path.join(self.locks_dir, f'{job_id}.lock')
            
            # File-based guard: if lock exists, reject as busy
            if os.path.exists(job_lock_path):
                print(f"[EXECUTION_API] ⚠️ analyze_job busy for job_id={job_id}")
                return jsonify({'status': 'busy', 'error': f'Analysis already in progress for job {job_id}'}), 409
            
            try:
                # Run analysis for the specific job
                analysis_result = self.run_job_analysis(data, job_lock_path)
                
                # Return the result
                print(f"[EXECUTION_API] ✅ analyze_job success for job_id={job_id}")
                return jsonify(analysis_result)
                
            except Exception as e:
                error_msg = f"Job analysis failed: {str(e)}"
                print(error_msg)
                traceback.print_exc()
                return jsonify({
                    'status': 'error',
                    'error': f'Analysis failed: {str(e)}',
                    'error_type': 'analysis_failure',
                    'error_code': 'ANALYSIS_FAILED',
                    'job_id': data.get('job_id', 'unknown'),
                    'message': 'An unexpected error occurred during analysis execution.'
                }), 500
        
        @self.app.route('/simpleqna/analyze_job', methods=['POST'])
        def simpleqna_analyze_job():
            """
            Handle Simple QnA query execution (v2).
            
            Pure execution endpoint - does NOT create jobs or update Firestore.
            Only executes agent logic and returns structured result.
            
            Expected payload:
            {
                "chat_id": "...",      # Chat ID (for metadata only)
                "query_id": "...",     # Query ID (used for path resolution)
                "query": "...",        # User question
                "model": "gpt-5.4",    # LLM model
                "session_id": "...",   # Session ID
                "user_id": "...",      # User ID
                "user_email": "...",   # User email (for path resolution)
                "user_email_sanitized": "..."  # Sanitized email
            }
            
            Returns:
            {
                "answer": "<html>",    # HTML answer
                "metrics": {...},      # Token usage metrics
                "error": null          # Error message if failed
            }
            """
            data = request.get_json()
            if not data or 'query' not in data:
                return jsonify({'error': 'Missing query'}), 400
            
            # Support both v2 format (chat_id/query_id) and legacy format (job_id)
            query_id = data.get('query_id') or data.get('job_id')
            chat_id = data.get('chat_id')
            
            if not query_id:
                return jsonify({'error': 'Missing query_id or job_id'}), 400
            
            # Use query_id for per-query execution pathing.
            # Use chat_id (if provided) as the canonical Job Framework job_id for milestones/progress.
            job_id = query_id
            milestone_job_id = (chat_id or job_id)
            print(f"[EXECUTION_API] ▶️ simpleqna_analyze_job received for query_id={query_id}, chat_id={chat_id}")
            
            # Create job-specific lock path
            job_lock_path = os.path.join(self.locks_dir, f'{job_id}.lock')
            
            # Create file-based run lock to prevent re-entry
            try:
                with open(job_lock_path, 'x') as _f:
                    _f.write('running')
            except FileExistsError:
                print(f"[EXECUTION_API] ⚠️ simpleqna_analyze_job busy for job_id={job_id}")
                return jsonify({'status': 'busy', 'error': f'Analysis already in progress for job {job_id}'}), 409
            
            try:
                # Get job parameters
                user_query = data['query']
                model_name = self._resolve_model_name(data.get('model'))
                session_id = data.get('session_id', '')
                user_email = data.get('user_email')
                user_email_sanitized = data.get('user_email_sanitized')
                
                # Clear any previous cancellation flag for this job (fresh request)
                self.cancellation_manager.clear_job(job_id)
                if session_id:
                    self.cancellation_manager.clear_session(session_id)
                
                # Check if already cancelled before starting
                if self.cancellation_manager.is_cancelled(session_id=session_id, job_id=job_id):
                    print(f"🛑 [CONTAINER] Simple QnA job {job_id} cancelled before processing")
                    return jsonify({
                        'status': 'cancelled',
                        'message': 'Job was cancelled before processing started',
                        'job_id': job_id
                    }), 499
                
                # Resolve job-specific directories (Cloud Run only)
                job_input_dir, job_output_dir = self._resolve_paths(
                    user_email=user_email,
                    user_email_sanitized=user_email_sanitized,
                    job_id=job_id,
                    session_id=session_id,
                )
                
                print(f"🔧 [CLOUD_RUN] Setting up Simple QnA job {job_id} directories:")
                print(f"📥 Cloud Run input dir: {job_input_dir} (session shared)")
                print(f"📤 Cloud Run output dir: {job_output_dir} (job-specific)")
                
                # Create job-specific output directory WITHIN the session mount
                os.makedirs(job_output_dir, exist_ok=True)
                
                print(f"✅ [CLOUD_RUN] Starting Simple QnA job analysis {job_id}")
                print(f"📝 Query: {user_query}")
                print(f"🤖 Model: {model_name}")
                
                # RESTORED: Connect to API layer for real-time progress streaming
                self.connect_to_api_layer()
                
                # Track active job
                with self.progress_lock:
                    self.active_jobs.add(job_id)
                
                # Initialize metrics state
                metrics: MetricsState = {
                    "total_tokens": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "successful_requests": 0
                }
                
                # Emit initial progress against the canonical chat job id (for Job Framework compatibility).
                self._emit_progress(milestone_job_id, "Query Analysis", "Processing your question", "🧠")
                
                # State for QnA pipeline
                state = {
                    "original_query": user_query,
                    "results": [],
                    "error": None,
                    "history": [],
                    "metrics": metrics,
                    "model_name": model_name,
                    "output_dir": job_output_dir,  # Job-specific output dir
                    "input_dir": job_input_dir,    # Session shared input dir
                    "job_id": job_id,
                    "milestone_job_id": milestone_job_id,
                    "query_id": query_id,
                    "chat_id": chat_id,
                    "session_id": session_id,  # For cancellation tracking
                    "session_pseudonymized": self._get_session_pseudonymized_flag(session_id),
                    "cancellation_manager": self.cancellation_manager,  # For cancellation checking
                    "progress_callback": lambda stage, message, emoji="": self._emit_progress(milestone_job_id, stage, message, emoji),
                    "milestone_callback": lambda desc, name=None, data=None: self._create_milestone(
                        milestone_job_id, name or (desc[:50].replace(" ", "_").lower()), desc, data
                    ),
                }
                
                # Run the Simple QnA agent
                if self.simpleqna_agent is None:
                    raise ImportError("SimpleQnaDataAnalysisAgent is not available. Check if simpleqna_data_analysis.py exists.")
                with self.simpleqna_agent_lock:
                    result_state = asyncio.run(self.simpleqna_agent.ainvoke(state))
                
                # Check if processing was cancelled
                if result_state.get("cancelled"):
                    print(f"🛑 [CONTAINER] Simple QnA job {job_id} was cancelled by user")
                    return jsonify({
                        'status': 'cancelled',
                        'message': 'Job was cancelled by user',
                        'job_id': milestone_job_id,
                        'query_id': query_id,
                        'metrics': result_state.get("metrics", {})
                    }), 499
                
                # Calculate costs
                costs = self.calculate_costs(result_state.get("metrics", metrics), model_name)
                print(f"Job {job_id} Simple QnA analysis completed with costs: {json.dumps(costs)}")
                
                # Ensure report file exists and is named correctly
                report_file = os.path.join(job_output_dir, 'analysis_report.html')
                
                # If agent saved to analysis_summary.html, rename it
                summary_file = os.path.join(job_output_dir, 'analysis_summary.html')
                if os.path.exists(summary_file) and not os.path.exists(report_file):
                    try:
                        os.rename(summary_file, report_file)
                        print(f"✅ Renamed {summary_file} to {report_file}")
                    except Exception as rename_err:
                        print(f"⚠️ Could not rename summary file: {str(rename_err)}")
                
                # If report doesn't exist but we have HTML in state, save it
                if not os.path.exists(report_file):
                    html_content = result_state.get("final_output_html") or result_state.get("final_html_report", "")
                    if html_content:
                        try:
                            with open(report_file, 'w', encoding='utf-8') as f:
                                f.write(html_content)
                            print(f"✅ Saved HTML report to: {report_file}")
                        except Exception as save_err:
                            print(f"⚠️ Could not save report file: {str(save_err)}")
                
                # Emit completion
                self._emit_progress(milestone_job_id, "Analysis Complete", "Response generated successfully", "✅")
                
                # Extract HTML answer from result state
                html_answer = result_state.get("final_output_html") or result_state.get("final_html_report", "")
                
                # Compose v2 response format (pure execution result)
                if result_state.get("error"):
                    response = {
                        "answer": None,
                        "metrics": result_state.get("metrics", metrics),
                        "error": result_state.get("error")
                    }
                elif result_state.get("cancelled"):
                    response = {
                        "answer": None,
                        "metrics": result_state.get("metrics", metrics),
                        "error": "Query execution was cancelled"
                    }
                else:
                    response = {
                        "answer": html_answer,
                        "metrics": result_state.get("metrics", metrics),
                        "error": None
                    }
                
                print(f"[EXECUTION_API] ✅ simpleqna_analyze_job success for query_id={query_id}")
                return jsonify(response)
                
            except Exception as e:
                print(f"[EXECUTION_API] ❌ simpleqna_analyze_job error: {str(e)}")
                traceback.print_exc()
                try:
                    err_metrics = metrics
                except NameError:
                    err_metrics = {}
                return jsonify({
                    "status": "error",
                    "error": f"QnA execution failed: {str(e)}",
                    "error_type": "simpleqna_failure",
                    "error_code": "SIMPLEQNA_FAILED",
                    "job_id": job_id,
                    "metrics": err_metrics
                }), 500
            finally:
                # Remove job lock
                try:
                    if os.path.exists(job_lock_path):
                        os.remove(job_lock_path)
                except Exception:
                    pass
                
                # Remove from active jobs
                with self.progress_lock:
                    self.active_jobs.discard(job_id)
        
        # Keep legacy analyze endpoint for backward compatibility
        @self.app.route('/analyze', methods=['POST'])
        def analyze():
            # Get analysis request from JSON body
            data = request.get_json()
            if not data or 'query' not in data:
                return jsonify({'error': 'Missing analysis query'}), 400
            
            # Force-release any previous lock so a new request can start analysis immediately.
            # (Cloud Run: locks are per-container, so this is mainly for recovery from crashes)
            force = str(data.get("force", "true")).strip().lower() in {"1", "true", "yes", "on"}
            if os.path.exists(self.run_lock_path):
                if force:
                    try:
                        os.remove(self.run_lock_path)
                        print(f"[EXECUTION_API] 🧹 Cleared previous legacy analyze lock (force=true): {self.run_lock_path}")
                    except Exception as e:
                        print(f"[EXECUTION_API] ⚠️ Could not clear legacy analyze lock: {e}")
                else:
                    # Auto-clear stale locks to prevent permanent busy state
                    _clear_lock_if_stale(self.run_lock_path)
                    if os.path.exists(self.run_lock_path):
                        print(f"[EXECUTION_API] ⚠️ analyze busy (legacy endpoint)")
                        return jsonify({'status': 'busy', 'error': 'Analysis already in progress'}), 409
            
            # Ensure output directory exists
            # os.makedirs(self.output_dir, exist_ok=True)
            
            try:
                # Run analysis in a separate thread
                print("[EXECUTION_API] ▶️ analyze start (legacy endpoint)")
                analysis_result = self.run_analysis(data)
                
                # Return the result
                print("[EXECUTION_API] ✅ analyze success (legacy endpoint)")
                return jsonify(analysis_result)
                
            except Exception as e:
                error_msg = f"Analysis failed: {str(e)}"
                print(error_msg)
                traceback.print_exc()
                return jsonify({
                    'status': 'error',
                    'error': f'Legacy analysis failed: {str(e)}',
                    'error_type': 'legacy_analysis_failure',
                    'error_code': 'LEGACY_ANALYSIS_FAILED',
                    'message': 'An unexpected error occurred during legacy analysis execution.'
                }), 500
        
        @self.app.route('/simpleqna/query', methods=['POST'])
        def simpleqna_query():
            """Simple QnA endpoint - works directly with sessions, no jobs required"""
            data = request.get_json()
            if not data or 'query' not in data:
                return jsonify({'error': 'Missing query'}), 400

            session_id = data.get('session_id')
            if not session_id:
                return jsonify({'error': 'Missing session_id'}), 400
            user_email = data.get('user_email')
            user_email_sanitized = data.get('user_email_sanitized')

            # Clear any previous cancellation flag for this session (fresh request)
            self.cancellation_manager.clear_session(session_id)
            
            # Check if already cancelled before starting (edge case: cancel came before request)
            if self.cancellation_manager.is_cancelled(session_id=session_id):
                print(f"🛑 [CLOUD_RUN] Request cancelled before processing for session {session_id}")
                return jsonify({
                    'status': 'cancelled',
                    'message': 'Request was cancelled',
                    'session_id': session_id
                }), 499  # Client Closed Request

            user_query = data['query']
            model_name = self._resolve_model_name(data.get('model'))
            
            # Resolve session-level directories (Cloud Run only)
            session_input_dir, session_output_dir = self._resolve_paths(
                user_email=user_email,
                user_email_sanitized=user_email_sanitized,
                job_id=None,
                session_id=session_id,
            )
            
            # Create session output directory if needed (already exists via mount, but ensure it's accessible)
            os.makedirs(session_output_dir, exist_ok=True)
            
            print(f"🔧 [CLOUD_RUN] Simple QnA query for session {session_id}")
            print(f"📥 Cloud Run input dir: {session_input_dir}")
            print(f"📤 Cloud Run output dir: {session_output_dir}")
            print(f"📝 Query: {user_query}")
            print(f"🤖 Model: {model_name}")
            
            # Connect to API layer for progress streaming (optional for Simple QnA)
            self.connect_to_api_layer()
            
            # Initialize metrics state
            metrics: MetricsState = {
                "total_tokens": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "successful_requests": 0
            }
            
            # State for QnA pipeline (no job_id needed)
            state = {
                "original_query": user_query,
                "results": [],
                "error": None,
                "history": [],
                "metrics": metrics,
                "model_name": model_name,
                "output_dir": session_output_dir,  # Session-level output dir
                "input_dir": session_input_dir,    # Session input dir
                "session_id": session_id,  # Use session_id instead of job_id
                "session_pseudonymized": self._get_session_pseudonymized_flag(session_id),
                "cancellation_manager": self.cancellation_manager,  # Pass for cancellation checking
            }
            
            try:
                # Run the Simple QnA agent
                if self.simpleqna_agent is None:
                    raise ImportError("SimpleQnaDataAnalysisAgent is not available. Check if simpleqna_data_analysis.py exists.")
                with self.simpleqna_agent_lock:
                    result_state = asyncio.run(self.simpleqna_agent.ainvoke(state))
                
                # Check if processing was cancelled
                if result_state.get("cancelled"):
                    print(f"🛑 [CLOUD_RUN] Simple QnA cancelled for session {session_id}")
                    return jsonify({
                        'status': 'cancelled',
                        'message': 'Processing was cancelled by user',
                        'session_id': session_id
                    }), 499
                
                # Calculate costs
                costs = self.calculate_costs(result_state.get("metrics", metrics), model_name)
                print(f"Simple QnA query completed with costs: {json.dumps(costs)}")
                
                # Get HTML response from state
                html_content = result_state.get("final_output_html") or result_state.get("final_html_report", "")
                
                # Save HTML to session directory (optional, for history)
                if html_content:
                    report_file = os.path.join(session_output_dir, f'qna_response_{int(time.time())}.html')
                    try:
                        with open(report_file, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                        print(f"✅ Saved QnA response to: {report_file}")
                    except Exception as save_err:
                        print(f"⚠️ Could not save response file: {str(save_err)}")
                
                # Extract text response from HTML (simple extraction)
                import re
                text_response = ""
                if html_content:
                    # Remove HTML tags for text response
                    text_response = re.sub(r'<[^>]+>', '', html_content).strip()
                    # Limit length for response
                    if len(text_response) > 1000:
                        text_response = text_response[:1000] + "..."
                
                # Return response
                response = {
                    "status": "success" if not result_state.get("error") else "error",
                    "response": text_response or "Response generated successfully",
                    "html": html_content,
                    "metrics": result_state.get("metrics", metrics),
                    "costs": costs,
                    "session_id": session_id
                }
                
                print(f"[EXECUTION_API] ✅ simpleqna_query success for session={session_id}")
                return jsonify(response)
                
            except Exception as e:
                print(f"❌ [SIMPLEQNA] Error in Simple QnA query: {str(e)}")
                traceback.print_exc()
                return jsonify({
                    "status": "error",
                    "error": f"QnA query failed: {str(e)}",
                    "error_type": "simpleqna_failure",
                    "error_code": "SIMPLEQNA_FAILED",
                    "session_id": session_id
                }), 500
            
    def calculate_costs(self, metrics: MetricsState, model_name) -> Dict[str, Any]:
        """Calculate costs based on metrics and model pricing"""
        # Default pricing per 1K tokens
        model_pricing = {
            "gpt-5.4": {"input": 0.002, "output": 0.008},
            "gpt-5.4-mini": {"input": 0.0004, "output": 0.0016},
        }
        
        # Get pricing for model or use default
        pricing = model_pricing.get(model_name, model_pricing["gpt-5.4"])
        
        try:
            # Convert to cost per 1000 tokens
            prompt_cost = (metrics["prompt_tokens"] / 1000) * pricing["input"]
            completion_cost = (metrics["completion_tokens"] / 1000) * pricing["output"]
            total_cost = prompt_cost + completion_cost
            
            print(f"Calculated costs for {model_name}: prompt=${prompt_cost:.4f}, completion=${completion_cost:.4f}, total=${total_cost:.4f}")
            
            return {
                "prompt_cost": float(prompt_cost),
                "completion_cost": float(completion_cost),
                "total_cost": float(total_cost),
                "model": model_name,
                "prompt_tokens": metrics["prompt_tokens"],
                "completion_tokens": metrics["completion_tokens"],
                "total_tokens": metrics["total_tokens"]
            }
        except Exception as e:
            print(f"Error calculating costs: {str(e)}")
            return {
                "prompt_cost": 0.0,
                "completion_cost": 0.0,
                "total_cost": 0.0,
                "model": model_name,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            }

    def _generate_unsupported_query_report(self, user_query: str, unsupported_reason: str, query_analysis: dict) -> str:
        """Generate a simple HTML report for unsupported queries - just title and reason"""
        
        html_report = f"""<!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="utf-8"/>
            <meta content="width=device-width, initial-scale=1" name="viewport"/>
            <title>Query Cannot Be Supported</title>
            <style>
                body {{
                    margin: 0;
                    padding: 0;
                    font-family: 'Segoe UI', Arial, sans-serif;
                    background: #fff;
                }}
                .container {{
                    max-width: 800px;
                    margin: 0 auto;
                    padding: 2rem;
                }}
                .title {{
                    color: #e67e22;
                    font-size: 1.4rem;
                    font-weight: 600;
                    margin-bottom: 1.5rem;
                    text-align: center;
                }}
                .reason {{
                    color: #333;
                    font-size: 1.05rem;
                    line-height: 1.6;
                    text-align: justify;
                }}
            </style>
            </head>
            <body>
            <div class="container">
                <div class="title">Query Cannot Be Supported</div>
                <div class="reason">{unsupported_reason}</div>
            </div>
            </body>
            </html>"""
        
        return html_report


    def create_graph(self):
        """
        Build a graph with Data Analysis Agent as the main coordinator.
        Data Analysis Agent uses EDA Agent and Code Agent internally.
        """
        from langgraph.graph import Graph
        
        g = Graph()
        
        # Create main data analysis agent
        data_analysis_agent = DataAnalysisAgent(output_dir=self.output_dir)
        
        # Add node
        g.add_node("data_analysis_agent", data_analysis_agent)
        
        # Set entry and exit points
        g.set_entry_point("data_analysis_agent")
        g.set_finish_point("data_analysis_agent")
        
        return g.compile()

    def run_job_analysis(self, data, job_lock_path):
        """Run analysis for a specific job with JOB-SPECIFIC directories (FIXED VERSION)"""
        # Ensure job_id is always available for error/cleanup paths
        job_id = data.get('job_id', 'unknown')
        try:
            # Get job parameters (needed for path resolution and idempotency check)
            job_id = data['job_id']
            user_query = data['query']
            model_name = self._resolve_model_name(data.get('model'))
            analysis_mode = data.get('analysis_mode', 'slim')
            user_email = data.get('user_email')
            user_email_sanitized = data.get('user_email_sanitized')
            session_id = data.get('session_id', '')
            session_pseudonymized = self._get_session_pseudonymized_flag(session_id)
            
            # Cloud Run: GCS FUSE mount provides session-level directory structure
            host_input_dir = data.get('input_dir', '')
            host_output_dir = data.get('output_dir', '')
            
            # Resolve Cloud Run paths (/data via GCS FUSE mount)
            job_input_dir, job_output_dir = self._resolve_paths(
                user_email=user_email,
                user_email_sanitized=user_email_sanitized,
                job_id=job_id,
                session_id=session_id,
            )
            
            # IDEMPOTENCY: If job already completed (report exists), return cached result without re-running.
            # Prevents duplicate analysis from HTTP retries (e.g. connection drop after container sends 200).
            report_file = os.path.join(job_output_dir, 'analysis_report.html')
            if os.path.exists(report_file):
                print(f"[EXECUTION_API] ⏭️ analyze_job idempotent skip for job_id={job_id} - report already exists")
                return {
                    'status': 'success',
                    'job_id': job_id,
                    'costs': {},
                    'metrics': {},  # Don't double-count tokens
                    'output_dir': job_output_dir,
                    'host_output_dir': host_output_dir,
                    'container_output_dir': job_output_dir,
                    'session_mount': os.path.join('execution_layer', 'output_data'),
                    'report_processing': {},
                    'analysis_completed_early': False,
                    'completion_reason': '',
                    'final_message': '',
                    '_idempotent_cache': True,
                }
            
            # Create file-based run lock to prevent re-entry
            try:
                with open(job_lock_path, 'x') as _f:
                    _f.write('running')
            except FileExistsError:
                return {
                    'status': 'busy',
                    'error': f'Analysis already in progress for job {job_id}'
                }
            
            print(f"🔧 [CLOUD_RUN] Setting up job {job_id} directories:")
            print(f"📥 Cloud Run input dir: {job_input_dir} (session shared)")
            print(f"📤 Cloud Run output dir: {job_output_dir} (job-specific)")
            print(f"🏠 Host output will be at: {host_output_dir}")
            
            # Create job-specific output directory WITHIN the session mount
            # No need to create input dir - using shared session input
            os.makedirs(job_output_dir, exist_ok=True)

            # Ensure image artifacts only for pseudonymized sessions.
            if session_pseudonymized:
                try:
                    os.makedirs(os.path.join(job_output_dir, "image_utils"), exist_ok=True)
                    if not os.path.exists(os.path.join(job_output_dir, "image_master.json")):
                        write_image_master_atomic(job_output_dir, load_image_master(job_output_dir))
                except Exception:
                    pass
            
            print(f"✅ [CLOUD_RUN] Starting job analysis {job_id} with JOB-SPECIFIC directories")
            print(f"🔍 Job input dir (Cloud Run): {job_input_dir}")
            print(f"💾 Job output dir (Cloud Run): {job_output_dir}")
            print(f"🗂️ Host output dir: {host_output_dir}")
            print(f"📝 Query: {user_query}")
            print(f"🤖 Model: {model_name}")
            print(f"🧭 Analysis mode: {analysis_mode}")
            
            # RESTORED: Connect to API layer for real-time progress streaming
            self.connect_to_api_layer()
            
            # VERIFICATION: Check job directory within Cloud Run mount
            try:
                # Get session-level output dir to check existing jobs
                _, session_output_dir = self._resolve_paths(
                    user_email=user_email,
                    user_email_sanitized=user_email_sanitized,
                    job_id=None,
                    session_id=session_id,
                )
                if os.path.exists(session_output_dir):
                    existing_items = [d for d in os.listdir(session_output_dir) if os.path.isdir(os.path.join(session_output_dir, d))]
                    print(f"📂 [CLOUD_RUN] Existing job directories in session mount: {existing_items}")
                    print(f"📊 [CLOUD_RUN] Total job directories in this session: {len(existing_items)}")
                    
                    # Check if our job directory was created
                    if os.path.exists(job_output_dir):
                        print(f"✅ [CLOUD_RUN] Job directory created successfully: {job_output_dir}")
                    else:
                        print(f"❌ [CLOUD_RUN] ERROR: Job directory NOT created: {job_output_dir}")
                else:
                    print(f"❌ [CLOUD_RUN] ERROR: Session mount directory doesn't exist: {session_output_dir}")
                    
            except Exception as e:
                print(f"❌ [CLOUD_RUN] Error checking job directories: {str(e)}")
            
            # Track active job
            with self.progress_lock:
                self.active_jobs.add(job_id)
            
            # Initialize metrics state
            metrics: MetricsState = {
                "total_tokens": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "successful_requests": 0
            }
            
            # Clear any previous cancellation flag for this job (fresh request)
            self.cancellation_manager.clear_job(job_id)
            if session_id:
                self.cancellation_manager.clear_session(session_id)
            
            # Check if already cancelled before starting
            if self.cancellation_manager.is_cancelled(session_id=session_id, job_id=job_id):
                print(f"🛑 [CLOUD_RUN] Job {job_id} cancelled before processing")
                return {
                    'status': 'cancelled',
                    'message': 'Job was cancelled before processing started',
                    'job_id': job_id
                }
            
            # Initialize analysis state with JOB-SPECIFIC paths and progress callback  
            state = {
                "original_query": user_query,
                "query_analysis":{
                    "user_intent": "",
                    "sub_queries": [],
                    "plan": [],
                    "expected_output":[]
                    },
                "command": "",
                "eda_outputs": [],
                "image_paths": [],
                "eda_summary": "",
                "eda_vision_analysis": "",
                "hypothesis_findings": [],
                "hypothesis_summary":"",
                "patterns_found": [],
                "final_html_report": "",
                "narrator_frame_text": "",
                "narrator_file_analyses": [],
                # Populated after analysis completes; kept here so state shape is explicit.
                "report_processing": {},
                # Populated by DataAnalysisAgent after reading domain_directory.json
                "pseudonymized_columns_map": {},
                "error": None,
                "history": [],
                "last_code": "",
                "last_output": "",
                "last_error": None,
                "metrics": metrics,  
                "model_name": model_name,
                "analysis_mode": analysis_mode,
                "output_dir": job_output_dir,  # ✅ FIXED: Use JOB-SPECIFIC output dir
                "input_dir": job_input_dir,    # ✅ FIXED: Use JOB-SPECIFIC input dir
                "job_id": job_id,
                "session_id": session_id,  # For cancellation tracking
                "session_pseudonymized": session_pseudonymized,
                # "container_id": container_id,  # Legacy Docker field - not used in Cloud Run
                "user_email": data.get('user_email'),
                "user_token_info": data.get('user_token_info'),
                "cancellation_manager": self.cancellation_manager,  # For cancellation checking
                "progress_callback": lambda stage, message, emoji="": self._emit_progress(job_id, stage, message, emoji),
                "milestone_callback": lambda desc, name=None, data=None: self._create_milestone(
                    job_id, name or (desc[:50].replace(" ", "_").lower()), desc, data
                ),
            }

            # Create data analysis agent with JOB-SPECIFIC output directory  
            data_analysis_agent = DataAnalysisAgent(output_dir=job_output_dir)
            
            from langgraph.graph import Graph
            g = Graph()
            g.add_node("data_analysis_agent", data_analysis_agent)
            g.set_entry_point("data_analysis_agent")
            g.set_finish_point("data_analysis_agent")
            compiled = g.compile()
            
            print("Running job analysis")
            
            # Run the analysis
            state = asyncio.run(compiled.ainvoke(state))
            
            # Check if processing was cancelled
            if state.get("cancelled"):
                print(f"🛑 [CLOUD_RUN] Job {job_id} analysis was cancelled by user")
                return {
                    'status': 'cancelled',
                    'message': 'Job was cancelled by user',
                    'job_id': job_id,
                    'metrics': state.get("metrics", {}),
                }

            # Fail explicitly when the pipeline reports an internal error.
            if state.get("error"):
                err_msg = str(state.get("error"))
                print(f"❌ [CLOUD_RUN] Job {job_id} failed in pipeline: {err_msg}")
                return {
                    'status': 'error',
                    'error': err_msg,
                    'error_type': 'analysis_pipeline_error',
                    'error_code': 'ANALYSIS_PIPELINE_ERROR',
                    'job_id': job_id,
                    'metrics': state.get("metrics", {}),
                }
            
            # Check if query is not supportable (early return from query analysis)
            if state.get("query_not_supportable", False):
                unsupported_reason = state.get("unsupported_reason", "Unable to answer this query with available data.")
                query_analysis = state.get("query_analysis", {})
                
                print(f"⚠️ [CLOUD_RUN] Job {job_id} - Query not supportable: {unsupported_reason}")
                
                # Calculate costs for the query analysis that was performed
                costs = self.calculate_costs(state["metrics"], state["model_name"])
                
                # Generate a minimal HTML report explaining the issue
                unsupported_html = self._generate_unsupported_query_report(
                    user_query=user_query,
                    unsupported_reason=unsupported_reason,
                    query_analysis=query_analysis
                )
                
                # Save the unsupported query report
                report_file = os.path.join(job_output_dir, 'analysis_report.html')
                try:
                    with open(report_file, 'w', encoding='utf-8') as f:
                        f.write(unsupported_html)
                    print(f"✅ [CONTAINER] Saved unsupported query report to: {report_file}")
                except Exception as save_err:
                    print(f"⚠️ [CONTAINER] Could not save unsupported report: {str(save_err)}")
                
                return {
                    'status': 'success',
                    'job_id': job_id,
                    'query_not_supportable': True,
                    'unsupported_reason': unsupported_reason,
                    'query_analysis': query_analysis,
                    'costs': costs,
                    'metrics': state.get("metrics", {}),
                    'output_dir': job_output_dir,
                    'host_output_dir': host_output_dir,
                    'container_output_dir': job_output_dir,
                    'message': 'The query cannot be answered with the available data. Please see the unsupported_reason for details and suggestions.',
                    'analysis_skipped': True,
                    'analysis_skip_reason': 'query_not_supportable'
                }
            
            # Calculate final costs
            costs = self.calculate_costs(state["metrics"], state["model_name"])
            print(f"Job {job_id} analysis completed with costs: {json.dumps(costs)}")
            
            # Path to the report file in JOB-SPECIFIC output directory
            report_file = os.path.join(job_output_dir, 'analysis_report.html')
            
            try:
                if os.path.exists(report_file):
                    with open(report_file, 'r', encoding='utf-8') as f:
                        report_str = f.read()
                else:
                    report_str = state.get("final_html_report", "<p>No report generated</p>")
                    # Save report to file for future reference
                    if report_str and report_str != "<p>No report generated</p>":
                        with open(report_file, 'w', encoding='utf-8') as f:
                            f.write(report_str)
                        print(f"HTML report saved to: {report_file}")
                    else:
                        print("Warning: No final_html_report found in state")
                        print(f"State keys: {list(state.keys())}")
            except Exception as e:
                print(f"Error reading/writing report file: {str(e)}")
                report_str = f"<p>Error with report: {str(e)}</p>"

            if not report_str or report_str == "<p>No report generated</p>":
                return {
                    'status': 'error',
                    'error': 'Analysis finished without generating analysis_report.html',
                    'error_type': 'missing_report',
                    'error_code': 'MISSING_REPORT',
                    'job_id': job_id,
                    'metrics': state.get("metrics", {}),
                }
            
            print(f"✅ [CONTAINER] Job {job_id} analysis completed successfully")
            
            # FINAL VERIFICATION: Check job output files within session mount
            try:
                if os.path.exists(job_output_dir):
                    output_files = os.listdir(job_output_dir)
                    print(f"📄 [CONTAINER] Files created in job directory {job_id}:")
                    for file in output_files:
                        file_path = os.path.join(job_output_dir, file)
                        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                        print(f"   - {file} ({file_size} bytes)")
                    print(f"📊 [CONTAINER] Total files created for this job: {len(output_files)}")
                    
                    # Also show all job directories in this session
                    session_mount = os.path.join('execution_layer', 'output_data')
                    all_jobs = [d for d in os.listdir(session_mount) if os.path.isdir(os.path.join(session_mount, d))]
                    print(f"🗂️ [CONTAINER] All job directories in this session: {all_jobs}")
                else:
                    print(f"❌ [CONTAINER] ERROR: Job output directory doesn't exist at completion: {job_output_dir}")
                    
            except Exception as e:
                print(f"❌ [CONTAINER] Error checking final output files: {str(e)}")
            
            # Prepare response with job-specific information
            response = {
                'status': 'success',
                'job_id': job_id,
                # 'report': report_str,
                'costs': costs,
                'metrics': state["metrics"],
                'output_dir': job_output_dir,  # Container job directory
                'host_output_dir': host_output_dir,  # Host job directory path
                'container_output_dir': job_output_dir,  # Container path for debugging
                'session_mount': os.path.join('execution_layer', 'output_data'),  # Session mount point
                'report_processing': state.get("report_processing", {}),
                # Include token exhaustion indicators for job status determination
                'analysis_completed_early': state.get('analysis_completed_early', False),
                'completion_reason': state.get('completion_reason', ''),
                'final_message': state.get('final_message', '')
            }
            return response
        except Exception as e:
            # Emit error progress
            self._emit_progress(job_id, "Analysis Failed", f"Something went wrong", "❌")
            print(f"Error in job {data.get('job_id', 'unknown')} analysis: {str(e)}")
            traceback.print_exc()
            # Include metrics so API can add tokens even on failure (tokens were consumed)
            try:
                err_metrics = metrics
            except NameError:
                err_metrics = {}
            return {
                'status': 'error',
                'error': f'Job execution failed: {str(e)}',
                'error_type': 'job_execution_failure',
                'error_code': 'JOB_EXECUTION_FAILED',
                'job_id': data.get('job_id'),
                'user_email': data.get('user_email', ''),
                'message': 'An unexpected error occurred during job execution.',
                'metrics': err_metrics
            }
        finally:
            # Remove job-specific run lock
            try:
                if os.path.exists(job_lock_path):
                    os.remove(job_lock_path)
            except Exception:
                pass
            
            # Remove job from active jobs
            with self.progress_lock:
                self.active_jobs.discard(job_id)
    
    def run_analysis(self, data):
        """Run analysis for the given data"""
        try:
            # Create file-based run lock to prevent re-entry
            try:
                with open(self.run_lock_path, 'x') as _f:
                    _f.write('running')
            except FileExistsError:
                return {
                    'status': 'busy',
                    'error': 'Analysis already in progress'
                }
            # FIXED: No longer clean up directories - preserve job outputs!
            # Just ensure output directory exists
            try:
                os.makedirs(self.output_dir, exist_ok=True)
                print(f"✅ FIXED: /analyze endpoint no longer deletes job directories")
                print(f"📁 Output directory: {self.output_dir}")
            except Exception as e:
                print(f"Error creating output directory {self.output_dir}: {e}")
            

            # Get analysis parameters
            user_query = data['query']
            model_name = self._resolve_model_name(data.get('model'))
            # Cloud Run: container_id not used (each Cloud Run instance is isolated)
            # container_id = data.get('container_id', '')  # Legacy Docker field - not used in Cloud Run
            user_email = data.get('user_email', '')
            user_token_info = data.get('user_token_info', {})

            # Cloud Run only: Resolve input directory from request or use Cloud Run path structure
            # Note: Legacy endpoint - should use /analyze_job instead for Cloud Run
            requested_input_dir = (data.get('input_dir') or "").strip()
            if requested_input_dir and os.path.exists(requested_input_dir):
                input_dir = requested_input_dir
            else:
                # Cloud Run: Use DATA_DIR structure (requires user_email and session_id)
                # This is a legacy endpoint - prefer /analyze_job for proper Cloud Run support
                user_email = data.get('user_email', '')
                session_id = data.get('session_id', '')
                if user_email and session_id:
                    # Use Cloud Run path resolution
                    user_email_sanitized = data.get('user_email_sanitized')
                    input_dir, _ = self._resolve_paths(
                        user_email=user_email,
                        user_email_sanitized=user_email_sanitized,
                        job_id=None,
                        session_id=session_id,
                    )
                else:
                    # Fallback: construct path manually (not recommended)
                    safe_user = self._sanitize_email_for_storage(user_email) if user_email else "anonymous"
                    safe_session = session_id or "no_session"
                    input_dir = os.path.join(self.data_dir, safe_user, safe_session, "input_data")

            print(f"Starting analysis for query: {user_query} using model: {model_name}")
            print(f"📊 [TOKEN MANAGEMENT] User: {user_email}, Tokens: {user_token_info.get('used_token', 0)}/{user_token_info.get('issued_token', 0)} (remaining: {user_token_info.get('remaining_token', 0)})")
            print(f"📥 [EXECUTION_API] Using input_dir: {input_dir}")
            
            # Check if user has enough tokens to proceed
            if user_token_info and user_token_info.get('remaining_token', 0) <= 0:
                return jsonify({
                    "status": "error",
                    "error": "🚫 INSUFFICIENT TOKENS: User has no tokens remaining. Contact admin for more tokens.",
                    "error_type": "insufficient_tokens",
                    "error_code": "TOKEN_INSUFFICIENT",
                    "user_email": user_email,
                    "token_info": {
                        "used_token": user_token_info.get('used_token', 0),
                        "issued_token": user_token_info.get('issued_token', 0),
                        "remaining_token": user_token_info.get('remaining_token', 0)
                    },
                    "message": "Please contact administrator to purchase additional tokens."
                }), 402  # 402 Payment Required - more appropriate for token limits
            
            # Initialize metrics state
            metrics: MetricsState = {
                "total_tokens": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "successful_requests": 0
            }
            
            # Initialize analysis state
            state = {
                "original_query": user_query,
                "query_analysis":{
                    "user_intent": "",
                    "sub_queries": [],
                    "plan": [],
                    "expected_output":[]
                    },
                "command": "",
                "eda_outputs": [],
                "image_paths": [],
                "eda_summary": "",
                "eda_vision_analysis": "",
                "hypothesis_findings": [],
                "hypothesis_summary":"",
                "patterns_found": [],
                "final_html_report": "",
                "narrator_frame_text": "",
                "narrator_file_analyses": [],
                "error": None,
                "history": [],
                "last_code": "",
                "last_output": "",
                "last_error": None,
                "metrics": metrics,  
                "model_name": self._resolve_model_name(data.get('model')),
                "user_email": user_email,
                "session_pseudonymized": self._get_session_pseudonymized_flag(data.get('session_id', '')),
                "user_token_info": user_token_info,  # Pass token info for internal tracking
                "input_dir": input_dir,  # Cloud Run: resolved from DATA_DIR structure
                "output_dir": self.output_dir,  # Legacy: not used in Cloud Run mode (use job-specific dirs)
                # "container_id": container_id  # Legacy Docker field - not used in Cloud Run
            }
 
            # Import TokenLimitExceededException for handling token limits
            from agents.token_manager import TokenLimitExceededException
            
            try:
                # Run analysis
                print("Initializing analysis")
                compiled = self.create_graph()
                print("Running analysis")
                state = asyncio.run(compiled.ainvoke(state))
                
            except TokenLimitExceededException as e:
                # Token limit exceeded - immediately stop and return error
                print(f"🚫 [EXECUTION_API] Token limit exceeded: {str(e)}")
                return jsonify({
                    "status": "error",
                    "error": f"🚫 ANALYSIS STOPPED: {str(e)}",
                    "error_type": "token_limit_exceeded",
                    "error_code": "TOKEN_LIMIT_EXCEEDED",
                    "user_email": user_email,
                    "job_id": data.get('job_id', 'unknown'),
                    "metrics": state.get("metrics", {}),
                    "token_info": user_token_info,
                    "message": "Token limit reached during analysis. Contact administrator for more tokens."
                }), 402  # 402 Payment Required - semantically correct for token limits
            
            # Check if query is not supportable (early return from query analysis)
            if state.get("query_not_supportable", False):
                unsupported_reason = state.get("unsupported_reason", "Unable to answer this query with available data.")
                query_analysis = state.get("query_analysis", {})
                
                print(f"⚠️ [EXECUTION_API] Query not supportable: {unsupported_reason}")
                
                # Calculate costs for the query analysis that was performed
                costs = self.calculate_costs(state["metrics"], state["model_name"])
                
                # Generate a minimal HTML report explaining the issue
                unsupported_html = self._generate_unsupported_query_report(
                    user_query=user_query,
                    unsupported_reason=unsupported_reason,
                    query_analysis=query_analysis
                )
                
                # Save the unsupported query report
                report_file = os.path.join(self.output_dir, 'analysis_report.html')
                try:
                    with open(report_file, 'w', encoding='utf-8') as f:
                        f.write(unsupported_html)
                    print(f"✅ Saved unsupported query report to: {report_file}")
                except Exception as save_err:
                    print(f"⚠️ Could not save unsupported report: {str(save_err)}")
                
                return {
                    'status': 'success',
                    'query_not_supportable': True,
                    'unsupported_reason': unsupported_reason,
                    'query_analysis': query_analysis,
                    'report': unsupported_html,
                    'costs': costs,
                    'metrics': state.get("metrics", {}),
                    'output_dir': self.output_dir,
                    'message': 'The query cannot be answered with the available data. Please see the unsupported_reason for details and suggestions.',
                    'analysis_skipped': True,
                    'analysis_skip_reason': 'query_not_supportable'
                }
            
            # Calculate final costs
            costs = self.calculate_costs(state["metrics"], state["model_name"])
            print(f"Analysis completed with costs: {json.dumps(costs)}")
            
            # Path to the report file
            report_file = os.path.join(self.output_dir, 'analysis_report.html')
            
            try:
                if os.path.exists(report_file):
                    with open(report_file, 'r', encoding='utf-8') as f:
                        report_str = f.read()
                else:
                    report_str = state.get("final_html_report", "<p>No report generated</p>")
                    # Save report to file for future reference
                    with open(report_file, 'w', encoding='utf-8') as f:
                        f.write(report_str)
            except Exception as e:
                print(f"Error reading/writing report file: {str(e)}")
                report_str = f"<p>Error with report: {str(e)}</p>"
            
            print("Analysis completed successfully")
            
            # Prepare response
            response = {
                'status': 'success',
                'report': report_str,
                'costs': costs,
                'metrics': state["metrics"],
                'output_dir': self.output_dir,
                # Include token exhaustion indicators for job status determination
                'analysis_completed_early': state.get('analysis_completed_early', False),
                'completion_reason': state.get('completion_reason', ''),
                'final_message': state.get('final_message', '')
            }
            return response
        except Exception as e:
            print(f"Error in analysis: {str(e)}")
            traceback.print_exc()
            return {
                'status': 'error',
                'error': f'Legacy analysis execution failed: {str(e)}',
                'error_type': 'legacy_execution_failure',
                'error_code': 'LEGACY_EXECUTION_FAILED',
                'user_email': user_email,
                'message': 'An unexpected error occurred during legacy analysis execution.',
                'metrics': {}
            }
        finally:
            # Remove run lock
            try:
                if os.path.exists(self.run_lock_path):
                    os.remove(self.run_lock_path)
            except Exception:
                pass
    
    def run(self, host='0.0.0.0', port=5001, debug=True):
        """Run the execution API server (Flask only, no WebSocket)"""
        # Cloud Run mandates listening on $PORT (usually 8080). Prefer env var when present.
        try:
            port = int(os.getenv("PORT", str(port)))
        except Exception:
            pass
        print(f"[EXECUTION LAYER] Starting Flask server on {host}:{port}")
        self.app.run(debug=debug, host=host, port=port)

def create_app():
    """Create and configure the Flask app"""
    api = ExecutionApi()
    return api.app

if __name__ == '__main__':
    api = ExecutionApi()
    api.run() 