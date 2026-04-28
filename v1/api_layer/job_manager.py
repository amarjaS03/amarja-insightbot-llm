import uuid
import time
import threading
import json
import os
import requests
# Docker import commented out - Cloud Run only
# import docker
from typing import Dict, Any, Optional, Tuple
from enum import Enum
from datetime import datetime
import traceback
from api_layer.firebase_user_manager import FirebaseUserManager
from logger import add_log
from .firebase_data_models import JobDocument, get_data_manager
from .log_aggregator import persist_combined_logs_to_gcs
from utils.env import init_env
from utils.marketingCampaign import marketing_campaign_logs_silent
from utils.sanitize import sanitize_email_for_storage

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobManager:
    """Manages asynchronous analysis jobs"""
    
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.jobs_file = "jobs.json"
        
        # Base directories for job data
        self.base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.input_base_dir = os.path.join(self.base_dir, 'execution_layer', 'input_data')
        self.output_base_dir = os.path.join(self.base_dir, 'execution_layer', 'output_data')
        
        # Firebase data manager
        self.data_manager = get_data_manager()
        
        # Docker client commented out - Cloud Run only
        # # Docker client for container logs
        # try:
        #     self.docker_client = docker.from_env()
        # except Exception as e:
        #     print(f"⚠️ [DOCKER] Failed to initialize Docker client: {str(e)}")
        #     self.docker_client = None
        self.docker_client = None
        
        # Load existing jobs from file
        self._load_jobs_from_file()
    
    def _load_jobs_from_file(self):
        """Load existing jobs from JSON file"""
        if os.path.exists(self.jobs_file):
            try:
                with open(self.jobs_file, 'r') as f:
                    stored_jobs = json.load(f)
                
                # Restore jobs
                for job_id, job_data in stored_jobs.items():
                    self.jobs[job_id] = job_data
                    # Attach job_id so restored jobs appear in per-job backend logs
                    add_log(f"Restored job {job_id} from file", job_id=job_id)
                        
            except Exception as e:
                add_log(f"Error loading jobs from file: {str(e)}", job_id=None)
                self.jobs = {}
    
    def _save_jobs_to_file(self):
        """Save current jobs to JSON file"""
        try:
            # Convert jobs to JSON-serializable format
            jobs_to_save = {}
            for job_id, job_info in self.jobs.items():
                jobs_to_save[job_id] = {
                    'job_id': job_info['job_id'],
                    'session_id': job_info['session_id'],
                    'status': job_info['status'].value if isinstance(job_info['status'], JobStatus) else job_info['status'],
                    'query': job_info['query'],
                    'model': job_info['model'],
                    'created_at': job_info['created_at'],
                    'started_at': job_info.get('started_at'),
                    'completed_at': job_info.get('completed_at'),
                    'error': job_info.get('error'),
                    'container_port': job_info.get('container_port'),
                    'output_dir': job_info.get('output_dir'),
                    'input_dir': job_info.get('input_dir'),
                    'user_info': job_info.get('user_info', {})
                }
            
            with open(self.jobs_file, 'w') as f:
                json.dump(jobs_to_save, f, indent=2)
                
        except Exception as e:
            add_log(f"Error saving jobs to file: {str(e)}", job_id=None)
    
    def create_job(self, session_id: str, query: str, model: str = "gpt-5.4", 
                   session_info: Optional[Dict[str, Any]] = None,
                   user_info: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """Create a new analysis job with JOB prefix"""
        # Generate job ID with JOB prefix for better identification  
        job_id = f"JOB_{str(uuid.uuid4())}"
        
        with self.lock:
            try:
                # add_log(job_id, f"Creating new job: {job_id} for session: {session_id}")
                
                # CORRECTED: Keep original session-based design for HOST
                # Input: Session-based (shared across jobs in session)
                # Output: Session-based on HOST, job-based INSIDE container
                
                session_input_dir = os.path.join(self.input_base_dir, session_id)
                session_output_dir = os.path.join(self.output_base_dir, session_id)
                
                print(f"🔧 [JOB MANAGER] Setting up job {job_id} (session: {session_id})")
                print(f"📥 Session input dir (host): {session_input_dir}")
                print(f"📤 Session output dir (host): {session_output_dir}")
                print(f"💡 Container will create job subdir: /app/execution_layer/output_data/{job_id}/")
                
                # Ensure session input directory exists (for shared session data)
                if session_id:
                    os.makedirs(session_input_dir, exist_ok=True)
                    print(f"✅ Session input directory ensured: {session_input_dir}")
                
                # Ensure session output directory exists (container mount point)
                os.makedirs(session_output_dir, exist_ok=True)
                print(f"✅ Session output directory ensured: {session_output_dir}")
                
                # Job-specific paths for reference (used by container)
                job_input_dir = session_input_dir  # Jobs share session input
                job_output_dir = os.path.join(session_output_dir, job_id)  # Job subdir in session output
                
                # Store job information
                job_info = {
                    'job_id': job_id,
                    'session_id': session_id,
                    'status': JobStatus.PENDING,
                    'query': query,
                    'model': model,
                    'created_at': time.time(),
                    'started_at': None,
                    'completed_at': None,
                    'error': None,
                    'container_port': session_info.get('container_port') if session_info else None,
                    'output_dir': job_output_dir,
                    'input_dir': job_input_dir,
                    'user_info': user_info or {}  # Store user information for job ownership
                }
                
                self.jobs[job_id] = job_info
                self._save_jobs_to_file()
                
                # add_log(job_id, f"Job {job_id} created successfully")
                return job_id, job_info
                
            except Exception as e:
                # add_log(job_id, f"Error creating job {job_id}: {str(e)}")
                raise
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job information by ID"""
        with self.lock:
            # Try direct lookup first
            job_info = self.jobs.get(job_id)
            if job_info:
                # Ensure status is properly typed
                if isinstance(job_info['status'], str):
                    job_info['status'] = JobStatus(job_info['status'])
                return job_info.copy()
            
            # If not found, reload from file (in case file was updated externally)
            self._load_jobs_from_file()
            job_info = self.jobs.get(job_id)
            if job_info:
                # Ensure status is properly typed
                if isinstance(job_info['status'], str):
                    job_info['status'] = JobStatus(job_info['status'])
                return job_info.copy()
            
            return None
    
    def update_job_status(self, job_id: str, status: JobStatus, 
                         error: Optional[str] = None) -> bool:
        """Update job status"""
        with self.lock:
            job_info = self.jobs.get(job_id)
            if not job_info:
                return False
            
            old_status = job_info['status']
            job_info['status'] = status
            
            if status == JobStatus.RUNNING and job_info.get('started_at') is None:
                job_info['started_at'] = time.time()
            elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                job_info['completed_at'] = time.time()
                
            if error:
                job_info['error'] = error
            
            self._save_jobs_to_file()
            # Record status transition in job-specific logs for premium tracking
            add_log(job_id, f"Job {job_id} status updated: {old_status} -> {status}")
            return True
    
    def start_job_execution(self, job_id: str, session_manager, job_type: str = 'standard') -> bool:
        """Start asynchronous job execution"""
        job_info = self.get_job(job_id)
        if not job_info:
            return False
        
        def execute_job():
            # Cloud Run only mode - always enabled
            use_cloud_run = True  # Cloud Run only mode
            deployed_service_name = None
            try:
                constants = init_env()
                def _log_marketing(action_key: str, email: str, error_note: str = "", session_id_arg: str = "", job_id_arg: str = ""):
                    try:
                        firebase_user_manager = FirebaseUserManager()
                        user = firebase_user_manager.get_user_by_email(email)
                        print(f"🔍 [MARKETING] User: {user}")
                        # if user and user.get('registration_type') == "external":
                        action_cfg = (constants.get("marketing_action_types", {}) or {}).get(action_key, {})
                        if not action_cfg:
                            return
                        # Build augmented note with context when error is provided
                        base_note = action_cfg.get("note") or ""
                        if error_note:
                            context = f"SessionID: {session_id_arg} JobID: {job_id_arg}"
                        else:
                            context = ""
                        payload = {
                            "actionType": action_cfg.get("actionType"),
                            "note": f"{base_note}{context}",
                            "email": (email or "").lower()
                        }
                        marketing_campaign_logs_silent(payload)
                        # else:
                        #     print(f"🚫 [MARKETING] User {email} is not an external user")
                        #     pass
                    except Exception:
                        pass

                add_log(job_id, f"Starting job execution: {job_id}")
                self.update_job_status(job_id, JobStatus.RUNNING)
                try:
                    user_email_for_marketing = (job_info.get('user_info', {}) or {}).get('email', '')
                    _log_marketing("BACKEND_REPORT_ANALYSIS_STARTED", user_email_for_marketing)
                except Exception:
                    pass
                
                # Get session info for container communication
                session_id = job_info['session_id']
                session_info = session_manager.get_session_container(session_id)
                
                if not session_info:
                    error_msg_local = f"Session {session_id} not found or inactive"
                    self.update_job_status(job_id, JobStatus.FAILED, 
                                         error_msg_local)
                    try:
                        _log_marketing(
                            "BACKEND_REPORT_ANALYSIS_FAILED",
                            (job_info.get('user_info', {}) or {}).get('email', ''),
                            error_note=error_msg_local,
                            session_id_arg=session_id,
                            job_id_arg=job_id
                        )
                    except Exception:
                        pass
                    return
                
                # Cloud Run only mode - always use Cloud Run
                user_email = ((job_info.get('user_info', {}) or {}).get('email') or "").lower()
                try:
                    add_log(f"[JOB_MANAGER] Deploying Cloud Run service for job: {job_id}", job_id=job_id)
                    deploy = session_manager.deploy_job_service(job_id=job_id, session_id=session_id, user_email=user_email)
                    deployed_service_name = deploy.get("service_name")
                    service_url = deploy.get("service_url")
                    if not service_url:
                        raise RuntimeError(f"Cloud Run service deployed but no URL returned for service: {deployed_service_name}")
                    path = '/analyze_job' if job_type != 'simpleqna' else '/simpleqna/analyze_job'
                    container_url = f"{service_url.rstrip('/')}{path}"
                    add_log(f"[JOB_MANAGER] Cloud Run service deployed successfully: {container_url}", job_id=job_id)
                except Exception as deploy_err:
                    error_msg_deploy = f"Failed to deploy Cloud Run service: {str(deploy_err)} | traceback: {traceback.format_exc()}"
                    add_log(f"[JOB_MANAGER] {error_msg_deploy}", job_id=job_id)
                    self.update_job_status(job_id, JobStatus.FAILED, error_msg_deploy)
                    try:
                        _log_marketing(
                            "BACKEND_REPORT_ANALYSIS_FAILED",
                            user_email,
                            error_note=error_msg_deploy,
                            session_id_arg=session_id,
                            job_id_arg=job_id
                        )
                    except Exception:
                        pass
                    return
                
                # Create requests session with timeout
                session_req = requests.Session()
                
                try:
                    # Extract user email for token management
                    user_info = job_info.get('user_info', {})
                    user_email = user_info.get('email', '')
                    
                    # Get current user token info ONCE at the start
                    user_token_info = {}
                    if user_email and self.data_manager:
                        user = self.data_manager.get_user(user_email)
                        if user:
                            user_token_info = {
                                'used_token': user.used_token,
                                'issued_token': user.issued_token,
                                'remaining_token': user.issued_token - user.used_token
                            }
                            print(f"📊 [JOB_MANAGER] User tokens: {user.used_token}/{user.issued_token} (remaining: {user_token_info['remaining_token']})")
                        else:
                            print(f"⚠️ [JOB_MANAGER] User '{user_email}' not found in database")
                    
                    # Send job execution request with user token info for internal tracking
                    container_response = session_req.post(
                        container_url,
                        json={
                            'job_id': job_id,
                            'query': job_info['query'],
                            'model': job_info['model'],
                            'session_id': session_id,
                            'input_dir': job_info['input_dir'],
                            'output_dir': job_info['output_dir'],
                            'user_email': user_email,
                            'user_email_sanitized': sanitize_email_for_storage(user_email) if user_email else '',
                            'user_token_info': user_token_info  # Pass current token info for internal tracking
                        },
                        timeout=3600  # 1 hour timeout for analysis
                    )
                    
                    if container_response.status_code == 200:
                        result = container_response.json()
                        print("Job completed successfully")
                        print(f"Result: {result}")
                        
                        # Determine job status to decide on token update
                        execution_status = result.get('status', 'success')
                        has_error = result.get('error') is not None
                        analysis_completed_early = result.get('analysis_completed_early', False)
                        completion_reason = result.get('completion_reason', '')
                        token_limit_reached = completion_reason == 'token_limit_reached'
                        
                        # Job is successful if no errors and no token limit reached
                        job_is_successful = not (execution_status == 'error' or has_error or token_limit_reached)
                        
                        # Check if report exists - if report exists, consider job successful even if there were minor errors
                        output_dir = job_info.get('output_dir', '')
                        report_path = os.path.join(output_dir, 'analysis_report.html') if output_dir else None
                        report_exists = report_path and os.path.exists(report_path)
                        
                        # If report exists, override job_is_successful to True (report generation is the ultimate success indicator)
                        if report_exists:
                            print(f"✅ [REPORT CHECK] Report found at {report_path} - marking job as successful despite execution status")
                            job_is_successful = True
                            # Clear error flags if report exists (report generation succeeded)
                            if execution_status == 'error' and not has_error:
                                execution_status = 'success'
                        
                        # Update user tokens ONCE at the end if tokens were consumed 
                        # (regardless of success/failure, as long as tokens were actually used)
                        if user_email and user_token_info:
                            metrics = result.get('metrics', {})
                            total_tokens_used = metrics.get('total_tokens', 0)
                            
                            if total_tokens_used > 0:
                                print(f"📊 [TOKEN UPDATE] Job used {total_tokens_used:,} tokens for user {user_email}")
                                
                                # Check if user would exceed limit
                                current_used = user_token_info.get('used_token', 0)
                                issued_tokens = user_token_info.get('issued_token', 0)
                                new_total = current_used + total_tokens_used
                                
                                if new_total > issued_tokens:
                                    print(f"⚠️ [TOKEN WARNING] Job would exceed token limit! {new_total} > {issued_tokens}")
                                    # Note: Job already completed, but warn about limit
                                
                                # Update user's token count in database
                                try:
                                    update_success = self.data_manager.update_user_tokens(user_email, total_tokens_used)
                                    if update_success:
                                        print(f"✅ [TOKEN UPDATE] Updated user {user_email}: +{total_tokens_used:,} tokens (Total: {new_total:,}/{issued_tokens:,})")
                                    else:
                                        print(f"❌ [TOKEN UPDATE] Failed to update tokens for user {user_email}")
                                except Exception as token_error:
                                    print(f"❌ [TOKEN UPDATE] Error updating tokens: {str(token_error)}")
                            else:
                                print(f"📊 [TOKEN UPDATE] No tokens consumed for job {job_id}")
                        
                        # Update job status based on actual success (check report existence)
                        if job_is_successful:
                            add_log(job_id, f"Job {job_id} completed successfully")
                            self.update_job_status(job_id, JobStatus.COMPLETED)
                        else:
                            error_msg_final = result.get('error', 'Job execution completed with errors')
                            add_log(job_id, f"Job {job_id} completed with errors: {error_msg_final}")
                            self.update_job_status(job_id, JobStatus.FAILED, error_msg_final)
                        
                        # Save job output to Firestore (will use current job status)
                        try:
                            firestore_success = self.save_job_to_firestore(job_id, result)
                            if firestore_success:
                                if job_is_successful:
                                    print(f"📊 [PROGRESS] 🔥 Analysis Complete - Results saved to database")
                                else:
                                    print(f"📊 [PROGRESS] 🔥 Analysis completed with errors - Results saved to database")
                            else:
                                print(f"⚠️ [PROGRESS] 🔥 Analysis Complete - Database save failed but analysis completed")
                        except Exception as firestore_error:
                            print(f"❌ [FIRESTORE ERROR] Failed to save to database: {str(firestore_error)}")
                            add_log(f"Firestore save error for job {job_id}: {str(firestore_error)}", job_id=job_id)
                        
                        # Only log marketing success if job actually succeeded
                        if job_is_successful:
                            try:
                                _log_marketing("BACKEND_REPORT_ANALYSIS_COMPLETE", user_email, session_id_arg=session_id, job_id_arg=job_id)
                            except Exception:
                                pass
                        else:
                            try:
                                _log_marketing(
                                    "BACKEND_REPORT_ANALYSIS_FAILED",
                                    user_email,
                                    error_note=result.get('error', 'Job completed with errors'),
                                    session_id_arg=session_id,
                                    job_id_arg=job_id
                                )
                            except Exception:
                                pass
                    elif container_response.status_code == 402:
                        # Token limit exceeded - handle gracefully
                        try:
                            error_data = container_response.json()
                            error_msg = error_data.get('error', 'Token limit exceeded')
                            print(f"🚫 [TOKEN LIMIT] Job {job_id} stopped: {error_msg}")
                            
                            # Save failed job to Firestore 
                            try:
                                failed_result = {
                                    'status': 'error',
                                    'error': error_msg,
                                    'metrics': error_data.get('metrics', {}),
                                    'costs': {'total_cost': 0, 'total_tokens': 0}
                                }
                                firestore_success = self.save_job_to_firestore(job_id, failed_result)
                                if firestore_success:
                                    print(f"📊 [FAILED JOB] Failed job {job_id} saved to Firestore")
                            except Exception as firestore_error:
                                print(f"❌ [FIRESTORE ERROR] Failed to save failed job to database: {str(firestore_error)}")
                            
                            self.update_job_status(job_id, JobStatus.FAILED, f"TOKEN_LIMIT_EXCEEDED: {error_msg}")
                            try:
                                _log_marketing(
                                    "BACKEND_REPORT_ANALYSIS_FAILED",
                                    user_email,
                                    error_note=error_msg,
                                    session_id_arg=session_id,
                                    job_id_arg=job_id
                                )
                            except Exception:
                                pass
                        except:
                            error_msg = f"Token limit exceeded (HTTP 402): {container_response.text}"
                            print(f"🚫 [TOKEN LIMIT] Job {job_id} failed: {error_msg}")
                            
                            # Save failed job to Firestore 
                            try:
                                failed_result = {
                                    'status': 'error',
                                    'error': error_msg,
                                    'metrics': {},
                                    'costs': {'total_cost': 0, 'total_tokens': 0}
                                }
                                self.save_job_to_firestore(job_id, failed_result)
                            except Exception as firestore_error:
                                print(f"❌ [FIRESTORE ERROR] Failed to save failed job to database: {str(firestore_error)}")
                            
                            self.update_job_status(job_id, JobStatus.FAILED, error_msg)
                            try:
                                _log_marketing(
                                    "BACKEND_REPORT_ANALYSIS_FAILED",
                                    user_email,
                                    error_note=error_msg,
                                    session_id_arg=session_id,
                                    job_id_arg=job_id
                                )
                            except Exception:
                                pass
                    else:
                        # Other HTTP errors (400, 500, etc.)
                        try:
                            error_data = container_response.json()
                            error_msg = f"Analysis failed: {error_data.get('error', container_response.text)}"
                            error_type = error_data.get('error_type', 'unknown_error')
                            print(f"❌ [JOB FAILED] {error_type}: {error_msg}")
                            
                            # Save failed job to Firestore 
                            try:
                                failed_result = {
                                    'status': 'error',
                                    'error': error_msg,
                                    'metrics': error_data.get('metrics', {}),
                                    'costs': {'total_cost': 0, 'total_tokens': 0}
                                }
                                firestore_success = self.save_job_to_firestore(job_id, failed_result)
                                if firestore_success:
                                    print(f"📊 [FAILED JOB] Failed job {job_id} saved to Firestore")
                            except Exception as firestore_error:
                                print(f"❌ [FIRESTORE ERROR] Failed to save failed job to database: {str(firestore_error)}")
                                
                        except:
                            error_msg = f"Container API returned error: {container_response.status_code} - {container_response.text}"
                            
                            # Save failed job to Firestore 
                            try:
                                failed_result = {
                                    'status': 'error',
                                    'error': error_msg,
                                    'metrics': {},
                                    'costs': {'total_cost': 0, 'total_tokens': 0}
                                }
                                self.save_job_to_firestore(job_id, failed_result)
                            except Exception as firestore_error:
                                print(f"❌ [FIRESTORE ERROR] Failed to save failed job to database: {str(firestore_error)}")
                        
                        add_log(job_id, f"Job {job_id} failed: {error_msg}")
                        self.update_job_status(job_id, JobStatus.FAILED, error_msg)
                        try:
                            _log_marketing(
                                "BACKEND_REPORT_ANALYSIS_FAILED",
                                user_email,
                                error_note=error_msg,
                                session_id_arg=session_id,
                                job_id_arg=job_id
                            )
                        except Exception:
                            pass
                        
                except requests.RequestException as e:
                    error_msg = f"Error communicating with container API: {str(e)} | traceback: {traceback.format_exc()}"
                    
                    # Save failed job to Firestore 
                    try:
                        failed_result = {
                            'status': 'error',
                            'error': error_msg,
                            'metrics': {},
                            'costs': {'total_cost': 0, 'total_tokens': 0}
                        }
                        firestore_success = self.save_job_to_firestore(job_id, failed_result)
                        if firestore_success:
                            print(f"📊 [FAILED JOB] Failed job {job_id} saved to Firestore")
                    except Exception as firestore_error:
                        print(f"❌ [FIRESTORE ERROR] Failed to save failed job to database: {str(firestore_error)}")
                    
                    add_log(job_id, f"Job {job_id} failed: {error_msg}")
                    self.update_job_status(job_id, JobStatus.FAILED, error_msg)
                    try:
                        _log_marketing(
                            "BACKEND_REPORT_ANALYSIS_FAILED",
                            user_email,
                            error_note=error_msg,
                            session_id_arg=session_id,
                            job_id_arg=job_id
                        )
                    except Exception:
                        pass
                    
            except Exception as e:
                error_msg = f"Job execution failed: {str(e)} | traceback: {traceback.format_exc()}"
                
                # Save failed job to Firestore 
                try:
                    failed_result = {
                        'status': 'error',
                        'error': error_msg,
                        'metrics': {},
                        'costs': {'total_cost': 0, 'total_tokens': 0}
                    }
                    firestore_success = self.save_job_to_firestore(job_id, failed_result)
                    if firestore_success:
                        print(f"📊 [FAILED JOB] Failed job {job_id} saved to Firestore")
                except Exception as firestore_error:
                    print(f"❌ [FIRESTORE ERROR] Failed to save failed job to database: {str(firestore_error)}")
                
                add_log(job_id, f"Job {job_id} failed: {error_msg}")
                self.update_job_status(job_id, JobStatus.FAILED, error_msg)
                try:
                    _log_marketing(
                        "BACKEND_REPORT_ANALYSIS_FAILED",
                        (job_info.get('user_info', {}) or {}).get('email', ''),
                        error_note=error_msg,
                        session_id_arg=job_info.get('session_id', ''),
                        job_id_arg=job_id
                    )
                except Exception:
                    pass
            finally:
                # Cloud Run cleanup (best-effort): always delete per-job service once job is finished.
                if deployed_service_name:
                    try:
                        add_log(f"[JOB_MANAGER] Cleaning up Cloud Run service: {deployed_service_name}", job_id=job_id)
                        session_manager.delete_job_service(service_name=deployed_service_name, job_id=job_id)
                    except Exception as cleanup_err:
                        add_log(f"[JOB_MANAGER] Warning: Failed to cleanup Cloud Run service {deployed_service_name}: {str(cleanup_err)}", job_id=job_id)
                # Aggregate and upload combined logs to GCS (user/date/session/job-wise path)
                try:
                    # Refresh job_info to capture final times/status
                    current = self.get_job(job_id) or job_info
                    session_id_final = current.get('session_id')
                    created_at = current.get('created_at')
                    started_at = current.get('started_at')
                    completed_at = current.get('completed_at')
                    user_email_final = self._extract_user_email_from_job(current)
                    
                    container_log_path = None
                    
                    urls = persist_combined_logs_to_gcs(
                        job_id=job_id,
                        session_id=session_id_final,
                        user_email=user_email_final,
                        container_logs_path=container_log_path,
                        created_at=created_at,
                        started_at=started_at,
                        completed_at=completed_at,
                    )
                    # Attach job_id to log entry so it is captured in backend logs
                    add_log(f"Combined logs uploaded for {job_id}: {urls}", job_id=job_id)
                except Exception as agg_err:
                    add_log(
                        f"Log aggregation/upload failed for {job_id}: {str(agg_err)} | traceback: {traceback.format_exc()}",
                        job_id=job_id,
                    )
        
        # Start job execution in background thread
        job_thread = threading.Thread(target=execute_job, daemon=True)
        job_thread.start()
        
        return True
    
    def get_job_report_path(self, job_id: str) -> Optional[str]:
        """Get the path to the job's analysis report"""
        job_info = self.get_job(job_id)
        if not job_info or job_info['status'] != JobStatus.COMPLETED:
            return None
            
        output_dir = job_info.get('output_dir')
        if not output_dir:
            return None
            
        report_path = os.path.join(output_dir, 'analysis_report.html')
        if os.path.exists(report_path):
            return report_path
            
        return None
    
    def cleanup_old_jobs(self, max_age_hours: int = 168):  # Increased to 7 days (168 hours)
        """Clean up jobs older than max_age_hours - only clean up completed/failed jobs"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        with self.lock:
            jobs_to_cleanup = []
            for job_id, job_info in self.jobs.items():
                # Only clean up jobs that are completed, failed, or cancelled
                job_status = job_info['status']
                if isinstance(job_status, JobStatus):
                    status_str = job_status.value
                else:
                    status_str = str(job_status)
                
                # Only clean up finished jobs that are old enough
                if (status_str in ['completed', 'failed', 'cancelled'] and 
                    current_time - job_info['created_at'] > max_age_seconds):
                    jobs_to_cleanup.append(job_id)
            
            for job_id in jobs_to_cleanup:
                add_log(f"Cleaning up old completed job: {job_id}", job_id=job_id)
                self._cleanup_job(job_id)
    
    def _cleanup_job(self, job_id: str):
        """Clean up a specific job and its associated files"""
        job_info = self.jobs.get(job_id)
        if job_info:
            try:
                # Remove job directories
                import shutil
                
                input_dir = job_info.get('input_dir')
                if input_dir and os.path.exists(input_dir):
                    shutil.rmtree(input_dir)
                    
                output_dir = job_info.get('output_dir')  
                if output_dir and os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                
                # Remove from jobs dict
                del self.jobs[job_id]
                self._save_jobs_to_file()
                
                add_log(f"Job {job_id} cleaned up successfully", job_id=job_id)
                
            except Exception as e:
                add_log(f"Error cleaning up job {job_id}: {str(e)}", job_id=job_id)
    
    def get_jobs_by_session(self, session_id: str) -> list:
        """Get all jobs for a specific session"""
        with self.lock:
            session_jobs = []
            for job_id, job_info in self.jobs.items():
                if job_info.get('session_id') == session_id:
                    job_copy = job_info.copy()
                    if isinstance(job_copy['status'], JobStatus):
                        job_copy['status'] = job_copy['status'].value
                    session_jobs.append(job_copy)
            return session_jobs
    
    def get_jobs_by_user(self, user_email: str) -> list:
        """Get all jobs for a specific user"""
        with self.lock:
            user_jobs = []
            for job_id, job_info in self.jobs.items():
                job_user_info = job_info.get('user_info', {})
                if job_user_info.get('email', '').lower() == user_email.lower():
                    job_copy = job_info.copy()
                    if isinstance(job_copy['status'], JobStatus):
                        job_copy['status'] = job_copy['status'].value
                    user_jobs.append(job_copy)
            return user_jobs
    
    def _extract_user_email_from_job(self, job_info: Dict[str, Any]) -> Optional[str]:
        """Extract user email from job info for Firestore path"""
        user_info = job_info.get('user_info', {})
        return user_info.get('email')
    
    def _calculate_total_cost(self, metrics: Dict[str, Any]) -> float:
        """Calculate total cost based on token usage"""
        # Pricing for GPT-4o-mini (example pricing - adjust as needed)
        input_cost_per_1k = 0.00015  # $0.00015 per 1K input tokens
        output_cost_per_1k = 0.0006  # $0.0006 per 1K output tokens
        
        input_tokens = metrics.get('prompt_tokens', 0)
        output_tokens = metrics.get('completion_tokens', 0)
        
        input_cost = (input_tokens / 1000) * input_cost_per_1k
        output_cost = (output_tokens / 1000) * output_cost_per_1k
        
        return round(input_cost + output_cost, 6)
    
    def save_job_to_firestore(self, job_id: str, execution_response: Dict[str, Any]) -> bool:
        """
        Save job output to Firestore according to the data model [[memory:6942292]]
        
        Args:
            job_id: Job identifier
            execution_response: Response from container execution containing metrics, costs, etc.
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            job_info = self.get_job(job_id)
            if not job_info:
                add_log(f"❌ Job {job_id} not found for Firestore save", job_id=job_id)
                return False
            
            # Extract user email for Firestore path
            user_email = self._extract_user_email_from_job(job_info)
            if not user_email:
                add_log(f"❌ No user email found for job {job_id}, cannot save to Firestore", job_id=job_id)
                return False
            
            session_id = job_info['session_id']
            
            # Extract metrics from execution response
            metrics = execution_response.get('metrics', {})
            costs = execution_response.get('costs', {})
            
            # Get current job status from jobs.json (should already be updated to COMPLETED)
            # This ensures consistency between jobs.json and Firestore
            current_status = job_info.get('status')
            current_status_str = getattr(current_status, 'value', current_status) if hasattr(current_status, 'value') else str(current_status)
            
            # Determine job status based on execution response (fallback if status not set)
            execution_status = execution_response.get('status', 'success')
            has_error = execution_response.get('error') is not None
            
            # Check for token exhaustion indicators (from graceful completion)
            analysis_completed_early = execution_response.get('analysis_completed_early', False)
            completion_reason = execution_response.get('completion_reason', '')
            token_limit_reached = completion_reason == 'token_limit_reached'
            
            # Use current job status if it's already set to 'completed', otherwise determine from execution response
            if current_status_str == 'completed':
                job_status = "success"  # Job is successful if status is already completed
            else:
                # Job is failed if: execution failed OR has error OR token limit was reached
                job_status = "failed" if (execution_status == 'error' or has_error or token_limit_reached) else "success"
            
            print(f"📊 [JOB_STATUS] Job {job_id} status: {job_status}")
            print(f"📊 [JOB_STATUS] Factors: execution_status={execution_status}, has_error={has_error}, token_limit_reached={token_limit_reached}")
            
            # Upload analysis report to Firebase Storage ONLY for successful jobs
            output_dir = job_info.get('output_dir', '')
            
            # Check if depseudonymized_report.html exists, otherwise use analysis_report.html
            depseudonymized_report_path = os.path.join(output_dir, 'depseudonymized_report.html')
            analysis_report_path = os.path.join(output_dir, 'analysis_report.html')
            
            # Prefer depseudonymized_report.html if it exists, otherwise use analysis_report.html
            if os.path.exists(depseudonymized_report_path):
                report_path = depseudonymized_report_path
                print(f"📤 [PROGRESS] Using depseudonymized_report.html for upload")
                add_log(f"Using depseudonymized_report.html for job {job_id} upload", job_id=job_id)
            else:
                report_path = analysis_report_path
            
            # Firebase Storage path: sessionId/jobId/analysis_report.html
            # Always upload as analysis_report.html in storage (even if source is depseudonymized_report.html)
            storage_path = f"{session_id}/{job_id}/analysis_report.html"
            
            # Upload file and get Firebase Storage URL - SKIP ERROR REPORTS
            report_url = ""
            if job_status == "success" and os.path.exists(report_path):
                try:
                    firebase_storage_url = self.data_manager.crud.upload_file_to_storage(report_path, storage_path)
                    if firebase_storage_url:
                        report_url = firebase_storage_url  # Use Firebase Storage URL ONLY
                        print(f"📤 [PROGRESS] 🔗 Report uploaded to Firebase Storage: {report_url}")
                        add_log(f"✅ Report uploaded to Firebase Storage for job {job_id}: {report_url}", job_id=job_id)
                    else:
                        print(f"❌ [PROGRESS] 🔗 Failed to upload report to Firebase Storage - Empty URL will be saved")
                        add_log(f"❌ Failed to upload report to Firebase Storage for job {job_id} - JobDocument will have empty report_url", job_id=job_id)
                        # Use empty string instead of local path - ensures ONLY Firebase Storage URLs or empty
                        report_url = ""
                except Exception as upload_error:
                    print(f"❌ [STORAGE ERROR] Failed to upload report: {str(upload_error)}")
                    add_log(f"Storage upload error for job {job_id}: {str(upload_error)}", job_id=job_id)
                    # Use empty string instead of local path - ensures ONLY Firebase Storage URLs or empty
                    report_url = ""
            elif job_status == "failed":
                if token_limit_reached:
                    print(f"🚫 [TOKEN EXHAUSTED] Not uploading partial report to Firebase Storage for token-exhausted job {job_id}")
                    add_log(f"Skipped Firebase Storage upload for token-exhausted job {job_id} - partial reports not stored in GCP", job_id=job_id)
                else:
                    print(f"🚫 [SKIPPED] Not uploading error report to Firebase Storage for failed job {job_id}")
                    add_log(f"Skipped Firebase Storage upload for failed job {job_id} - error reports not stored in GCP", job_id=job_id)
                report_url = ""
            else:
                print(f"⚠️ [WARNING] Analysis report not found at: {report_path}")
                add_log(f"Warning: Analysis report not found for job {job_id} - JobDocument will have empty report_url", job_id=job_id)
                # Use empty string instead of local path - ensures ONLY Firebase Storage URLs or empty
                report_url = ""
            
            logs_url = f"/logs/{session_id}/{job_id}/"
            
            # Final validation: Ensure report_url is either empty or a valid Firebase Storage URL
            if report_url and not report_url.startswith('https://storage.googleapis.com/'):
                print(f"⚠️ [VALIDATION WARNING] Report URL is not a Firebase Storage URL: {report_url}")
                print(f"🔒 [VALIDATION] Converting to empty string to ensure Firebase Storage URLs only")
                add_log(f"Validation: Non-Firebase Storage URL detected for job {job_id}, converting to empty string", job_id=job_id)
                report_url = ""
            
            print(f"✅ [VALIDATION] Final report_url for JobDocument: '{report_url}' (Firebase Storage URL or empty)")
            
            # Create JobDocument according to the data model
            job_document = JobDocument(
                job_id=job_id,
                created_at=datetime.fromtimestamp(job_info['created_at']),
                logs_url=logs_url,
                report_url=report_url,
                total_token_used=metrics.get('total_tokens', 0),
                total_cost=costs.get('total_cost', 0),
                question=job_info['query'],
                job_status=job_status
            )
            
            # Save to Firestore using the data manager
            success = self.data_manager.create_job(user_email, session_id, job_document)
            
            if success:
                if token_limit_reached:
                    add_log(f"⚠️ Token-exhausted job {job_id} saved to Firestore with status 'failed'", job_id=job_id)
                    print(f"🔥 [FIRESTORE] Token-exhausted job {job_id} saved as FAILED for user {user_email} in session {session_id}")
                else:
                    add_log(f"✅ Job {job_id} saved to Firestore successfully", job_id=job_id)
                    print(f"🔥 [FIRESTORE] Job {job_id} saved for user {user_email} in session {session_id}")
            else:
                add_log(f"❌ Failed to save job {job_id} to Firestore", job_id=job_id)
            
            return success
            
        except Exception as e:
            error_msg = f"Error saving job {job_id} to Firestore: {str(e)}"
            add_log(f"❌ {error_msg}", job_id=job_id)
            print(f"❌ [FIRESTORE ERROR] {error_msg}")
            return False

# Global job manager instance
job_manager = JobManager()
