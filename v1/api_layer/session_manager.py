# Docker imports commented out - Cloud Run only
# import docker
# from docker.errors import NotFound
import uuid
import time
import threading
import os
import json
import shutil
from typing import Dict, Optional, Tuple, Any, cast, List
from utils.env import init_env
from logger import add_log, get_logs, clear_logs
import traceback
import yaml
from utils.sanitize import sanitize_email_for_storage

# Cloud Run / GCS (Cloud Run only mode)
try:
    from google.cloud import storage  # type: ignore
    import google.auth  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
    CLOUD_RUN_AVAILABLE = True
except Exception:
    CLOUD_RUN_AVAILABLE = False
    storage = None

constants = init_env()
class SessionManager:
    """Manages session-container pairs for isolated code execution environments"""
    
    def __init__(self, docker_image: str = None):
        # Cloud Run only mode - flag checking commented out
        # self.use_cloud_run = str(os.getenv("USE_CLOUD_RUN", "false")).lower() in ("1", "true", "yes", "on")
        # Always enable Cloud Run (Cloud Run only branch)
        self.cloud_run_enabled = bool(CLOUD_RUN_AVAILABLE)

        # Docker client commented out - Cloud Run only
        # self.docker_client = docker.from_env()
        # self.docker_image = docker_image
        self.sessions: Dict[str, Dict[str, Any]] = {}  # session_id -> {container_id, container_obj, created_at}
        self.lock = threading.Lock()
        self.sessions_file = "sessions.json"

        # Cloud Run configuration (hardcoded values, only USE_CLOUD_RUN flag from env)
        self.project_id = "insightbot-dev-474509"
        self.region = "us-central1"
        self.gcs_bucket = "insightbot-dev-474509.firebasestorage.app"
        self.cloud_run_image = "us-central1-docker.pkg.dev/insightbot-dev-474509/code-execution/code-execution-env:latest"
        self.service_account = "dev-insightbot@insightbot-dev-474509.iam.gserviceaccount.com"
        self.cloud_run_yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloud_run_service.yaml")

        # API layer URL to allow execution service to stream progress back (Socket.IO client)
        # Prefer explicit env var; fallback to constants if present.
        # For local development, can be empty (will be determined from request context if needed)
        self.api_layer_url = (os.getenv("API_LAYER_URL") or os.getenv("api_layer_url") or constants.get("api_layer_url") or "").strip()

        self._load_sessions_from_file()

    # ----------------------------
    # Cloud Run + GCS helpers
    # ----------------------------
    def ensure_user_session_prefixes(self, email: str, session_id: str) -> bool:
        """Ensure user/session prefixes exist in the configured global bucket (idempotent)."""
        try:
            # Cloud Run only mode - always check storage
            if storage is None:
                return False
            if not self.gcs_bucket or not email or not session_id:
                return False
            client = storage.Client(project=self.project_id or None)
            bucket = client.bucket(self.gcs_bucket)
            if not bucket.exists(client):
                add_log(f"[SESSION_MANAGER] GCS bucket not found for prefixes: {self.gcs_bucket}")
                return False

            user_email_sanitized = sanitize_email_for_storage(email)
            base_prefix = f"{user_email_sanitized}/{session_id}/"
            keys = [
                f"{base_prefix}.keep",
                f"{base_prefix}input_data/.keep",
                f"{base_prefix}output_data/.keep",
            ]
            for key in keys:
                blob = bucket.blob(key)
                if not blob.exists(client):
                    blob.upload_from_string(b"", content_type="text/plain")
            return True
        except Exception as e:
            add_log(f"[SESSION_MANAGER] Error ensuring user session prefixes: {str(e)} | traceback: {traceback.format_exc()}")
            return False

    def _load_cloud_run_yaml(self, *, service_name: str, bucket_name: str, api_layer_url: str) -> Dict[str, Any]:
        """Load and render cloud_run_service.yaml for a job-specific service."""
        if not os.path.exists(self.cloud_run_yaml_path):
            raise FileNotFoundError(f"Cloud Run YAML not found: {self.cloud_run_yaml_path}")
        with open(self.cloud_run_yaml_path, "r", encoding="utf-8") as f:
            text = f.read()

        # Keep existing placeholders behavior (from scripts/deploy_cloud_run.py)
        text = text.replace("PROJECT_PLACEHOLDER", self.project_id)
        # SESSION_PLACEHOLDER is re-used as a generic identifier in service name; we override metadata.name anyway.
        text = text.replace("SESSION_PLACEHOLDER", service_name.replace("exec-", ""))
        text = text.replace("IMAGE_PLACEHOLDER", self.cloud_run_image)
        text = text.replace("BUCKET_PLACEHOLDER", bucket_name)
        text = text.replace("SERVICE_ACCOUNT_PLACEHOLDER", self.service_account)

        docs = list(yaml.safe_load_all(text))
        service_doc = docs[0] if docs else {}
        if not isinstance(service_doc, dict) or service_doc.get("kind") != "Service":
            raise RuntimeError("First document in YAML must be a Cloud Run Service")

        # Enforce desired service name and inject API layer URL env var for progress streaming
        service_doc.setdefault("metadata", {})["name"] = service_name
        tpl = (service_doc.get("spec") or {}).get("template") or {}
        spec = tpl.get("spec") or {}
        containers = spec.get("containers") or []
        if containers and isinstance(containers, list) and isinstance(containers[0], dict):
            env = containers[0].get("env") or []
            if not isinstance(env, list):
                env = []
            # Remove any previous api_layer_url env entries, then add ours (only if URL is provided)
            env = [e for e in env if not (isinstance(e, dict) and e.get("name") in ("api_layer_url", "API_LAYER_URL"))]
            if api_layer_url:  # Only add if URL is provided
                env.append({"name": "api_layer_url", "value": api_layer_url})
            containers[0]["env"] = env
        return service_doc

    def _wait_for_operation(self, run_client, op_name: str, poll_seconds: float = 1.0, timeout_seconds: int = 300) -> Dict[str, Any]:
        """Wait for Cloud Run operation. poll_seconds=1 for faster completion detection."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            op = run_client.projects().locations().operations().get(name=op_name).execute()
            if op.get("done"):
                if "error" in op:
                    raise RuntimeError(f"Operation failed: {op['error']}")
                return op
            time.sleep(poll_seconds)
        raise TimeoutError(f"Operation did not complete within {timeout_seconds} seconds: {op_name}")

    def _poll_service_url(self, run_client, service_name: str, max_wait_seconds: int = 300, poll_seconds: float = 1.0) -> Optional[str]:
        name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        deadline = time.time() + max_wait_seconds
        while time.time() < deadline:
            try:
                svc = run_client.projects().locations().services().get(name=name).execute()
                url = svc.get("status", {}).get("url")
                if url:
                    return url
            except Exception:
                pass
            time.sleep(poll_seconds)
        return None

    def _set_public_invoker_binding(self, run_client, service_name: str) -> None:
        resource = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        policy = run_client.projects().locations().services().getIamPolicy(resource=resource).execute()
        bindings = policy.get("bindings", [])
        binding = next((b for b in bindings if b.get("role") == "roles/run.invoker"), None)
        if binding:
            members = set(binding.get("members", []))
            if "allUsers" in members:
                return
            members.add("allUsers")
            binding["members"] = sorted(list(members))
        else:
            bindings.append({"role": "roles/run.invoker", "members": ["allUsers"]})
        policy["bindings"] = bindings
        run_client.projects().locations().services().setIamPolicy(resource=resource, body={"policy": policy}).execute()

    def deploy_job_service(self, *, job_id: str, session_id: str, user_email: str) -> Dict[str, str]:
        """Create/replace a Cloud Run service dedicated to this job and return {service_name, service_url}."""
        # Cloud Run only mode - always enabled
        if not self.cloud_run_enabled:
            raise RuntimeError("Cloud Run not available (Cloud Run only mode)")
        if not self.project_id or not self.region:
            raise RuntimeError("Missing GCP_PROJECT/GCP_REGION configuration for Cloud Run")
        if not self.gcs_bucket:
            raise RuntimeError("Missing GCS_BUCKET configuration for Cloud Run")
        if not self.cloud_run_image:
            raise RuntimeError("Missing CLOUD_RUN_IMAGE configuration for Cloud Run")
        if not self.service_account:
            raise RuntimeError("Missing CLOUD_RUN_SERVICE_ACCOUNT configuration for Cloud Run")
        # API_LAYER_URL is optional - if not set, Cloud Run service will still work but won't stream progress
        # For local development, this can be empty
        # if not self.api_layer_url:
        #     raise RuntimeError("Missing API_LAYER_URL/api_layer_url configuration for Cloud Run progress streaming")
        if not self.api_layer_url:
            add_log(f"[CLOUD_RUN] Warning: API_LAYER_URL not set - progress streaming will be disabled", job_id=job_id)

        # Ensure user/session prefixes exist (best-effort)
        try:
            self.ensure_user_session_prefixes(user_email, session_id)
        except Exception:
            pass

        # Cloud Run service names must be lowercase, numbers, hyphens; keep it short.
        raw = f"exec-{job_id}".lower().replace("_", "-")
        service_name = raw[:60].strip("-")

        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        run = build("run", "v1", credentials=creds, cache_discovery=True)

        full_name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        parent = f"projects/{self.project_id}/locations/{self.region}"

        body = self._load_cloud_run_yaml(service_name=service_name, bucket_name=self.gcs_bucket, api_layer_url=self.api_layer_url)
        # Remove namespace for location-based API
        if "namespace" in body.get("metadata", {}):
            body["metadata"].pop("namespace", None)

        try:
            run.projects().locations().services().get(name=full_name).execute()
            add_log(f"[CLOUD_RUN] Updating service: {full_name}", job_id=job_id)
            op = run.projects().locations().services().replaceService(name=full_name, body=body).execute()
            if "name" in op:
                self._wait_for_operation(run, op["name"])
        except HttpError as e:
            if e.resp.status == 404:
                add_log(f"[CLOUD_RUN] Creating service: {full_name}", job_id=job_id)
                op = run.projects().locations().services().create(parent=parent, body=body).execute()
                if "name" in op:
                    self._wait_for_operation(run, op["name"])
            else:
                raise

        self._set_public_invoker_binding(run, service_name)
        svc = run.projects().locations().services().get(name=full_name).execute()
        url = svc.get("status", {}).get("url") or self._poll_service_url(run, service_name, max_wait_seconds=300, poll_seconds=1.0)
        if not url:
            raise RuntimeError(f"Cloud Run service URL not available for {service_name}")
        return {"service_name": service_name, "service_url": url}

    def delete_job_service(self, *, service_name: str, job_id: str | None = None) -> bool:
        """Delete a job-specific Cloud Run service (best-effort)."""
        try:
            # Cloud Run only mode - always enabled
            # if not self.cloud_run_enabled:
            #     return False
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            run = build("run", "v1", credentials=creds, cache_discovery=False)
            full_name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
            run.projects().locations().services().delete(name=full_name).execute()
            add_log(f"[CLOUD_RUN] Deleted service {service_name}", job_id=job_id)
            return True
        except Exception as e:
            add_log(f"[CLOUD_RUN] Warning: could not delete service {service_name}: {str(e)}", job_id=job_id)
            return False
    
    def _load_sessions_from_file(self):
        """Load existing sessions from JSON file"""
        if os.path.exists(self.sessions_file):
            try:
                with open(self.sessions_file, 'r') as f:
                    stored_sessions = json.load(f)
                
                # Validate and restore sessions
                for session_id, session_data in stored_sessions.items():
                    if self._validate_existing_session(session_id, session_data):
                        self.sessions[session_id] = session_data
                        # add_log(f"Restored session {session_id} from file")
                    else:
                        add_log(f"Session {session_id} from file is invalid, skipping")
                        
            except Exception as e:
                add_log(f"Error loading sessions from file: {str(e)} | traceback: {traceback.format_exc()}")
                # If file is corrupted, start fresh
                self.sessions = {}
    
    def _save_sessions_to_file(self):
        """Save current sessions to JSON file"""
        try:
            # Convert sessions to JSON-serializable format
            sessions_to_save = {}
            for session_id, session_info in self.sessions.items():
                sessions_to_save[session_id] = {
                    'container_id': session_info['container_id'],
                    'created_at': session_info['created_at'],
                    'status': session_info['status'],
                    'container_ip': session_info.get('container_ip', ''),
                    'container_port': session_info.get('container_port', ''),
                    'output_dir': session_info.get('output_dir', ''),
                    'user_info': session_info.get('user_info', {})
                }
            
            with open(self.sessions_file, 'w') as f:
                json.dump(sessions_to_save, f, indent=2)
                
        except Exception as e:
            add_log(f"Error saving sessions to file: {str(e)} | traceback: {traceback.format_exc()}")
    
    def check_session_ownership(self, session_id: str, user_email: str) -> bool:
        """Check if a user owns a specific session"""
        with self.lock:
            session_info = self.sessions.get(session_id)
            if not session_info:
                return False
            
            session_user_info = session_info.get('user_info', {})
            session_user_email = session_user_info.get('email', '').lower()
            
            return session_user_email == user_email.lower()
    
    def _validate_existing_session(self, session_id: str, session_data: Dict[str, Any]) -> bool:
        """Validate if an existing session from file is still valid - Cloud Run only mode"""
        try:
            # Cloud Run only mode - no Docker containers to validate
            # Just check if session data exists
            if not session_data:
                return False
            
            # In Cloud Run mode, sessions don't have containers
            # Just restore the session info
            session_data['status'] = session_data.get('status', 'active')
            add_log(f"✅ Restored session {session_id} (Cloud Run mode)")
            return True
                    
        except Exception as e:
            add_log(f"Error validating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    # Docker method commented out - Cloud Run only
    # def _get_container_ip(self, container) -> str:
    #     """Get container IP address"""
    #     try:
    #         container.reload()
    #         if not container.attrs:
    #             return ''
    #             
    #         network_settings = container.attrs.get('NetworkSettings', {})
    #         if not network_settings:
    #             return ''
    #             
    #         networks = network_settings.get('Networks', {})
    #         if not networks:
    #             return ''
    #             
    #         if 'bridge' in networks:
    #             bridge_config = networks['bridge']
    #             if bridge_config and 'IPAddress' in bridge_config:
    #                 ip_address = bridge_config['IPAddress']
    #                 if ip_address:
    #                     return ip_address
    #         
    #         # If not on bridge, get the first available network
    #         for network_name, network_config in networks.items():
    #             if network_config and 'IPAddress' in network_config:
    #                 ip_address = network_config['IPAddress']
    #                 if ip_address:
    #                     return ip_address
    #                 
    #         return ''
    #     except Exception as e:
    #         add_log(f"Error getting container IP: {str(e)} | traceback: {traceback.format_exc()}")
    #         return ''
        
    def create_session(self, user_info: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
        """Create a new session. Cloud Run only mode - sets up GCS prefixes (no Docker container)."""
        session_id = str(uuid.uuid4())
        
        with self.lock:
            try:
                add_log(f"Creating new session: {session_id}")
                
                # Cloud Run only mode: set up GCS prefixes, no Docker container
                # if self.cloud_run_enabled:
                email = (user_info.get('email') or '').lower() if user_info else ''
                if email:
                    try:
                        self.ensure_user_session_prefixes(email, session_id)
                        add_log(f"GCS prefixes ensured for session {session_id} (user: {email})")
                    except Exception as e:
                        add_log(f"Warning: GCS prefix setup failed for session {session_id}: {str(e)}")
                
                # Store session info without container details
                self.sessions[session_id] = {
                    'container_id': '',  # No container in Cloud Run mode
                    'container_obj': None,
                    'container_ip': '',
                    'container_port': None,
                    'created_at': time.time(),
                    'status': 'active',
                    'output_dir': '',  # Outputs go to GCS
                    'user_info': user_info or {}
                }
                
                # Save sessions to file
                self._save_sessions_to_file()
                
                add_log(f"Session {session_id} created (Cloud Run mode, no Docker container)")
                return session_id, session_id  # Return session_id as placeholder for container_id
                
            except Exception as e:
                add_log(f"Error creating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
                raise
    
    def _find_free_port(self) -> int:
        """Find a free port on the host"""
        import socket
        
        # Start from port 5100
        start_port = 5100
        max_port = 5999
        
        for port in range(start_port, max_port + 1):
            # Check if port is already used by another container
            if any(session_info.get('container_port') == port for session_info in self.sessions.values()):
                continue
                
            # Check if port is free on host
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('', port))
                    return port
                except OSError:
                    continue
                    
        # If we get here, no ports were available
        raise Exception(f"No free ports available in range {start_port}-{max_port}")
    
    def _copy_session_input_files(self, source_session_id: str, target_session_id: str, base_dir: str):
        """Copy input files from source session to target session"""
        try:
            source_input_dir = os.path.join(base_dir, 'execution_layer', 'input_data', source_session_id)
            target_input_dir = os.path.join(base_dir, 'execution_layer', 'input_data', target_session_id)
            
            # Check if source session input directory exists
            if not os.path.exists(source_input_dir):
                add_log(f"Source session {source_session_id} input directory not found: {source_input_dir}")
                return
            
            # Create target input directory
            os.makedirs(target_input_dir, exist_ok=True)
            
            # Copy all files from source to target
            files_copied = 0
            for filename in os.listdir(source_input_dir):
                source_file = os.path.join(source_input_dir, filename)
                target_file = os.path.join(target_input_dir, filename)
                
                if os.path.isfile(source_file):
                    shutil.copy2(source_file, target_file)
                    files_copied += 1
                    add_log(f"Copied file: {filename} from session {source_session_id} to {target_session_id}")
            
            add_log(f"Successfully copied {files_copied} files from session {source_session_id} to {target_session_id}")
            
        except Exception as e:
            add_log(f"Error copying files from session {source_session_id} to {target_session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    def _cleanup_previous_session(self, session_id: str):
        """Clean up previous session completely after successful data copy (removes from sessions.json and deletes files)"""
        try:
            add_log(f"Starting complete cleanup of previous session: {session_id}")
            
            # Check if session exists and is safe to delete
            session_info = None
            
            # First check in-memory sessions
            if session_id in self.sessions:
                session_info = self.sessions[session_id]
                
                # Don't delete if session is currently active (has running container)
                if session_info.get('status') == 'active' and session_info.get('container_obj'):
                    add_log(f"⚠️ Skipping cleanup - session {session_id} is currently active with running container")
                    return
            
            # Also check persistent sessions.json file
            persistent_session_info = None
            if os.path.exists(self.sessions_file):
                try:
                    with open(self.sessions_file, 'r') as f:
                        persistent_sessions = json.load(f)
                        persistent_session_info = persistent_sessions.get(session_id)
                except Exception as e:
                    add_log(f"Error reading persistent sessions for cleanup: {str(e)}")
            
            # 1. Remove session from in-memory sessions
            if session_id in self.sessions:
                del self.sessions[session_id]
                add_log(f"✅ Removed session {session_id} from in-memory sessions")
            
            # 2. Remove session from persistent sessions.json file
            if persistent_session_info:
                try:
                    with open(self.sessions_file, 'r') as f:
                        persistent_sessions = json.load(f)
                    
                    if session_id in persistent_sessions:
                        del persistent_sessions[session_id]
                        
                        with open(self.sessions_file, 'w') as f:
                            json.dump(persistent_sessions, f, indent=2)
                        
                        add_log(f"✅ Removed session {session_id} from persistent sessions.json")
                    
                except Exception as e:
                    add_log(f"Error removing session from persistent file: {str(e)}")
            
            # 3. Remove input directory and files (the old session's data)
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            input_dir = os.path.join(base_dir, 'execution_layer', 'input_data', session_id)
            
            if os.path.exists(input_dir):
                try:
                    files_before = len(os.listdir(input_dir))
                    shutil.rmtree(input_dir)
                    add_log(f"✅ Deleted previous session input directory {input_dir} with {files_before} files")
                except Exception as e:
                    add_log(f"Error deleting input directory {input_dir}: {str(e)}")
            else:
                add_log(f"Input directory {input_dir} does not exist - already cleaned")
            
            # 4. Also clean up output directory if it exists
            output_dir = None
            if session_info:
                output_dir = session_info.get('output_dir')
            elif persistent_session_info:
                output_dir = persistent_session_info.get('output_dir')
            
            if output_dir and os.path.exists(output_dir):
                try:
                    shutil.rmtree(output_dir)
                    add_log(f"✅ Deleted previous session output directory {output_dir}")
                except Exception as e:
                    add_log(f"Error deleting output directory {output_dir}: {str(e)}")
            
            add_log(f"🎯 Previous session {session_id} completely cleaned up - only new session remains with copied data")
            
        except Exception as e:
            add_log(f"Error during previous session cleanup {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            # Don't raise - cleanup failure shouldn't stop new session creation
    
    def _get_most_recent_session_with_files(self, current_user_info: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Get the most recent session that has input files, STRICTLY filtered by current user only"""
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            input_data_base = os.path.join(base_dir, 'execution_layer', 'input_data')
            
            if not os.path.exists(input_data_base):
                return None
            
            # If no user info provided, don't copy any data (security measure)
            if not current_user_info or not current_user_info.get('email'):
                add_log("No user info provided, skipping previous session data copy for security")
                return None
            
            current_user_email = current_user_info.get('email').lower()
            user_sessions_with_files = []
            
            # Load all session data from both memory and persistent file
            all_session_data = {}
            
            # First, load from persistent sessions.json file
            if os.path.exists(self.sessions_file):
                try:
                    with open(self.sessions_file, 'r') as f:
                        persistent_sessions = json.load(f)
                        all_session_data.update(persistent_sessions)
                        add_log(f"Loaded {len(persistent_sessions)} sessions from persistent file")
                except Exception as e:
                    add_log(f"Error reading persistent sessions file: {str(e)}")
            
            # Then, update with in-memory sessions (more recent)
            for session_id, session_info in self.sessions.items():
                if session_id not in all_session_data or session_info.get('created_at', 0) > all_session_data.get(session_id, {}).get('created_at', 0):
                    all_session_data[session_id] = session_info
            
            add_log(f"Total sessions to check: {len(all_session_data)}")
            
            for session_dir in os.listdir(input_data_base):
                session_path = os.path.join(input_data_base, session_dir)
                if os.path.isdir(session_path):
                    # Check if this directory has any files
                    files = [f for f in os.listdir(session_path) if os.path.isfile(os.path.join(session_path, f))]
                    if files:
                        add_log(f"Session {session_dir} has {len(files)} files")
                        
                        # Get session info from combined data (persistent + memory)
                        session_info = all_session_data.get(session_dir, {})
                        
                        if not session_info:
                            add_log(f"No session info found for {session_dir} - skipping")
                            continue
                            
                        session_user_info = session_info.get('user_info', {})
                        session_user_email = session_user_info.get('email', '').lower()
                        
                        add_log(f"Session {session_dir}: user_email={session_user_email}, current_user={current_user_email}")
                        
                        # STRICT USER FILTER: Only include sessions from the SAME user
                        if session_user_email == current_user_email:
                            created_at = session_info.get('created_at', 0)
                            user_sessions_with_files.append({
                                'session_id': session_dir,
                                'created_at': created_at,
                                'user_email': session_user_email
                            })
                            add_log(f"✅ Found user session with files: {session_dir} (user: {session_user_email}, created: {created_at})")
                        else:
                            add_log(f"❌ Skipping session {session_dir} - belongs to different user: {session_user_email}")
                    else:
                        add_log(f"Session {session_dir} has no files - skipping")
            
            if not user_sessions_with_files:
                add_log(f"No previous sessions with files found for user: {current_user_email}")
                return None
            
            # Sort by creation time (newest first) - only same-user sessions
            user_sessions_with_files.sort(key=lambda x: x['created_at'], reverse=True)
            
            most_recent_session = user_sessions_with_files[0]['session_id']
            add_log(f"✅ Selected most recent user session: {most_recent_session} for user: {current_user_email} (created: {user_sessions_with_files[0]['created_at']})")
            return most_recent_session
            
        except Exception as e:
            add_log(f"Error getting most recent session with files: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    def get_session_container(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get container information for a session. In Cloud Run mode, returns session info without container."""
        with self.lock:
            session_info = self.sessions.get(session_id)
            
            # If session not in memory, try to load from file
            if not session_info:
                session_info = self._load_session_from_file(session_id)
                if session_info:
                    self.sessions[session_id] = session_info
            
            if session_info:
                # Cloud Run mode: return session info without container checks
                return session_info
                
            return None
    
    def _load_session_from_file(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Load a specific session from file if it exists"""
        try:
            if os.path.exists(self.sessions_file):
                with open(self.sessions_file, 'r') as f:
                    stored_sessions = json.load(f)
                
                if session_id in stored_sessions:
                    session_data = stored_sessions[session_id]
                    if self._validate_existing_session(session_id, session_data):
                        add_log(f"✅ Restored session {session_id} from file")
                        return session_data
                    else:
                        add_log(f"Session {session_id} from file is invalid")
        except Exception as e:
            add_log(f"Error loading session {session_id} from file: {str(e)}")
        return None
    
    def cleanup_session(self, session_id: str) -> bool:
        """Clean up session - Cloud Run only mode (no Docker containers)"""
        with self.lock:
            session_info = self.sessions.get(session_id)
            if session_info:
                try:
                    # Mark session as inactive instead of deleting from memory
                    self.sessions[session_id]['status'] = 'cleaned'
                    self.sessions[session_id]['container_obj'] = None
                    
                    # Save updated sessions to file (preserves session history)
                    self._save_sessions_to_file()

                    add_log(f"Session {session_id} cleaned up successfully (Cloud Run mode, no Docker container)")
                    return True
                    
                except Exception as e:
                    add_log(f"Error cleaning up session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
                    return False
            return False
    
    def cleanup_inactive_sessions(self, max_age_hours: int = 24):
        """Clean up sessions older than max_age_hours"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        with self.lock:
            sessions_to_cleanup = []
            for session_id, session_info in self.sessions.items():
                if current_time - session_info['created_at'] > max_age_seconds:
                    sessions_to_cleanup.append(session_id)
            
            for session_id in sessions_to_cleanup:
                add_log(f"Cleaning up old session: {session_id}")
                self.cleanup_session(session_id)
    
    def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get status information for a session"""
        session_info = self.get_session_container(session_id)
        if session_info:
            return {
                'session_id': session_id,
                'container_id': session_info['container_id'],
                'container_ip': session_info.get('container_ip', ''),
                'status': session_info['status'],
                'created_at': session_info['created_at'],
                'uptime': time.time() - session_info['created_at']
            }
        else:
            return {
                'session_id': session_id,
                'status': 'not_found',
                'error': 'Session not found or inactive'
            }

# Global session manager instance
session_manager = SessionManager() 