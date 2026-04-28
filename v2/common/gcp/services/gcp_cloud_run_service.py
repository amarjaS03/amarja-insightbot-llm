"""
GCP Cloud Run Service - Utilities for Cloud Run operations.
"""

import os
import time
import yaml
import traceback
from typing import Dict, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

try:
    from google.cloud import storage
    import google.auth
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    CLOUD_RUN_AVAILABLE = True
except Exception:
    CLOUD_RUN_AVAILABLE = False
    storage = None

from v2.utils.env import init_env
from v2.common.logger import add_log


class GcpCloudRunService:
    """Service for managing Cloud Run operations."""
    
    def __init__(self, project_id: Optional[str] = None, credentials=None):
        """Initialize Cloud Run service."""
        constants = init_env()
        
        self.project_id = project_id or os.getenv('GCP_PROJECT') or constants.get('gcp_project') or "insightbot-dev-474509"
        self.region = os.getenv('GCP_REGION') or constants.get('gcp_region') or "us-central1"
        self.gcs_bucket = os.getenv('GCS_BUCKET') or constants.get('storage_bucket') or "insightbot-dev-474509.firebasestorage.app"
        self.cloud_run_image = os.getenv('CLOUD_RUN_IMAGE') or constants.get('cloud_run_image') or "us-central1-docker.pkg.dev/insightbot-dev-474509/code-execution/code-execution-env:latest"
        self.service_account = os.getenv('CLOUD_RUN_SERVICE_ACCOUNT') or constants.get('cloud_run_service_account') or "dev-insightbot@insightbot-dev-474509.iam.gserviceaccount.com"
        
        # API layer URL for progress streaming
        self.api_layer_url = (os.getenv("API_LAYER_URL") or os.getenv("api_layer_url") or constants.get("api_layer_url") or "").strip()
        
        # Path to cloud_run_service.yaml (relative to project root)
        project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
        self.cloud_run_yaml_path = project_root / "cloud_run_service.yaml"
        
        # Initialize credentials
        if credentials:
            self.credentials = credentials
        else:
            try:
                self.credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            except Exception:
                self.credentials = None
        
        self.cloud_run_enabled = bool(CLOUD_RUN_AVAILABLE)
    
    def _ensure_user_session_prefixes(self, user_id: str, session_id: str) -> bool:
        """Ensure user/session prefixes exist in the configured global bucket (idempotent)."""
        try:
            if storage is None:
                return False
            if not self.gcs_bucket or not user_id or not session_id:
                return False
            
            client = storage.Client(project=self.project_id or None)
            bucket = client.bucket(self.gcs_bucket)
            if not bucket.exists(client):
                add_log(f"[CLOUD_RUN] GCS bucket not found for prefixes: {self.gcs_bucket}")
                return False

            # Use user_id directly without sanitization (as per user requirement)
            base_prefix = f"{user_id}/{session_id}/"
            # Create lowercase folders only (standard naming convention: input_data/output_data)
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
            add_log(f"[CLOUD_RUN] Error ensuring user session prefixes: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    def _load_cloud_run_yaml(self, *, service_name: str, bucket_name: str, api_layer_url: str) -> Dict[str, Any]:
        """Load and render cloud_run_service.yaml for a job-specific service."""
        if not self.cloud_run_yaml_path.exists():
            raise FileNotFoundError(f"Cloud Run YAML not found: {self.cloud_run_yaml_path}")
        
        with open(self.cloud_run_yaml_path, "r", encoding="utf-8") as f:
            text = f.read()

        # Replace placeholders
        text = text.replace("PROJECT_PLACEHOLDER", self.project_id)
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
            # Normalize existing env to a {name: value} map
            env_map: Dict[str, str] = {}
            for e in env:
                if isinstance(e, dict) and e.get("name") and "value" in e:
                    env_map[str(e["name"])] = str(e.get("value") or "")

            # Single source of truth: the API layer process env / constants control what gets injected.
            # Always ensure these are present for the execution layer container:
            env_map["GCS_BUCKET"] = bucket_name
            env_map.setdefault("DATA_DIR", "/data")

            # Inject API layer URL for Socket.IO progress streaming (only if provided)
            if api_layer_url:
                env_map["api_layer_url"] = api_layer_url.strip().rstrip("/")

            # Forward OpenAI key if available (do NOT hardcode in Dockerfile/YAML).
            openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()
            if openai_key:
                env_map["OPENAI_API_KEY"] = openai_key

            # Rebuild env list
            containers[0]["env"] = [{"name": k, "value": v} for k, v in env_map.items() if v != ""]
        return service_doc
    
    def _wait_for_operation(self, run_client, op_name: str, poll_seconds: float = 1.0, timeout_seconds: int = 300) -> Dict[str, Any]:
        """Wait for a Cloud Run operation to complete. poll_seconds=1 for faster detection."""
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
        """Poll for Cloud Run service URL."""
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
        """Set public invoker binding for Cloud Run service."""
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
    
    def deploy_job_service(self, *, user_id: str, job_id: str, session_id: str) -> Dict[str, str]:
        """
        Create/replace a Cloud Run service dedicated to this job and return {service_name, service_url}.
        
        Args:
            user_id: User ID (no sanitization needed)
            job_id: Job ID as string
            session_id: Session ID as string
            
        Returns:
            Dict with service_name and service_url
        """
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
        
        # Read API_LAYER_URL fresh from environment each time (not cached) to ensure .env changes are picked up
        # This ensures that if .env is updated and server restarted, new deployments use the latest URL
        load_dotenv(override=True)  # Reload .env file to pick up latest changes
        constants = init_env()
        current_api_layer_url = (os.getenv("API_LAYER_URL") or os.getenv("api_layer_url") or constants.get("api_layer_url") or "").strip().rstrip("/")
        
        if not current_api_layer_url:
            add_log(f"[CLOUD_RUN] Warning: API_LAYER_URL not set - progress streaming will be disabled", job_id=job_id)
            add_log(f"[CLOUD_RUN] Set API_LAYER_URL env var or api_layer_url in constants.json to enable progress streaming", job_id=job_id)
        else:
            # Log the URL being used for transparency (truncate for security)
            url_display = current_api_layer_url[:50] + "..." if len(current_api_layer_url) > 50 else current_api_layer_url
            add_log(f"[CLOUD_RUN] Using API_LAYER_URL: {url_display}", job_id=job_id)

        # Note: Bucket folders (input_data/output_data) are already created when session is created,
        # so we don't need to create them here. This avoids duplicate folder creation.

        # Cloud Run service names must be lowercase, numbers, hyphens; keep it short.
        raw = f"exec-{job_id}".lower().replace("_", "-")
        service_name = raw[:60].strip("-")

        if not self.credentials:
            self.credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        
        run = build("run", "v1", credentials=self.credentials, cache_discovery=True)

        full_name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        parent = f"projects/{self.project_id}/locations/{self.region}"

        body = self._load_cloud_run_yaml(service_name=service_name, bucket_name=self.gcs_bucket, api_layer_url=current_api_layer_url)
        # Remove namespace for location-based API
        if "namespace" in body.get("metadata", {}):
            body["metadata"].pop("namespace", None)

        try:
            existing_svc = run.projects().locations().services().get(name=full_name).execute()
            existing_url = existing_svc.get("status", {}).get("url", "")
            add_log(f"[CLOUD_RUN] Service exists: {service_name} (URL: {existing_url})", job_id=job_id)
            add_log(f"[CLOUD_RUN] Updating service: {full_name}", job_id=job_id)
            op = run.projects().locations().services().replaceService(name=full_name, body=body).execute()
            if "name" in op:
                add_log(f"[CLOUD_RUN] Waiting for update operation: {op['name']}", job_id=job_id)
                self._wait_for_operation(run, op["name"])
                add_log(f"[CLOUD_RUN] Service update completed: {service_name}", job_id=job_id)
        except HttpError as e:
            if e.resp.status == 404:
                add_log(f"[CLOUD_RUN] Service not found, creating new service: {full_name}", job_id=job_id)
                add_log(f"[CLOUD_RUN] Image: {self.cloud_run_image}", job_id=job_id)
                add_log(f"[CLOUD_RUN] Bucket: {self.gcs_bucket}", job_id=job_id)
                add_log(f"[CLOUD_RUN] API Layer URL: {self.api_layer_url or 'Not set'}", job_id=job_id)
                op = run.projects().locations().services().create(parent=parent, body=body).execute()
                if "name" in op:
                    add_log(f"[CLOUD_RUN] Waiting for create operation: {op['name']}", job_id=job_id)
                    self._wait_for_operation(run, op["name"])
                    add_log(f"[CLOUD_RUN] Service creation completed: {service_name}", job_id=job_id)
            else:
                add_log(f"[CLOUD_RUN] Error checking/creating service: {str(e)}", job_id=job_id)
                raise

        add_log(f"[CLOUD_RUN] Setting public invoker binding for: {service_name}", job_id=job_id)
        self._set_public_invoker_binding(run, service_name)
        add_log(f"[CLOUD_RUN] Public invoker binding set", job_id=job_id)
        
        add_log(f"[CLOUD_RUN] Fetching service details: {full_name}", job_id=job_id)
        svc = run.projects().locations().services().get(name=full_name).execute()
        url = svc.get("status", {}).get("url")
        
        if not url:
            add_log(f"[CLOUD_RUN] Service URL not immediately available, polling...", job_id=job_id)
            url = self._poll_service_url(run, service_name, max_wait_seconds=300, poll_seconds=1.0)
        
        if not url:
            raise RuntimeError(f"Cloud Run service URL not available for {service_name}")
        
        add_log(f"[CLOUD_RUN] ✅ Cloud Run service deployed successfully", job_id=job_id)
        add_log(f"[CLOUD_RUN] Service Name: {service_name}", job_id=job_id)
        add_log(f"[CLOUD_RUN] Service URL: {url}", job_id=job_id)
        add_log(f"[CLOUD_RUN] Region: {self.region}", job_id=job_id)
        add_log(f"[CLOUD_RUN] Project: {self.project_id}", job_id=job_id)
        
        return {"service_name": service_name, "service_url": url}
