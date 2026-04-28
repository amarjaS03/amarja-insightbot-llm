#!/usr/bin/env python3
"""
Deploy Cloud Run service from cloud_run_service.yaml using Google APIs and update sessions.json.

Prereqs:
- Application Default Credentials available (e.g., service account or `gcloud auth application-default login`)
- Cloud Run Admin API enabled
- Artifact Registry image already built and pushed (pass its full URL via --image)
"""

import json
import os
import sys
import time
from typing import Any, Dict, List

import yaml
from google.cloud import storage
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------
# Constant configuration
# ---------------------------
# Fill these values before running the script.
PROJECT_ID: str = "insightbot-dev-474509"
REGION: str = "us-central1"

# Full Artifact Registry image URL, e.g. "us-central1-docker.pkg.dev/<project>/<repo>/<image>:tag"
IMAGE_URI: str = "us-central1-docker.pkg.dev/insightbot-dev-474509/code-execution/code-execution-env:latest"

# Session and user identifiers
SESSION_ID: str = "dev-session-001"
USER_EMAIL_SANITIZED: str = "prateek.k@insightbot.ai"

# GCS bucket and prefixes (prefixes should typically be "<user>/input_data" and "<user>/output_data")
BUCKET: str = "insightbot-dev-474509.firebasestorage.app"
INPUT_PREFIX: str = f"{USER_EMAIL_SANITIZED}/input_data"
OUTPUT_PREFIX: str = f"{USER_EMAIL_SANITIZED}/output_data"

# Optional explicit service name; default will be "exec-<SESSION_ID>"
SERVICE_NAME: str | None = None

# Path to the Cloud Run YAML
YAML_PATH: str = "cloud_run_service.yaml"

# Service account to run the Cloud Run service
SERVICE_ACCOUNT_EMAIL: str = "dev-insightbot@insightbot-dev-474509.iam.gserviceaccount.com"


def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    return prefix if prefix.endswith("/") else prefix + "/"


def ensure_prefixes(bucket_name: str, prefixes: List[str]) -> None:
    """Ensure "folders" (prefix markers) exist by creating zero-byte objects."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for p in prefixes:
        norm = _normalize_prefix(p)
        if not norm:
            continue
        marker = bucket.blob(norm)
        # Only create if missing
        if not marker.exists():
            marker.upload_from_string(b"", content_type="application/octet-stream")
            print(f"Created prefix marker: gs://{bucket_name}/{norm}")


def load_and_render_yaml(yaml_path: str, replacements: Dict[str, str]) -> Dict[str, Any]:
    """Load service YAML and apply simple placeholder replacements."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        text = f.read()
    for key, value in replacements.items():
        text = text.replace(key, value)
    docs = list(yaml.safe_load_all(text))
    # First doc is Cloud Run Service
    service_doc = docs[0] if docs else {}
    if not isinstance(service_doc, dict) or service_doc.get("kind") != "Service":
        raise RuntimeError("First document in YAML must be a Cloud Run Service")
    return service_doc


def wait_for_operation(run_client, op_name: str, poll_seconds: int = 2, timeout_seconds: int = 300) -> Dict[str, Any]:
    """Poll long-running operation until completion."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        op = run_client.projects().locations().operations().get(name=op_name).execute()
        if op.get("done"):
            if "error" in op:
                raise RuntimeError(f"Operation failed: {op['error']}")
            return op
        time.sleep(poll_seconds)
    raise TimeoutError(f"Operation did not complete within {timeout_seconds} seconds: {op_name}")

def poll_service_url(project: str, region: str, service_name: str, max_wait_seconds: int = 300, poll_seconds: int = 3) -> str | None:
    """Poll the Cloud Run service until status.url is available or timeout."""
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    run = build("run", "v1", credentials=creds, cache_discovery=False)
    name = f"projects/{project}/locations/{region}/services/{service_name}"
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            svc = run.projects().locations().services().get(name=name).execute()
            url = svc.get("status", {}).get("url")
            if url:
                return url
        except Exception:
            pass
        time.sleep(poll_seconds)
    return None

def health_check(url: str, path: str = "/health", max_wait_seconds: int = 180, poll_seconds: int = 5) -> bool:
    """Poll the service health endpoint until 200 OK or timeout."""
    import urllib.request
    import urllib.error

    endpoint = url.rstrip("/") + path
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            req = urllib.request.Request(endpoint, method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if 200 <= resp.getcode() < 300:
                    return True
        except urllib.error.HTTPError as e:
            # If unauthenticated, setting IAM may be pending; keep polling
            pass
        except Exception:
            pass
        time.sleep(poll_seconds)
    return False


def upsert_cloud_run_service(project: str, region: str, service_body: Dict[str, Any]) -> Dict[str, Any]:
    """Create or replace a Cloud Run service using location-based API."""
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    run = build("run", "v1", credentials=creds, cache_discovery=False)

    service_name = service_body.get("metadata", {}).get("name")
    if not service_name:
        raise RuntimeError("Service body missing metadata.name")

    full_name = f"projects/{project}/locations/{region}/services/{service_name}"
    parent = f"projects/{project}/locations/{region}"

    # Make sure namespace field doesn't conflict for location-based API
    try:
        if "namespace" in service_body.get("metadata", {}):
            service_body["metadata"].pop("namespace", None)
    except Exception:
        pass

    try:
        # Check if service exists
        run.projects().locations().services().get(name=full_name).execute()
        # Replace existing service
        print(f"Updating existing Cloud Run service: {full_name}")
        op = run.projects().locations().services().replaceService(name=full_name, body=service_body).execute()
        if "name" in op:
            wait_for_operation(run, op["name"])
    except HttpError as e:
        if e.resp.status == 404:
            print(f"Creating new Cloud Run service: {full_name}")
            op = run.projects().locations().services().create(parent=parent, body=service_body).execute()
            if "name" in op:
                wait_for_operation(run, op["name"])
        else:
            raise

    # Fetch final service to get URL
    service = run.projects().locations().services().get(name=full_name).execute()
    return service


def set_public_invoker_binding(project: str, region: str, service_name: str) -> None:
    """Grant allUsers roles/run.invoker to the service."""
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    run = build("run", "v1", credentials=creds, cache_discovery=False)
    resource = f"projects/{project}/locations/{region}/services/{service_name}"

    policy = run.projects().locations().services().getIamPolicy(resource=resource).execute()
    bindings = policy.get("bindings", [])
    binding = next((b for b in bindings if b.get("role") == "roles/run.invoker"), None)
    if binding:
        members = set(binding.get("members", []))
        if "allUsers" in members:
            print("IAM binding already includes allUsers for roles/run.invoker")
            return
        members.add("allUsers")
        binding["members"] = sorted(list(members))
    else:
        bindings.append({
            "role": "roles/run.invoker",
            "members": ["allUsers"]
        })
    policy["bindings"] = bindings
    run.projects().locations().services().setIamPolicy(resource=resource, body={"policy": policy}).execute()
    print("Granted roles/run.invoker to allUsers")


def update_sessions_json(session_id: str, service_url: str, path: str = "sessions.json") -> None:
    """Append or update the service URL for the session in sessions.json."""
    data: Dict[str, Any] = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = f.read().strip()
                data = json.loads(raw) if raw else {}
                if not isinstance(data, dict):
                    data = {}
        except Exception:
            data = {}
    entry = data.get(session_id, {})
    if not isinstance(entry, dict):
        entry = {}
    entry["service_url"] = service_url
    data[session_id] = entry
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Updated {path} with session_id={session_id}, service_url={service_url}")


def main():
    # Resolve service name
    service_name = SERVICE_NAME or f"exec-{SESSION_ID}"

    # Ensure prefixes exist
    ensure_prefixes(BUCKET, [INPUT_PREFIX, OUTPUT_PREFIX])

    # Render YAML
    replacements = {
        "PROJECT_PLACEHOLDER": PROJECT_ID,
        "SESSION_PLACEHOLDER": SESSION_ID,
        "IMAGE_PLACEHOLDER": IMAGE_URI,
        "BUCKET_PLACEHOLDER": BUCKET,
        "INPUT_PREFIX_PLACEHOLDER": INPUT_PREFIX,
        "OUTPUT_PREFIX_PLACEHOLDER": OUTPUT_PREFIX,
        "USER_PLACEHOLDER": USER_EMAIL_SANITIZED,
        "SERVICE_ACCOUNT_PLACEHOLDER": SERVICE_ACCOUNT_EMAIL,
    }
    svc_body = load_and_render_yaml(YAML_PATH, replacements)
    # Enforce desired service name
    svc_body.setdefault("metadata", {})["name"] = service_name

    # Deploy
    service = upsert_cloud_run_service(PROJECT_ID, REGION, svc_body)
    # Poll for URL if not immediately available
    url = service.get("status", {}).get("url") or poll_service_url(PROJECT_ID, REGION, service_name, max_wait_seconds=300)
    if url:
        print(f"Cloud Run service URL: {url}")
    else:
        print("Warning: Service URL not yet available; will continue without failing.")

    # Make public
    set_public_invoker_binding(PROJECT_ID, REGION, service_name)

    # Update sessions.json if URL is present
    if url:
        update_sessions_json(SESSION_ID, url, path="sessions.json")
        # Health check (non-fatal)
        ready = health_check(url, path="/health", max_wait_seconds=180, poll_seconds=5)
        print(f"Health check: {'READY' if ready else 'NOT READY (timed out)'}")
    else:
        print("Skipped health check and sessions.json update because URL is not available yet.")


if __name__ == "__main__":
    main()


