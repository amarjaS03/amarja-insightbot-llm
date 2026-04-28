import os
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from google.cloud import storage
from google.oauth2 import service_account

from utils.env import KEY_FILE_PATH, GCP_PROJECT, init_env
from logger import get_logs, get_job_logs, add_log


def _utc_today_parts() -> Dict[str, str]:
    now = datetime.now(timezone.utc)
    return {
        "yyyy": f"{now.year:04d}",
        "mm": f"{now.month:02d}",
        "dd": f"{now.day:02d}",
        "ts": now.strftime("%Y%m%d_%H%M%S"),
    }


def _gcs_client():
    try:
        if KEY_FILE_PATH and KEY_FILE_PATH.exists():
            creds = service_account.Credentials.from_service_account_file(str(KEY_FILE_PATH.resolve()))
            project = GCP_PROJECT or getattr(creds, "project_id", None)
            return storage.Client(project=project, credentials=creds)
        # Fallback to ADC
        return storage.Client(project=GCP_PROJECT or None)
    except Exception as e:
        # Last resort, let storage try ADC with no explicit project
        try:
            add_log(f"log_aggregator._gcs_client error: {str(e)}")
        except Exception:
            pass
        return storage.Client()


def _upload_bytes(bucket_name: str, blob_path: str, data: bytes, content_type: str = "application/octet-stream") -> Optional[str]:
    try:
        client = _gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(data, content_type=content_type)
        return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"
    except Exception as e:
        try:
            add_log(f"log_aggregator._upload_bytes error for {blob_path}: {str(e)}")
        except Exception:
            pass
        return None


def _sanitize_email_for_path(email: Optional[str]) -> str:
    """
    Convert user email into a safe path segment.
    Falls back to 'unknown_user' when not provided.
    """
    if not email:
        return "unknown_user"
    # Lowercase and replace characters that are problematic in paths
    safe = email.strip().lower()
    for ch in ["@", " ", ":", ";"]:
        safe = safe.replace(ch, "_")
    # Keep dots but collapse consecutive separators
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe or "unknown_user"


def _load_file_text(path: Optional[str]) -> str:
    if not path:
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        try:
            add_log(f"log_aggregator._load_file_text error for {path}: {str(e)}")
        except Exception:
            pass
        return ""


def _parse_container_log_text_to_jsonl(text: str, job_id: str, session_id: str) -> List[Dict[str, Any]]:
    """
    Parse raw Docker container logs into structured JSON lines.

    Expected log format (Docker with --timestamps):
        "2025-11-18T09:14:30.579953Z Actual log message..."

    We try to split the leading ISO timestamp into the `timestamp` field and keep
    the remainder as the `message`. Empty/whitespace-only lines are skipped.
    """
    lines = text.splitlines()
    out: List[Dict[str, Any]] = []

    for ln in lines:
        raw = (ln or "").strip()
        if not raw:
            # Skip empty lines to avoid "" messages in JSONL
            continue

        ts_str: Optional[str] = None
        msg_str: str = raw

        # Attempt to split "TIMESTAMP rest-of-line"
        parts = raw.split(" ", 1)
        if parts:
            candidate_ts = parts[0]
            # Heuristic: Docker timestamps are ISO-like and contain 'T'
            if "T" in candidate_ts:
                try:
                    # Normalize possible trailing 'Z' to +00:00 for fromisoformat
                    iso_candidate = candidate_ts.replace("Z", "+00:00")
                    # fromisoformat will raise on invalid strings
                    datetime.fromisoformat(iso_candidate)
                    ts_str = candidate_ts
                    msg_str = parts[1] if len(parts) > 1 else ""
                except Exception:
                    # Not an ISO timestamp; treat entire line as message
                    ts_str = None
                    msg_str = raw

        out.append(
            {
                "timestamp": ts_str,
                "level": "INFO",
                "source": "container",
                "job_id": job_id,
                "session_id": session_id,
                "message": msg_str,
            }
        )

    return out


def _parse_logger_ts_to_epoch(ts: Optional[str]) -> Optional[float]:
    # logger.py uses "%Y-%m-%d %H:%M:%S" localtime
    if not ts:
        return None
    try:
        tstruct = time.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return time.mktime(tstruct)
    except Exception:
        return None


def _sort_entries_by_timestamp(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort entries by their timestamp (if available). Entries without a
    parseable timestamp are kept at the end in original order.
    """
    def _to_epoch(e: Dict[str, Any]) -> Optional[float]:
        ts = e.get("timestamp")
        if not ts:
            return None
        # Try logger format first
        epoch = _parse_logger_ts_to_epoch(ts)
        if epoch is not None:
            return epoch
        # Try generic ISO 8601 (e.g., container logs)
        try:
            iso_ts = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(iso_ts).timestamp()
        except Exception:
            return None

    with_ts: List[Dict[str, Any]] = []
    without_ts: List[Dict[str, Any]] = []
    for e in entries:
        if _to_epoch(e) is None:
            without_ts.append(e)
        else:
            with_ts.append(e)

    with_ts.sort(key=lambda e: _to_epoch(e) or 0.0)
    return with_ts + without_ts


def _collect_backend_logs(job_id: str, session_id: str, created_at: Optional[float], started_at: Optional[float], completed_at: Optional[float]) -> List[Dict[str, Any]]:
    """
    Combine logs from:
    - Explicit job logs (via get_job_logs(job_id))
    - Global logs STRICTLY filtered by job_id match (avoid cross-job leakage)
    """
    backend: List[Dict[str, Any]] = []

    # 1) Explicit job logs
    try:
        job_logs = get_job_logs(job_id)
    except Exception as e:
        try:
            add_log(f"log_aggregator._collect_backend_logs get_job_logs error for job {job_id}: {str(e)}")
        except Exception:
            pass
        job_logs = []
    for e in job_logs:
        backend.append({
            "timestamp": e.get("timestamp"),
            "level": e.get("level", "INFO"),
            "source": "backend",
            "job_id": e.get("job_id") or job_id,
            "session_id": session_id,
            "message": e.get("message"),
            "extra": e.get("extra", {}),
        })

    # 2) Global logs with STRICT job_id matching only
    try:
        all_logs = get_logs()
    except Exception as e:
        try:
            add_log(f"log_aggregator._collect_backend_logs get_logs error: {str(e)}")
        except Exception:
            pass
        all_logs = []

    for e in all_logs:
        if e.get("job_id") == job_id:
            backend.append({
                "timestamp": e.get("timestamp"),
                "level": e.get("level", "INFO"),
                "source": "backend",
                "job_id": job_id,
                "session_id": session_id,
                "message": e.get("message"),
                "extra": e.get("extra", {}),
            })

    return backend


def persist_combined_logs_to_gcs(
    *,
    job_id: str,
    session_id: str,
    user_email: Optional[str],
    container_logs_path: Optional[str],
    created_at: Optional[float],
    started_at: Optional[float],
    completed_at: Optional[float],
) -> Dict[str, Any]:
    """
    Gather backend and container logs, merge to JSONL, and upload to GCS.
    Returns dict of uploaded URLs.
    """
    consts = init_env()
    bucket_name = consts.get("storage_bucket")
    if not bucket_name:
        return {"error": "No storage_bucket configured"}

    date_parts = _utc_today_parts()
    # Premium structure:
    # logs/<user_email>/<yyyy>/<mm>/<dd>/<session_id>/<job_id>/
    safe_email = _sanitize_email_for_path(user_email)
    base_path = f"logs/{safe_email}/{date_parts['yyyy']}/{date_parts['mm']}/{date_parts['dd']}/{session_id}/{job_id}/"

    # Backend logs
    backend_entries = _collect_backend_logs(job_id, session_id, created_at, started_at, completed_at)

    # Container logs
    container_text = _load_file_text(container_logs_path)
    container_entries = _parse_container_log_text_to_jsonl(container_text, job_id, session_id)

    # Combined JSONL (sorted by timestamp when possible)
    combined = []
    combined.extend(backend_entries)
    combined.extend(container_entries)
    combined_sorted = _sort_entries_by_timestamp(combined)

    combined_bytes = ("\n".join(json.dumps(x, ensure_ascii=False) for x in combined_sorted) + "\n").encode("utf-8")
    backend_bytes = ("\n".join(json.dumps(x, ensure_ascii=False) for x in backend_entries) + "\n").encode("utf-8")
    container_txt_bytes = container_text.encode("utf-8")

    ts = date_parts["ts"]
    urls = {
        "combined": _upload_bytes(bucket_name, base_path + f"combined_{ts}.jsonl", combined_bytes, "application/json"),
        "backend": _upload_bytes(bucket_name, base_path + f"backend_{ts}.jsonl", backend_bytes, "application/json"),
        "container": _upload_bytes(bucket_name, base_path + f"container_{ts}.txt", container_txt_bytes, "text/plain"),
    }
    return urls


