"""
Logger module for V2 - Self-contained logging utility
"""
import time
import threading

# Global logs storage
_logs = []
_logs_lock = threading.Lock()
# Job-specific logs storage
_job_logs = {}  # job_id -> [log_entries]
_job_logs_lock = threading.Lock()
# Pending emits for real-time streaming (job_id, log_entry)
_pending_emits = []
_pending_emits_lock = threading.Lock()


def add_log(message, job_id=None):
    """
    Add a log message to the global logs storage and optionally to job-specific logs.
    When job_id is provided, also queues for real-time emit to job_logs room (reconnect-safe).
    """
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_entry = {
        "timestamp": timestamp,
        "message": message,
        "job_id": job_id
    }

    with _logs_lock:
        _logs.append(log_entry)

    if job_id:
        with _job_logs_lock:
            if job_id not in _job_logs:
                _job_logs[job_id] = []
            _job_logs[job_id].append(log_entry)
        with _pending_emits_lock:
            _pending_emits.append((job_id, log_entry))

    # Print to console for debugging
    print(f"[{timestamp}] {message}")


def get_and_clear_pending_job_log_emits():
    """Return and clear pending (job_id, log_entry) for real-time emit. Used by socketio."""
    with _pending_emits_lock:
        out = _pending_emits[:]
        _pending_emits.clear()
        return out


def get_logs():
    """Get all logs from the global logs storage"""
    with _logs_lock:
        return _logs.copy()


def get_job_logs(job_id):
    """Get logs for a specific job."""
    with _job_logs_lock:
        return _job_logs.get(job_id, []).copy()


def clear_logs():
    """Clear all logs from the global logs storage"""
    with _logs_lock:
        _logs.clear()


def clear_job_logs(job_id):
    """Clear logs for a specific job."""
    with _job_logs_lock:
        if job_id in _job_logs:
            _job_logs[job_id].clear()

