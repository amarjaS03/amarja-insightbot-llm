import json
import os
import threading
import time

# Global logs storage
_logs = []
_logs_lock = threading.Lock()

# Job-specific logs storage
_job_logs = {}  # job_id -> [log_entries]
_job_logs_lock = threading.Lock()


def add_log(message, job_id=None):
    """Add a log message to global storage and optionally to job-specific logs."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_entry = {
        "timestamp": timestamp,
        "message": message,
        "job_id": job_id,
    }

    with _logs_lock:
        _logs.append(log_entry)

    if job_id:
        with _job_logs_lock:
            if job_id not in _job_logs:
                _job_logs[job_id] = []
            _job_logs[job_id].append(log_entry)

    print(f"[{timestamp}] {message}")


def add_job_log(job_id, message):
    """Add a log message specifically for a job and emit via Socket.IO when available."""
    add_log(message, job_id=job_id)

    try:
        from api_layer.api_server import get_socketio_instance

        socketio = get_socketio_instance()
        if socketio:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            log_entry = {"timestamp": timestamp, "message": message, "job_id": job_id}
            socketio.emit("job_log", log_entry, room=f"job_logs_{job_id}")
    except ImportError:
        pass


def get_logs():
    """Get all logs from global storage."""
    with _logs_lock:
        return _logs.copy()


def get_job_logs(job_id):
    """Get logs specific to a job."""
    with _job_logs_lock:
        return _job_logs.get(job_id, []).copy()


def clear_logs():
    """Clear all global logs."""
    with _logs_lock:
        _logs.clear()


def clear_job_logs(job_id):
    """Clear logs for a specific job."""
    with _job_logs_lock:
        if job_id in _job_logs:
            _job_logs[job_id].clear()


def clear_all_job_logs():
    """Clear all job-specific logs."""
    with _job_logs_lock:
        _job_logs.clear()


def save_logs_to_file(file_path="logs.json"):
    """Save logs to a file."""
    with _logs_lock:
        with open(file_path, "w") as f:
            json.dump(_logs, f, indent=2)


def load_logs_from_file(file_path="logs.json"):
    """Load logs from a file."""
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            loaded_logs = json.load(f)
        with _logs_lock:
            _logs.extend(loaded_logs)
    return get_logs()
