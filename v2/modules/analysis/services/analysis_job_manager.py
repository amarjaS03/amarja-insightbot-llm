"""
Simple in-memory job manager for analysis jobs.
Matches v1's job_manager pattern but simplified for v2's use case.
"""
import uuid
import time
import threading
from typing import Dict, Optional, Any
from enum import Enum
from v2.common.logger import add_log


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AnalysisJobManager:
    """Simple in-memory job manager for analysis execution"""
    
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
    
    def create_job(self, session_id: str, user_id: str, query: str, model: str = "gpt-5.4") -> tuple[str, Dict[str, Any]]:
        """
        Create a new analysis job.
        
        Returns:
            tuple: (job_id, job_info)
        """
        # Generate job ID with JOB prefix (matching v1 pattern)
        job_id = f"JOB_{str(uuid.uuid4())}"
        
        with self.lock:
            job_info = {
                'job_id': job_id,
                'session_id': session_id,
                'user_id': user_id,
                'status': JobStatus.PENDING,
                'query': query,
                'model': model,
                'created_at': time.time(),
                'started_at': None,
                'completed_at': None,
                'error': None,
            }
            
            self.jobs[job_id] = job_info
            add_log(f"[ANALYSIS_JOB] Created job: {job_id} for session: {session_id}", job_id=job_id)
            return job_id, job_info
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job information by ID"""
        with self.lock:
            return self.jobs.get(job_id, {}).copy() if job_id in self.jobs else None
    
    def update_job_status(self, job_id: str, status: JobStatus, error: Optional[str] = None) -> bool:
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
            
            add_log(f"[ANALYSIS_JOB] Job {job_id} status updated: {old_status.value if isinstance(old_status, JobStatus) else old_status} -> {status.value}", job_id=job_id)
            return True
    
    def get_room_name(self, job_id: str) -> str:
        """Get socket room name for a job"""
        job_info = self.get_job(job_id)
        if job_info:
            session_id = job_info.get('session_id', '')
            if session_id:
                return f'session_{session_id}_job_{job_id}'
        return f'job_{job_id}'
