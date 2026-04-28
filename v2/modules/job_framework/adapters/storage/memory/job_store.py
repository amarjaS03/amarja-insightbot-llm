"""
In-memory job store - Default adapter, no dependencies
"""
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from v2.modules.job_framework.core.interfaces import JobStore
from v2.modules.job_framework.models.job_model import (
    JobCreate, JobResponse, JobUpdate, JobStatus
)
from v2.common.logger import add_log


class InMemoryJobStore(JobStore):
    """
    In-memory implementation of JobStore.
    Works out of the box - no external dependencies.
    Data is lost on restart.
    """
    
    def __init__(self):
        self._jobs: Dict[str, dict] = {}  # Changed from Dict[int, dict]
        self._lock = asyncio.Lock()
        self._session_job_ids: Dict[str, List[str]] = {}  # Changed to string keys for session_id
        add_log("InMemoryJobStore: Initialized")
    

    async def _validate_user_exists(self, user_id: str) -> bool:
        """Validate if user exists (user_id is UUID string)"""
        return True
    
    async def _validate_session_exists(self, session_id: str) -> bool:
        return True
    
    def _update_session_job_ids_internal(self, session_id: str, job_id: str, operation: str) -> None:
            if session_id not in self._session_job_ids:
                self._session_job_ids[session_id] = []
            
            if operation == 'add':
                if job_id not in self._session_job_ids[session_id]:
                    self._session_job_ids[session_id].append(job_id)
            elif operation == 'remove':
                if job_id in self._session_job_ids[session_id]:
                    self._session_job_ids[session_id].remove(job_id)
    
    async def _update_session_job_ids(self, session_id: str, job_id: str, operation: str) -> None:
        async with self._lock:
            self._update_session_job_ids_internal(session_id, job_id, operation)
    
    async def _create_job(self, job_data: JobCreate) -> JobResponse:
        import uuid
        job_id = str(uuid.uuid4())  # Generate UUID instead of sequential int
        now = datetime.now()
        
        job_dict = {
            'job_id': job_id,
            'user_id': job_data.user_id,
            'session_id': job_data.session_id,
            'job_type': job_data.job_type,
            'status': JobStatus.PENDING.value,
            'label': job_data.label,
            'created_on': now,
            'started_at': None,
            'completed_at': None,
            'error_message': None,
            'error_type': None,
            'execution_metadata': job_data.execution_metadata or {},
            'result_metadata': None,
            'job_config': job_data.job_config or {}
        }
        
        async with self._lock:
            self._jobs[job_id] = job_dict
        
        await self._update_session_job_ids(job_data.session_id, job_id, 'add')
        
        return JobResponse(
            job_id=job_id,
            user_id=job_dict['user_id'],
            session_id=job_dict['session_id'],
            job_type=job_dict['job_type'],
            status=JobStatus(job_dict['status']),
            label=job_dict['label'],
            created_on=job_dict['created_on'],
            started_at=job_dict['started_at'],
            completed_at=job_dict['completed_at'],
            error_message=job_dict['error_message'],
            error_type=job_dict['error_type'],
            execution_metadata=job_dict['execution_metadata'],
            result_metadata=job_dict['result_metadata'],
            job_config=job_dict['job_config']
        )
    
    async def _get_job_by_id(self, job_id: str) -> Optional[JobResponse]:
        async with self._lock:
            job_dict = self._jobs.get(job_id)
            if not job_dict:
                return None
            
            return JobResponse(
                job_id=job_dict['job_id'],
                user_id=job_dict['user_id'],
                session_id=job_dict['session_id'],
                job_type=job_dict['job_type'],
                status=JobStatus(job_dict['status']),
                label=job_dict['label'],
                created_on=job_dict['created_on'],
                started_at=job_dict['started_at'],
                completed_at=job_dict['completed_at'],
                error_message=job_dict['error_message'],
                error_type=job_dict['error_type'],
                execution_metadata=job_dict['execution_metadata'],
                result_metadata=job_dict['result_metadata'],
                job_config=job_dict['job_config']
            )
    
    async def _update_job(self, job_id: str, job_data: JobUpdate) -> Optional[JobResponse]:
        async with self._lock:
            job_dict = self._jobs.get(job_id)
            if not job_dict:
                return None
            
            if job_data.status is not None:
                job_dict['status'] = job_data.status.value
            if job_data.label is not None:
                job_dict['label'] = job_data.label
            if job_data.error_message is not None:
                job_dict['error_message'] = job_data.error_message
            if job_data.error_type is not None:
                job_dict['error_type'] = job_data.error_type
            if job_data.execution_metadata is not None:
                job_dict['execution_metadata'] = job_data.execution_metadata
            if job_data.result_metadata is not None:
                job_dict['result_metadata'] = job_data.result_metadata
            if job_data.job_config is not None:
                job_dict['job_config'] = job_data.job_config
            if job_data.started_at is not None:
                job_dict['started_at'] = job_data.started_at
            if job_data.completed_at is not None:
                job_dict['completed_at'] = job_data.completed_at
            elif job_data.status in [JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.CANCELLED]:
                job_dict['completed_at'] = datetime.now()
            
            return JobResponse(
                job_id=job_dict['job_id'],
                user_id=job_dict['user_id'],
                session_id=job_dict['session_id'],
                job_type=job_dict['job_type'],
                status=JobStatus(job_dict['status']),
                label=job_dict['label'],
                created_on=job_dict['created_on'],
                started_at=job_dict['started_at'],
                completed_at=job_dict['completed_at'],
                error_message=job_dict['error_message'],
                error_type=job_dict['error_type'],
                execution_metadata=job_dict['execution_metadata'],
                result_metadata=job_dict['result_metadata'],
                job_config=job_dict['job_config']
            )
    
    async def _delete_job(self, job_id: str) -> bool:
        async with self._lock:
            job_dict = self._jobs.get(job_id)
            if not job_dict:
                return False
            
            session_id = job_dict.get('session_id')
            if session_id:
                self._update_session_job_ids_internal(session_id, job_id, 'remove')
            
            del self._jobs[job_id]
            return True
    
    async def _get_all_jobs(self) -> List[JobResponse]:
        async with self._lock:
            jobs = []
            for job_dict in self._jobs.values():
                jobs.append(JobResponse(
                    job_id=job_dict['job_id'],
                    user_id=job_dict['user_id'],
                    session_id=job_dict['session_id'],
                    job_type=job_dict['job_type'],
                    status=JobStatus(job_dict['status']),
                    label=job_dict['label'],
                    created_on=job_dict['created_on'],
                    started_at=job_dict['started_at'],
                    completed_at=job_dict['completed_at'],
                    error_message=job_dict['error_message'],
                    error_type=job_dict['error_type'],
                    execution_metadata=job_dict['execution_metadata'],
                    result_metadata=job_dict['result_metadata'],
                    job_config=job_dict['job_config']
                ))
            return jobs
    
    async def _get_jobs_by_session_id(self, session_id: str, order_direction: str = 'desc') -> List[JobResponse]:
        """
        Get all jobs for a session, ordered by creation time.
        
        Args:
            session_id: Session ID to get jobs for
            order_direction: 'desc' for latest first (default), 'asc' for oldest first
        """
        async with self._lock:
            job_ids = self._session_job_ids.get(session_id, [])
            jobs = []
            for job_id in job_ids:
                job_dict = self._jobs.get(job_id)
                if job_dict:
                    jobs.append(JobResponse(
                        job_id=job_dict['job_id'],
                        user_id=job_dict['user_id'],
                        session_id=job_dict['session_id'],
                        job_type=job_dict['job_type'],
                        status=JobStatus(job_dict['status']),
                        label=job_dict['label'],
                        created_on=job_dict['created_on'],
                        started_at=job_dict['started_at'],
                        completed_at=job_dict['completed_at'],
                        error_message=job_dict['error_message'],
                        error_type=job_dict['error_type'],
                        execution_metadata=job_dict['execution_metadata'],
                        result_metadata=job_dict['result_metadata'],
                        job_config=job_dict['job_config']
                    ))
            # Sort by created_on: descending (latest first) or ascending (oldest first)
            reverse_order = (order_direction == 'desc')
            jobs.sort(key=lambda j: j.created_on if j.created_on else datetime.min, reverse=reverse_order)
            return jobs
