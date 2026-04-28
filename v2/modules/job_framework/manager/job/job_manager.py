"""
Job Manager for FastAPI v2 - Backward compatibility wrapper
This wraps the new core.manager.JobManager for backward compatibility
"""
from typing import Optional, List, Callable, Awaitable, Any
from v2.modules.job_framework.core.manager import JobManager as CoreJobManager
from v2.modules.job_framework.core.factory import create_job_manager
from v2.modules.job_framework.services.job.job_service import JobService
from v2.modules.job_framework.models.job_model import (
    JobCreate, JobResponse, JobUpdate,
    MilestoneCreate, MilestoneResponse, MilestoneUpdate
)
from v2.common.logger import add_log


class JobManager:
    """
    JobManager - Backward compatibility wrapper.
    
    This class maintains backward compatibility with the old interface
    while using the new cloud-agnostic core manager internally.
    
    For new code, use core.manager.JobManager directly with adapters.
    """
    
    def __init__(self, service: Optional[JobService] = None):
        """
        Initialize job manager.
        
        Args:
            service: Optional JobService instance (for backward compatibility).
                    If provided, wraps it as FirestoreJobStore.
                    If None, tries to use Firestore if available, otherwise uses in-memory storage.
        """
        if service is not None:
            # Backward compatibility: wrap old JobService
            # Convert JobService to adapters
            from v2.modules.job_framework.adapters.storage.firestore.job_store import FirestoreJobStore
            from v2.modules.job_framework.adapters.storage.firestore.milestone_store import FirestoreMilestoneStore
            
            # Extract firestore service from JobService
            firestore_service = service._JobService__firestore_service
            
            job_store = FirestoreJobStore(
                firestore_service=firestore_service,
                collection_name=service._JobService__collection_name,
                session_collection_name=service._JobService__session_collection_name,
                user_collection_name=service._JobService__user_collection_name,
                counter_collection_name=service._JobService__counter_collection_name
            )
            
            milestone_store = FirestoreMilestoneStore(
                firestore_service=firestore_service,
                collection_name=service._JobService__collection_name
            )
            
            self._core_manager = CoreJobManager(
                job_store=job_store,
                milestone_store=milestone_store
            )
            add_log("JobManager: Initialized with JobService (backward compatibility mode)")
        else:
            # Try Firestore first (for current app), fallback to memory
            try:
                from v2.common.gcp import GcpManager
                gcp_manager = GcpManager._get_instance()
                firestore_service = gcp_manager._firestore_service
                
                self._core_manager = create_job_manager(
                    storage_type="firestore",
                    firestore_service=firestore_service,
                    session_collection_name="sessions",  # Match session framework collection name
                    user_collection_name="userCollection"  # Match session framework collection name
                )
                add_log("JobManager: Initialized with Firestore (auto-detected)")
            except Exception:
                # Fallback to in-memory storage
                self._core_manager = create_job_manager(storage_type="memory")
                add_log("JobManager: Initialized with in-memory storage (Firestore not available)")
    
    # Delegate all methods to core manager
    async def _create_job(
        self, 
        job_data: JobCreate, 
        execute_func: Optional[Callable[[], Awaitable[Any]]] = None
    ) -> JobResponse:
        """
        Create a new job. If execute_func provided, execution starts automatically.
        
        Args:
            job_data: Job creation data
            execute_func: Optional async function that accepts JobContext and returns a dict.
                          If provided, execution starts automatically after creation.
        """
        return await self._core_manager._create_job(job_data, execute_func)
    
    async def _get_job_by_id(self, job_id: str) -> Optional[JobResponse]:
        """Get job by ID"""
        return await self._core_manager._get_job_by_id(job_id)
    
    async def _update_job(self, job_id: str, job_data: JobUpdate) -> Optional[JobResponse]:
        """Update a job"""
        return await self._core_manager._update_job(job_id, job_data)
    
    async def _delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        return await self._core_manager._delete_job(job_id)
    
    async def _get_all_jobs(self) -> List[JobResponse]:
        """Get all jobs"""
        return await self._core_manager._get_all_jobs()
    
    async def _get_jobs_by_session_id(self, session_id: str, order_direction: str = 'desc') -> List[JobResponse]:
        """
        Get jobs by session ID, ordered by creation time.
        
        Args:
            session_id: Session ID to get jobs for
            order_direction: 'desc' for latest first (default), 'asc' for oldest first
        """
        return await self._core_manager._get_jobs_by_session_id(session_id, order_direction)
    
    async def _execute_job(self, job_id: str, execute_func: Optional[Callable[[], Awaitable[Any]]] = None) -> bool:
        """Execute a job"""
        return await self._core_manager._execute_job(job_id, execute_func)
    
    async def _save_milestone(self, milestone_data: MilestoneCreate) -> MilestoneResponse:
        """Save a milestone"""
        return await self._core_manager._save_milestone(milestone_data)
    
    async def _update_milestone(self, milestone_id: str, job_id: str, milestone_data: MilestoneUpdate) -> Optional[MilestoneResponse]:
        """Update a milestone"""
        return await self._core_manager._update_milestone(milestone_id, job_id, milestone_data)
    
    async def _check_milestone(self, milestone_id: str, job_id: str) -> Optional[MilestoneResponse]:
        """Check milestone status"""
        return await self._core_manager._check_milestone(milestone_id, job_id)
    
    async def _get_job_milestones(self, job_id: str) -> List[MilestoneResponse]:
        """Get all milestones for a job"""
        return await self._core_manager._get_job_milestones(job_id)
    
    async def _create_job_object(self, job_id: str, job_config: Optional[dict] = None):
        """Create job object (for backward compatibility)"""
        return await self._core_manager._create_job_object(job_id, job_config)
    
    async def _get_job_object(self, job_id: str):
        """Get job object (for backward compatibility)"""
        return await self._core_manager._get_job_object(job_id)
    
    async def _cancel_job(self, job_id: str, reason: Optional[str] = None) -> bool:
        """Cancel a running job"""
        return await self._core_manager._cancel_job(job_id, reason)
    
    async def _wait_for_job_completion(
        self, 
        job_id: str, 
        poll_interval: float = 0.5,
        timeout: Optional[float] = None
    ) -> JobResponse:
        """
        Wait for a job to complete by polling its status.
        
        This method polls the job status until it reaches a terminal state
        (COMPLETED, ERROR, or CANCELLED). Use this when you want to wait
        for job execution before returning a response to the frontend.
        
        Args:
            job_id: ID of the job to wait for
            poll_interval: Seconds between status checks (default: 0.5)
            timeout: Maximum seconds to wait (None = no timeout)
            
        Returns:
            JobResponse: The job with final status
            
        Raises:
            TimeoutError: If timeout is exceeded
            ValueError: If job not found
        """
        return await self._core_manager._wait_for_job_completion(job_id, poll_interval, timeout)

