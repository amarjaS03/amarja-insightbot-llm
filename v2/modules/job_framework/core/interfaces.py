"""
Core interfaces for the job framework.

These define the contracts that storage adapters must implement:
- JobStore: Stores job records
- MilestoneStore: Stores milestone records (internal use)
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Callable, Awaitable, Any
from v2.modules.job_framework.models.job_model import (
    JobCreate, JobResponse, JobUpdate,
    MilestoneCreate, MilestoneResponse, MilestoneUpdate
)


class JobStore(ABC):
    """Abstract interface for job storage"""
    
    @abstractmethod
    async def _create_job(self, job_data: JobCreate) -> JobResponse:
        """Create a new job"""
        pass
    
    @abstractmethod
    async def _get_job_by_id(self, job_id: str) -> Optional[JobResponse]:
        """Get job by ID"""
        pass
    
    @abstractmethod
    async def _update_job(self, job_id: str, job_data: JobUpdate) -> Optional[JobResponse]:
        """Update a job"""
        pass
    
    @abstractmethod
    async def _delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        pass
    
    @abstractmethod
    async def _get_all_jobs(self) -> List[JobResponse]:
        """Get all jobs"""
        pass
    
    @abstractmethod
    async def _get_jobs_by_session_id(self, session_id: str, order_direction: str = 'desc') -> List[JobResponse]:
        """
        Get all jobs for a session, ordered by creation time.
        
        Args:
            session_id: Session ID to get jobs for
            order_direction: 'desc' for latest first (default), 'asc' for oldest first
        """
        pass
    
    @abstractmethod
    async def _validate_user_exists(self, user_id: str) -> bool:
        """Validate if user exists (user_id is UUID string)"""
        pass
    
    @abstractmethod
    async def _validate_session_exists(self, session_id: str) -> bool:
        """Validate if session exists"""
        pass
    
    @abstractmethod
    async def _update_session_job_ids(self, session_id: str, job_id: str, operation: str) -> None:
        """Update session's job_ids array"""
        pass


class MilestoneStore(ABC):
    """Abstract interface for milestone storage (internal use)"""
    
    @abstractmethod
    async def _create_milestone(self, milestone_data: MilestoneCreate) -> MilestoneResponse:
        """Create a milestone"""
        pass
    
    @abstractmethod
    async def _get_milestone(self, milestone_id: str, job_id: str) -> Optional[MilestoneResponse]:
        """Get milestone by ID"""
        pass
    
    @abstractmethod
    async def _update_milestone(self, milestone_id: str, job_id: str, milestone_data: MilestoneUpdate) -> Optional[MilestoneResponse]:
        """Update a milestone"""
        pass
    
    @abstractmethod
    async def _get_job_milestones(self, job_id: str) -> List[MilestoneResponse]:
        """Get all milestones for a job"""
        pass
    
    @abstractmethod
    async def _delete_job_milestones(self, job_id: str) -> None:
        """Delete all milestones for a job"""
        pass

