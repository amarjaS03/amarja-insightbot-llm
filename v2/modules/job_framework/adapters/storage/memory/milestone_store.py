"""
In-memory milestone store - Internal use for lifecycle tracking
"""
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from v2.modules.job_framework.core.interfaces import MilestoneStore
from v2.modules.job_framework.models.job_model import (
    MilestoneCreate, MilestoneResponse, MilestoneUpdate
)
from v2.common.logger import add_log
import uuid


class InMemoryMilestoneStore(MilestoneStore):
    """In-memory implementation of MilestoneStore"""
    
    def __init__(self):
        self._milestones: Dict[str, dict] = {}
        self._job_milestones: Dict[str, List[str]] = {}  # Changed from Dict[int, List[str]]
        self._lock = asyncio.Lock()
        add_log("InMemoryMilestoneStore: Initialized")
    
    async def _create_milestone(self, milestone_data: MilestoneCreate) -> MilestoneResponse:
        milestone_id = str(uuid.uuid4())
        now = datetime.now()
        
        # Get current job status (use provided status or default to 'running' since milestones are created during execution)
        current_job_status = milestone_data.job_status or 'running'
        
        milestone_dict = {
            'milestone_id': milestone_id,
            'job_id': milestone_data.job_id,
            'name': milestone_data.name,
            'description': milestone_data.description,
            'job_status': current_job_status,  # Track job status at this milestone point
            'created_at': now,
            'updated_at': now,
            'data': milestone_data.data or {}
        }
        
        async with self._lock:
            self._milestones[milestone_id] = milestone_dict
            if milestone_data.job_id not in self._job_milestones:
                self._job_milestones[milestone_data.job_id] = []
            self._job_milestones[milestone_data.job_id].append(milestone_id)
        
        return MilestoneResponse(
            milestone_id=milestone_id,
            job_id=milestone_dict['job_id'],
            name=milestone_dict['name'],
            description=milestone_dict['description'],
            job_status=milestone_dict['job_status'],
            created_at=milestone_dict['created_at'],
            updated_at=milestone_dict['updated_at'],
            data=milestone_dict['data']
        )
    
    async def _get_milestone(self, milestone_id: str, job_id: str) -> Optional[MilestoneResponse]:
        async with self._lock:
            milestone_dict = self._milestones.get(milestone_id)
            if not milestone_dict or milestone_dict['job_id'] != job_id:
                return None
            
            return MilestoneResponse(
                milestone_id=milestone_dict['milestone_id'],
                job_id=milestone_dict['job_id'],
                name=milestone_dict['name'],
                description=milestone_dict['description'],
                job_status=milestone_dict.get('job_status') or milestone_dict.get('status'),  # Support both for backward compatibility
                created_at=milestone_dict['created_at'],
                updated_at=milestone_dict['updated_at'],
                data=milestone_dict['data']
            )
    
    async def _update_milestone(self, milestone_id: str, job_id: str, milestone_data: MilestoneUpdate) -> Optional[MilestoneResponse]:
        async with self._lock:
            milestone_dict = self._milestones.get(milestone_id)
            if not milestone_dict or milestone_dict['job_id'] != job_id:
                return None
            
            if milestone_data.job_status is not None:
                milestone_dict['job_status'] = milestone_data.job_status
            if milestone_data.name is not None:
                milestone_dict['name'] = milestone_data.name
            if milestone_data.description is not None:
                milestone_dict['description'] = milestone_data.description
            if milestone_data.data is not None:
                milestone_dict['data'] = milestone_data.data
            
            milestone_dict['updated_at'] = datetime.now()
            
            return MilestoneResponse(
                milestone_id=milestone_dict['milestone_id'],
                job_id=milestone_dict['job_id'],
                name=milestone_dict['name'],
                description=milestone_dict['description'],
                job_status=milestone_dict.get('job_status') or milestone_dict.get('status'),  # Support both for backward compatibility
                created_at=milestone_dict['created_at'],
                updated_at=milestone_dict['updated_at'],
                data=milestone_dict['data']
            )
    
    async def _get_job_milestones(self, job_id: str) -> List[MilestoneResponse]:
        """Get all milestones for a job, ordered by creation time (created_at)"""
        async with self._lock:
            milestone_ids = self._job_milestones.get(job_id, [])
            milestones = []
            for milestone_id in milestone_ids:
                milestone_dict = self._milestones.get(milestone_id)
                if milestone_dict:
                    milestones.append(MilestoneResponse(
                        milestone_id=milestone_dict['milestone_id'],  # milestone_id is UUID string
                        job_id=milestone_dict['job_id'],
                        name=milestone_dict['name'],
                        description=milestone_dict['description'],
                        job_status=milestone_dict.get('job_status') or milestone_dict.get('status'),  # Support both for backward compatibility
                        created_at=milestone_dict['created_at'],
                        updated_at=milestone_dict['updated_at'],
                        data=milestone_dict['data']
                    ))
            # Sort by created_at to ensure correct order (milestones are appended in order, but sort for safety)
            milestones.sort(key=lambda m: m.created_at if m.created_at else datetime.min)
            return milestones
    
    async def _delete_job_milestones(self, job_id: str) -> None:
        async with self._lock:
            milestone_ids = self._job_milestones.get(job_id, [])
            for milestone_id in milestone_ids:
                self._milestones.pop(milestone_id, None)
            self._job_milestones.pop(job_id, None)
