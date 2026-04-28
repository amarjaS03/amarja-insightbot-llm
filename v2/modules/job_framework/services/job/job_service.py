"""
Job service for FastAPI v2
Service layer for job operations - handles Firestore storage
"""
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from v2.modules.job_framework.models.job_model import (
    JobCreate, JobResponse, JobUpdate, JobStatus,
    MilestoneCreate, MilestoneResponse, MilestoneUpdate
)
from v2.common.gcp import GcpManager
from v2.common.logger import add_log
import traceback
from google.cloud import firestore


class JobService:
    """
    Service layer for job operations.
    Manages Firestore storage of jobs and milestones.
    Matches the Firestore schema exactly.
    """
    
    def __init__(self):
        """Initialize job service with Firestore"""
        self.__gcp_manager = GcpManager._get_instance()
        self.__collection_name = "JOBS"
        self.__session_collection_name = "sessions"
        self.__user_collection_name = "userCollection"
        self.__counter_collection_name = "counters"
        self.__firestore_service = self.__gcp_manager._firestore_service
        add_log("JobService: Initialized with Firestore storage")
    
    def _get_next_job_id(self) -> int:
        """
        Get next sequential job_id using counter collection.
        Uses atomic increment for thread safety.
        """
        try:
            counter_ref = self.__firestore_service._client.collection(
                self.__counter_collection_name
            ).document("jobs")
            
            # Use transaction for atomic increment
            transaction = self.__firestore_service._client.transaction()
            
            @firestore.transactional
            def increment_counter(transaction):
                counter_doc = counter_ref.get(transaction=transaction)
                if counter_doc.exists:
                    current_count = counter_doc.to_dict().get('count', 0)
                    new_count = current_count + 1
                else:
                    new_count = 1
                
                transaction.set(counter_ref, {'count': new_count})
                return new_count
            
            job_id = increment_counter(transaction)
            return job_id
        except Exception as e:
            add_log(f"JobService: Error getting next job_id: {str(e)} | traceback: {traceback.format_exc()}")
            # Fallback: use timestamp-based ID if counter fails
            return int(datetime.now().timestamp() * 1000) % 1000000000
    
    async def _validate_user_exists(self, user_id: str) -> bool:
        """Validate if user exists in userCollection"""
        try:
            user_doc = await self.__firestore_service._get_document(
                self.__user_collection_name, 
                str(user_id)
            )
            return user_doc is not None
        except Exception as e:
            add_log(f"Error validating user {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def _validate_session_exists(self, session_id: str) -> bool:
        """Validate if session exists in SESSIONS collection"""
        try:
            session_doc = await self.__firestore_service._get_document(
                self.__session_collection_name,
                str(session_id)
            )
            return session_doc is not None
        except Exception as e:
            add_log(f"Error validating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    def _convert_firestore_to_response(self, doc_id: str, doc_data: dict) -> JobResponse:
        """Convert Firestore document to JobResponse"""
        # Use job_id from document if available, otherwise use doc_id (UUID)
        job_id = doc_data.get('job_id', doc_id)
        # Ensure job_id is a string (UUID)
        job_id = str(job_id)
        
        # Get session_id - ensure it's a string (UUID)
        session_id = doc_data.get('session_id')
        if session_id is not None:
            session_id = str(session_id)
        
        # Get user_id - ensure it's a string (UUID)
        user_id = doc_data.get('user_id')
        if user_id is not None:
            user_id = str(user_id)
        
        return JobResponse(
            job_id=job_id,
            user_id=user_id or "",  # Ensure string, default to empty string
            session_id=session_id,
            job_type=doc_data.get('job_type', ''),
            status=JobStatus(doc_data.get('status', 'pending')),
            label=doc_data.get('label', ''),
            created_on=doc_data.get('created_on'),
            started_at=doc_data.get('started_at'),
            completed_at=doc_data.get('completed_at'),
            error_message=doc_data.get('error_message'),
            error_type=doc_data.get('error_type'),
            execution_metadata=doc_data.get('execution_metadata'),
            result_metadata=doc_data.get('result_metadata'),
            job_config=doc_data.get('job_config')
        )
    
    async def _create_job(self, job_data: JobCreate) -> JobResponse:
        """
        Create a new job in Firestore.
        Validates user and session exist, generates int job_id, and updates session's job_ids array.
        """
        try:
            # Step 1: Validate user exists
            if not await self._validate_user_exists(job_data.user_id):
                raise ValueError(f"User with ID '{job_data.user_id}' does not exist")
            
            # Step 2: Validate session exists
            if not await self._validate_session_exists(job_data.session_id):
                raise ValueError(f"Session with ID '{job_data.session_id}' does not exist")
            
            # Step 3: Generate next job_id (wrap in asyncio.to_thread to avoid blocking)
            job_id = await asyncio.to_thread(self._get_next_job_id)
            
            # Step 4: Prepare job document data with all required fields
            now = datetime.now(timezone.utc)  # Use timezone-aware datetime for Firestore
            doc_data = {
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
            
            # Step 5: Create job document with job_id as document ID (string)
            doc_id = str(job_id)
            await self.__firestore_service._set_document(self.__collection_name, doc_id, doc_data)
            
            # Step 6: Update session's job_ids array
            session_doc_ref = self.__firestore_service._client.collection(
                self.__session_collection_name
            ).document(str(job_data.session_id))
            
            session_doc = await asyncio.to_thread(session_doc_ref.get)
            if not session_doc.exists:
                # Rollback: delete job if session doesn't exist
                await self.__firestore_service._delete_document(self.__collection_name, doc_id)
                raise ValueError(f"Session '{job_data.session_id}' was deleted during job creation")
            
            # Get existing job_ids array from session document
            session_data = session_doc.to_dict() or {}
            existing_job_ids = session_data.get('job_ids', [])
            
            # Add job_id if not already present
            if job_id not in existing_job_ids:
                existing_job_ids.append(job_id)
                await asyncio.to_thread(session_doc_ref.update, {'job_ids': existing_job_ids})
            
            add_log(f"JobService: Created job {job_id} of type {job_data.job_type}")
            
            # Step 7: Fetch complete job document and return response
            created_doc = await self.__firestore_service._get_document(self.__collection_name, doc_id)
            
            return self._convert_firestore_to_response(doc_id, created_doc)
        except ValueError:
            raise
        except Exception as e:
            add_log(f"JobService: Error creating job: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_job_by_id(self, job_id: str) -> Optional[JobResponse]:
        """
        Get a job by ID.
        
        Args:
            job_id: Job identifier (UUID string)
            
        Returns:
            Optional[JobResponse]: Job response if found, None otherwise
        """
        try:
            doc_id = str(job_id)
            doc = await self.__firestore_service._get_document(self.__collection_name, doc_id)
            
            if doc:
                return self._convert_firestore_to_response(doc_id, doc)
            return None
        except Exception as e:
            add_log(f"JobService: Error getting job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _update_job(self, job_id: str, job_data: JobUpdate) -> Optional[JobResponse]:
        """
        Update a job.
        
        Args:
            job_id: Job identifier (UUID string)
            job_data: Job update data
            
        Returns:
            Optional[JobResponse]: Updated job response if found, None otherwise
        """
        try:
            doc_id = str(job_id)
            existing_doc = await self.__firestore_service._get_document(self.__collection_name, doc_id)
            if not existing_doc:
                return None
            
            # Prepare update data (only include fields that are provided)
            update_data = {}
            if job_data.status is not None:
                update_data['status'] = job_data.status.value
            if job_data.label is not None:
                update_data['label'] = job_data.label
            if job_data.error_message is not None:
                update_data['error_message'] = job_data.error_message
            if job_data.error_type is not None:
                update_data['error_type'] = job_data.error_type
            if job_data.execution_metadata is not None:
                update_data['execution_metadata'] = job_data.execution_metadata
            if job_data.result_metadata is not None:
                update_data['result_metadata'] = job_data.result_metadata
            if job_data.job_config is not None:
                update_data['job_config'] = job_data.job_config
            if job_data.started_at is not None:
                update_data['started_at'] = job_data.started_at
            if job_data.completed_at is not None:
                update_data['completed_at'] = job_data.completed_at
            elif job_data.status in [JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.CANCELLED]:
                # Auto-set completed_at if status changed to terminal state
                update_data['completed_at'] = datetime.now(timezone.utc)  # Use timezone-aware datetime for Firestore
            
            # Update document
            if update_data:
                await self.__firestore_service._update_document(self.__collection_name, doc_id, update_data)
            
            # Get updated document
            updated_doc = await self.__firestore_service._get_document(self.__collection_name, doc_id)
            return self._convert_firestore_to_response(doc_id, updated_doc)
        except Exception as e:
            add_log(f"JobService: Error updating job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_all_jobs(self) -> List[JobResponse]:
        """
        Get all jobs.
        
        Returns:
            List[JobResponse]: List of all jobs
        """
        try:
            jobs_data = await self.__firestore_service._get_all_documents(self.__collection_name)
            
            jobs = []
            for doc in jobs_data:
                doc_id = doc.get('id', '')
                jobs.append(self._convert_firestore_to_response(doc_id, doc))
            
            return jobs
        except Exception as e:
            add_log(f"JobService: Error getting all jobs: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_jobs_by_session_id(self, session_id: str) -> List[JobResponse]:
        """
        Get all jobs for a specific session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            List[JobResponse]: List of jobs for the session
        """
        try:
            jobs_data = await self.__firestore_service._query_collection(
                self.__collection_name,
                filters=[('session_id', '==', session_id)]
            )
            
            jobs = []
            for doc in jobs_data:
                doc_id = doc.get('id', '')
                jobs.append(self._convert_firestore_to_response(doc_id, doc))
            
            return jobs
        except Exception as e:
            add_log(f"JobService: Error getting jobs for session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    
    async def _delete_job(self, job_id: str) -> bool:
        """
        Delete a job and remove it from session's job_ids array.
        
        Args:
            job_id: Job identifier
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        try:
            doc_id = str(job_id)
            
            # Get job document to find session_id
            job_doc = await self.__firestore_service._get_document(self.__collection_name, doc_id)
            if not job_doc:
                return False
            
            session_id = job_doc.get('session_id')
            
            # Remove job_id from session's job_ids array
            if session_id:
                session_doc_ref = self.__firestore_service._client.collection(
                    self.__session_collection_name
                ).document(str(session_id))
                
                session_doc = await asyncio.to_thread(session_doc_ref.get)
                if session_doc.exists:
                    session_data = session_doc.to_dict() or {}
                    existing_job_ids = session_data.get('job_ids', [])
                    
                    # Remove job_id from array
                    if job_id in existing_job_ids:
                        existing_job_ids.remove(job_id)
                        await asyncio.to_thread(session_doc_ref.update, {'job_ids': existing_job_ids})
            
            # Delete job document
            await self.__firestore_service._delete_document(self.__collection_name, doc_id)
            
            # Delete all milestones for this job (subcollection)
            milestones_collection = f"{self.__collection_name}/{doc_id}/Milestones"
            milestones = await self.__firestore_service._get_all_documents(milestones_collection)
            for milestone in milestones:
                milestone_id = milestone.get('id', '')
                if milestone_id:
                    await self.__firestore_service._delete_document(milestones_collection, milestone_id)
            
            add_log(f"JobService: Deleted job {job_id}")
            return True
        except Exception as e:
            add_log(f"JobService: Error deleting job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _save_milestone(self, milestone_data: MilestoneCreate) -> MilestoneResponse:
        """
        Save a new milestone for a job (as subcollection).
        
        Args:
            milestone_data: Milestone creation data
            
        Returns:
            MilestoneResponse: Created milestone response
        """
        try:
            # Validate job exists
            job_doc = await self._get_job_by_id(milestone_data.job_id)
            if not job_doc:
                raise ValueError(f"Job {milestone_data.job_id} not found")
            
            # Create milestone in subcollection
            milestones_collection = f"{self.__collection_name}/{str(milestone_data.job_id)}/Milestones"
            now = datetime.now(timezone.utc)  # Use timezone-aware datetime for Firestore
            
            milestone_dict = {
                'job_id': milestone_data.job_id,
                'name': milestone_data.name,
                'description': milestone_data.description,
                'status': 'pending',
                'created_at': now,
                'updated_at': now,
                'data': milestone_data.data or {}
            }
            
            milestone_id = await self.__firestore_service._create_document(milestones_collection, milestone_dict)
            
            add_log(f"JobService: Created milestone {milestone_id} for job {milestone_data.job_id}")
            
            return MilestoneResponse(
                milestone_id=milestone_id,
                job_id=milestone_data.job_id,
                name=milestone_dict['name'],
                description=milestone_dict['description'],
                status=milestone_dict['status'],
                created_at=milestone_dict['created_at'],
                updated_at=milestone_dict['updated_at'],
                data=milestone_dict['data']
            )
        except ValueError:
            raise
        except Exception as e:
            add_log(f"JobService: Error creating milestone: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _update_milestone(self, milestone_id: str, job_id: str, milestone_data: MilestoneUpdate) -> Optional[MilestoneResponse]:
        """
        Update a milestone's status and/or data.
        
        Args:
            milestone_id: ID of the milestone to update
            job_id: ID of the job this milestone belongs to
            milestone_data: Milestone update data
            
        Returns:
            Optional[MilestoneResponse]: Updated milestone response if found, None otherwise
        """
        try:
            milestones_collection = f"{self.__collection_name}/{str(job_id)}/Milestones"
            milestone_doc = await self.__firestore_service._get_document(milestones_collection, milestone_id)
            
            if not milestone_doc:
                return None
            
            update_data = {}
            if milestone_data.status is not None:
                update_data['status'] = milestone_data.status
            if milestone_data.name is not None:
                update_data['name'] = milestone_data.name
            if milestone_data.description is not None:
                update_data['description'] = milestone_data.description
            if milestone_data.data is not None:
                update_data['data'] = milestone_data.data
            
            update_data['updated_at'] = datetime.now(timezone.utc)  # Use timezone-aware datetime for Firestore
            
            if update_data:
                await self.__firestore_service._update_document(milestones_collection, milestone_id, update_data)
            
            # Get updated milestone
            updated_doc = await self.__firestore_service._get_document(milestones_collection, milestone_id)
            
            return MilestoneResponse(
                milestone_id=milestone_id,
                job_id=job_id,
                name=updated_doc['name'],
                description=updated_doc.get('description'),
                status=updated_doc.get('status'),
                created_at=updated_doc['created_at'],
                updated_at=updated_doc['updated_at'],
                data=updated_doc.get('data')
            )
        except Exception as e:
            add_log(f"JobService: Error updating milestone {milestone_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _check_milestone(self, milestone_id: str, job_id: str) -> Optional[MilestoneResponse]:
        """
        Check the status of a milestone.
        
        Args:
            milestone_id: ID of the milestone to check
            job_id: ID of the job this milestone belongs to
            
        Returns:
            Optional[MilestoneResponse]: Milestone response if found, None otherwise
        """
        try:
            milestones_collection = f"{self.__collection_name}/{str(job_id)}/Milestones"
            milestone_doc = await self.__firestore_service._get_document(milestones_collection, milestone_id)
            
            if not milestone_doc:
                return None
            
            return MilestoneResponse(
                milestone_id=milestone_id,
                job_id=job_id,
                name=milestone_doc['name'],
                description=milestone_doc.get('description'),
                status=milestone_doc.get('status'),
                created_at=milestone_doc['created_at'],
                updated_at=milestone_doc['updated_at'],
                data=milestone_doc.get('data')
            )
        except Exception as e:
            add_log(f"JobService: Error checking milestone {milestone_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_job_milestones(self, job_id: str) -> List[MilestoneResponse]:
        """
        Get all milestones for a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            List[MilestoneResponse]: List of milestones for the job
        """
        try:
            milestones_collection = f"{self.__collection_name}/{str(job_id)}/Milestones"
            milestones_data = await self.__firestore_service._get_all_documents(milestones_collection)
            
            milestones = []
            for doc in milestones_data:
                doc_id = doc.get('id', '')
                milestones.append(MilestoneResponse(
                    milestone_id=doc_id,
                    job_id=job_id,
                    name=doc['name'],
                    description=doc.get('description'),
                    status=doc.get('status'),
                    created_at=doc['created_at'],
                    updated_at=doc['updated_at'],
                    data=doc.get('data')
                ))
            return milestones
        except Exception as e:
            add_log(f"JobService: Error getting milestones for job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
