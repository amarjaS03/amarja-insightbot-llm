"""
Firestore job store adapter - Optional plugin
Requires google-cloud-firestore and GcpManager
User must provide Firestore service or configure GCP
"""
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from v2.modules.job_framework.core.interfaces import JobStore
from v2.modules.job_framework.models.job_model import (
    JobCreate, JobResponse, JobUpdate, JobStatus
)
from v2.common.logger import add_log
import traceback
from google.cloud import firestore


class FirestoreJobStore(JobStore):
    """
    Firestore implementation of JobStore.
    User must provide Firestore service or use GcpManager.
    """
    
    def __init__(
        self,
        firestore_service=None,
        collection_name: str = "JOBS",
        session_collection_name: str = "sessions",
        user_collection_name: str = "userCollection",
        counter_collection_name: str = "counters"
    ):
        """
        Initialize Firestore job store.
        
        Args:
            firestore_service: Firestore service instance (from GcpManager or user-provided)
            collection_name: Jobs collection name
            session_collection_name: Sessions collection name
            user_collection_name: Users collection name
            counter_collection_name: Counter collection name
        """
        if firestore_service is None:
            # Try to get from GcpManager if available
            try:
                from v2.common.gcp import GcpManager
                gcp_manager = GcpManager._get_instance()
                self._firestore_service = gcp_manager._firestore_service
            except Exception:
                raise ValueError(
                    "FirestoreJobStore requires firestore_service parameter or GcpManager must be configured. "
                    "Provide firestore_service explicitly for cloud-agnostic usage."
                )
        else:
            self._firestore_service = firestore_service
        
        self._collection_name = collection_name
        self._session_collection_name = session_collection_name
        self._user_collection_name = user_collection_name
        self._counter_collection_name = counter_collection_name
        add_log("FirestoreJobStore: Initialized with Firestore storage")
    
    async def _get_next_job_id(self) -> int:
        """Get next sequential job_id using counter collection"""
        try:
            counter_ref = self._firestore_service._client.collection(
                self._counter_collection_name
            ).document("jobs")
            
            def increment_counter(transaction):
                counter_doc = counter_ref.get(transaction=transaction)
                if counter_doc.exists:
                    current_count = counter_doc.to_dict().get('count', 0)
                    new_count = current_count + 1
                else:
                    new_count = 1
                transaction.set(counter_ref, {'count': new_count})
                return new_count
            
            transaction = self._firestore_service._client.transaction()
            result = await asyncio.to_thread(
                lambda: firestore.transactional(increment_counter)(transaction)
            )
            return result
        except Exception as e:
            add_log(f"FirestoreJobStore: Error getting next job_id: {str(e)} | traceback: {traceback.format_exc()}")
            return int(datetime.now().timestamp() * 1000) % 1000000000
    
    async def _validate_user_exists(self, user_id: str) -> bool:
        """Validate if user exists in userCollection (user_id is UUID string)"""
        try:
            user_doc = await self._firestore_service._get_document(
                self._user_collection_name, 
                user_id
            )
            return user_doc is not None
        except Exception as e:
            add_log(f"Error validating user {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def _validate_session_exists(self, session_id: str) -> bool:
        """Validate if session exists in SESSIONS collection"""
        try:
            session_doc = await self._firestore_service._get_document(
                self._session_collection_name,
                session_id
            )
            return session_doc is not None
        except Exception as e:
            add_log(f"Error validating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    def _convert_firestore_to_response(self, doc_id: str, doc_data: dict) -> JobResponse:
        """Convert Firestore document to JobResponse"""
        # Use doc_id (UUID) as job_id, or fallback to doc_data['job_id'] if exists
        job_id = doc_data.get('job_id', doc_id)  # Prefer stored job_id, fallback to doc_id
        
        # Get session_id - could be stored as string or int (for backward compatibility)
        session_id = doc_data.get('session_id')
        if session_id is not None:
            session_id = str(session_id)  # Ensure string
        
        # Get user_id - ensure it's a string (for backward compatibility, convert int to str if needed)
        user_id = doc_data.get('user_id')
        if user_id is not None:
            user_id = str(user_id)  # Ensure string (UUID)
        
        return JobResponse(
            job_id=str(job_id),  # Ensure string
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
        """Create a new job in Firestore with UUID"""
        try:
            # Get original string IDs from job_config if available (for Firestore lookups)
            job_config = job_data.job_config or {}
            original_user_id = job_config.get("_original_user_id")
            
            # Use original string ID for user validation if available, otherwise use string
            user_id_for_lookup = original_user_id if original_user_id else job_data.user_id
            
            # session_id is now always a string (UUID)
            session_id_for_lookup = job_data.session_id
            
            # Validate user exists (required for job creation)
            if not await self._validate_user_exists(job_data.user_id):
                raise ValueError(f"User with ID '{user_id_for_lookup}' does not exist")
            
            # Validate session exists (lenient - log warning but allow job creation)
            # Some workflows may create jobs before sessions are fully initialized
            session_exists = await self._validate_session_exists(job_data.session_id)
            if not session_exists:
                add_log(f"FirestoreJobStore: Warning - Session with ID '{session_id_for_lookup}' does not exist, but proceeding with job creation. Session may be created later or managed externally.")
            
            # Use _create_document() which generates UUID automatically
            now = datetime.now(timezone.utc)  # Use timezone-aware datetime for Firestore
            
            doc_data = {
                # No longer store job_id in doc_data - doc_id is the UUID
                'user_id': str(job_data.user_id),  # Store as string (UUID)
                'session_id': str(job_data.session_id),  # Store as string (UUID)
                'job_type': str(job_data.job_type),  # Ensure string
                'status': str(JobStatus.PENDING.value),  # Ensure string
                'label': str(job_data.label),  # Ensure string
                'created_on': now,  # This ensures ordering!
                'started_at': None,
                'completed_at': None,
                'error_message': None,
                'error_type': None,
                'execution_metadata': dict(job_data.execution_metadata or {}),  # Ensure dict
                'result_metadata': None,
                'job_config': dict(job_data.job_config or {})  # Ensure dict
            }
            
            # Use _create_document() instead of _set_document()
            # This generates a UUID automatically
            doc_id = await self._firestore_service._create_document(
                self._collection_name,
                doc_data
            )
            # doc_id is now the UUID generated by Firestore
            
            # Update session's job_ids array - use string ID for Firestore document lookup
            # Only update if session exists - don't fail if it doesn't (lenient validation)
            try:
                session_doc_ref = self._firestore_service._client.collection(
                    self._session_collection_name
                ).document(session_id_for_lookup)
                
                session_doc = await asyncio.to_thread(session_doc_ref.get)
                if session_doc.exists:
                    session_data = session_doc.to_dict() or {}
                    existing_job_ids = session_data.get('job_ids', [])
                    
                    # Use UUID string directly
                    if doc_id not in existing_job_ids:
                        existing_job_ids.append(doc_id)  # Append UUID string
                        await asyncio.to_thread(session_doc_ref.update, {'job_ids': existing_job_ids})
                        add_log(f"FirestoreJobStore: Updated session {session_id_for_lookup} with job_id {doc_id}")
                else:
                    add_log(f"FirestoreJobStore: Session {session_id_for_lookup} does not exist - skipping job_ids update (job created successfully)")
            except Exception as session_update_err:
                # Don't fail job creation if session update fails - log and continue
                add_log(f"FirestoreJobStore: Warning - Failed to update session job_ids (non-critical): {str(session_update_err)}")
            
            add_log(f"FirestoreJobStore: Created job {doc_id}")
            
            created_doc = await self._firestore_service._get_document(self._collection_name, doc_id)
            return self._convert_firestore_to_response(doc_id, created_doc)
        except ValueError:
            raise
        except Exception as e:
            add_log(f"FirestoreJobStore: Error creating job: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_job_by_id(self, job_id: str) -> Optional[JobResponse]:
        """Get job by ID"""
        try:
            doc_id = str(job_id)  # job_id is already a string (UUID)
            doc = await self._firestore_service._get_document(self._collection_name, doc_id)
            if doc:
                return self._convert_firestore_to_response(doc_id, doc)
            return None
        except Exception as e:
            add_log(f"FirestoreJobStore: Error getting job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _update_job(self, job_id: str, job_data: JobUpdate) -> Optional[JobResponse]:
        """Update a job"""
        try:
            doc_id = str(job_id)  # job_id is already a string (UUID)
            existing_doc = await self._firestore_service._get_document(self._collection_name, doc_id)
            if not existing_doc:
                return None
            
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
                update_data['completed_at'] = datetime.now(timezone.utc)  # Use timezone-aware datetime for Firestore
            
            if update_data:
                await self._firestore_service._update_document(self._collection_name, doc_id, update_data)
            
            updated_doc = await self._firestore_service._get_document(self._collection_name, doc_id)
            return self._convert_firestore_to_response(doc_id, updated_doc)
        except Exception as e:
            add_log(f"FirestoreJobStore: Error updating job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        try:
            doc_id = str(job_id)  # job_id is already a string (UUID)
            job_doc = await self._firestore_service._get_document(self._collection_name, doc_id)
            if not job_doc:
                return False
            
            session_id = job_doc.get('session_id')
            if session_id:
                session_id = str(session_id)  # Ensure string
                session_doc_ref = self._firestore_service._client.collection(
                    self._session_collection_name
                ).document(session_id)
                
                session_doc = await asyncio.to_thread(session_doc_ref.get)
                if session_doc.exists:
                    session_data = session_doc.to_dict() or {}
                    existing_job_ids = session_data.get('job_ids', [])
                    if job_id in existing_job_ids:
                        existing_job_ids.remove(job_id)
                        await asyncio.to_thread(session_doc_ref.update, {'job_ids': existing_job_ids})
            
            await self._firestore_service._delete_document(self._collection_name, doc_id)
            add_log(f"FirestoreJobStore: Deleted job {job_id}")
            return True
        except Exception as e:
            add_log(f"FirestoreJobStore: Error deleting job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_all_jobs(self) -> List[JobResponse]:
        """Get all jobs"""
        try:
            jobs_data = await self._firestore_service._get_all_documents(self._collection_name)
            jobs = []
            for doc in jobs_data:
                doc_id = doc.get('id', '')
                jobs.append(self._convert_firestore_to_response(doc_id, doc))
            return jobs
        except Exception as e:
            add_log(f"FirestoreJobStore: Error getting all jobs: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_jobs_by_session_id(self, session_id: str, order_direction: str = 'desc') -> List[JobResponse]:
        """
        Get all jobs for a session, ordered by creation time.
        
        Args:
            session_id: Session ID to get jobs for
            order_direction: 'desc' for latest first (default), 'asc' for oldest first
        """
        try:
            jobs_data = await self._firestore_service._query_collection(
                self._collection_name,
                filters=[('session_id', '==', session_id)],
                order_by='created_on',  # Order by creation time
                order_direction=order_direction  # 'desc' for latest first, 'asc' for oldest first
            )
            jobs = []
            for doc in jobs_data:
                doc_id = doc.get('id', '')
                jobs.append(self._convert_firestore_to_response(doc_id, doc))
            add_log(f"FirestoreJobStore: Found {len(jobs)} jobs for session {session_id} (ordered by created_on {order_direction})")
            return jobs
        except Exception as e:
            add_log(f"FirestoreJobStore: Error getting jobs for session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _update_session_job_ids(self, session_id: str, job_id: str, operation: str) -> None:
        """Update session's job_ids array"""
        try:
            # session_id is now always a string (UUID)
            session_doc_ref = self._firestore_service._client.collection(
                self._session_collection_name
            ).document(session_id)
            
            session_doc = await asyncio.to_thread(session_doc_ref.get)
            if not session_doc.exists:
                return
            
            session_data = session_doc.to_dict() or {}
            existing_job_ids = session_data.get('job_ids', [])
            
            # job_id is already a string (UUID)
            if operation == 'add' and job_id not in existing_job_ids:
                existing_job_ids.append(job_id)
                await asyncio.to_thread(session_doc_ref.update, {'job_ids': existing_job_ids})
            elif operation == 'remove' and job_id in existing_job_ids:
                existing_job_ids.remove(job_id)
                await asyncio.to_thread(session_doc_ref.update, {'job_ids': existing_job_ids})
        except Exception as e:
            add_log(f"FirestoreJobStore: Error updating session job_ids: {str(e)} | traceback: {traceback.format_exc()}")

