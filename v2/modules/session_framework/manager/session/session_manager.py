from typing import List, Optional, Dict, Any
from v2.modules.session_framework.controllers.session.dtos import SessionRequestDTO
from v2.modules.session_framework.services.session.session_service import SessionService
from v2.common.model.sessionModel import SessionModel
from v2.common.model.jobInfo import JobInfo
from v2.common.logger import add_log
import traceback
import asyncio


class SessionManager:
    """Session manager for session operations"""
    def __init__(self, service: Optional[SessionService] = None):
        self.__service = service or SessionService()

    async def _get_all_sessions(self) -> List[SessionModel]:
        """Get all sessions"""
        try:
            add_log("SessionManager: Fetching all sessions")
            result = await self.__service._get_all_sessions()
            add_log(f"SessionManager: Successfully fetched {len(result)} sessions")
            return result
        except Exception as e:
            add_log(f"SessionManager: Error getting all sessions: {str(e)} | traceback: {traceback.format_exc()}")
            raise

    async def _create_session(self, session_data: SessionRequestDTO):
        """Create a new session"""
        try:
            add_log(f"SessionManager: Creating session with data_source: {session_data.dataSource}, label: {session_data.label}")
            result = await self.__service._create_session(session_data)
            add_log(f"SessionManager: Successfully created session: {result.sessionId}")
            return result
        except Exception as e:
            add_log(f"SessionManager: Error creating session: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_sessions_by_user_id(self, user_id: str) -> List[SessionModel]:
        """Get sessions by user ID with enriched job details"""
        try:
            add_log(f"SessionManager: Fetching sessions for user: {user_id}")
            result = await self.__service._get_sessions_by_user_id(user_id)
            # Enrich sessions with job details
            enriched_result = await self._enrich_sessions_with_jobs(result)
            add_log(f"SessionManager: Successfully fetched {len(enriched_result)} sessions for user: {user_id}")
            return enriched_result
        except Exception as e:
            add_log(f"SessionManager: Error getting sessions for user {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _get_session_by_id(self, session_id: str) -> Optional[SessionModel]:
        """Get a session by ID with enriched job details"""
        try:
            add_log(f"SessionManager: Fetching session: {session_id}")
            result = await self.__service._get_session_by_id(session_id)
            if result:
                # Enrich session with job details
                enriched_result = await self._enrich_sessions_with_jobs([result])
                add_log(f"SessionManager: Successfully fetched session: {session_id}")
                return enriched_result[0] if enriched_result else None
            else:
                add_log(f"SessionManager: Session not found: {session_id}")
            return result
        except Exception as e:
            add_log(f"SessionManager: Error getting session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _enrich_sessions_with_jobs(self, sessions: List[SessionModel]) -> List[SessionModel]:
        """
        Enrich sessions with job details by fetching job information for each job ID.
        Uses parallel fetching to improve performance.
        
        Args:
            sessions: List of sessions to enrich
            
        Returns:
            List of sessions with jobs field populated
        """
        try:
            from v2.modules.job_framework.manager.job.job_manager import JobManager
            
            job_manager = JobManager()
            
            # Collect all job fetch tasks with their session context
            job_fetch_tasks = []
            job_context_map = []  # List of (session_index, job_id_str) tuples, same order as tasks
            
            for session_idx, session in enumerate(sessions):
                if session.jobIds:
                    for job_id_str in session.jobIds:
                        try:
                            # job_id is now a UUID string, not an integer
                            job_id = str(job_id_str)  # Ensure it's a string
                            job_fetch_tasks.append(job_manager._get_job_by_id(job_id))
                            job_context_map.append((session_idx, job_id_str))
                        except (ValueError, TypeError) as e:
                            add_log(f"SessionManager: Invalid job ID '{job_id_str}' in session {session.sessionId}: {str(e)}")
                            continue
            
            # Fetch all jobs in parallel
            if job_fetch_tasks:
                job_responses = await asyncio.gather(*job_fetch_tasks, return_exceptions=True)
            else:
                job_responses = []
            
            # Group jobs by session index
            session_jobs = {}  # session_idx -> list of JobInfo
            
            for idx, job_response in enumerate(job_responses):
                session_idx, job_id_str = job_context_map[idx]
                
                if isinstance(job_response, Exception):
                    add_log(f"SessionManager: Error fetching job {job_id_str}: {str(job_response)}")
                    continue
                
                if not job_response:
                    continue
                
                if session_idx not in session_jobs:
                    session_jobs[session_idx] = []
                
                # Convert JobStatus enum to string
                status_str = job_response.status.value if hasattr(job_response.status, 'value') else str(job_response.status)
                
                session_jobs[session_idx].append(JobInfo(
                    job_id=job_response.job_id,
                    job_type=job_response.job_type,
                    status=status_str,
                    label=job_response.label,
                    createdOn=job_response.created_on
                ))
            
            # Create enriched sessions
            enriched_sessions = []
            for session_idx, session in enumerate(sessions):
                jobs = session_jobs.get(session_idx, [])
                
                enriched_session = SessionModel(
                    user=session.user,
                    sessionId=session.sessionId,
                    dataSource=session.dataSource,
                    label=session.label,
                    status=session.status,
                    createdOn=session.createdOn,
                    currentStep=session.currentStep,
                    nextStep=session.nextStep,
                    credentialId=session.credentialId,
                    jobIds=session.jobIds,  # Keep for backward compatibility
                    jobs=jobs,  # New enriched field
                    pseudonymized=getattr(session, 'pseudonymized', False),
                    suggestedQuestions=getattr(session, 'suggestedQuestions', []),
                    suggestedQuestionsSimple=getattr(session, 'suggestedQuestionsSimple', [])
                )
                enriched_sessions.append(enriched_session)
            
            return enriched_sessions
        except Exception as e:
            add_log(f"SessionManager: Error enriching sessions with jobs: {str(e)} | traceback: {traceback.format_exc()}")
            # Return sessions without enrichment if there's an error
            return sessions
    
    async def _update_session(self, session_id: str, session_data: SessionRequestDTO) -> Optional[SessionModel]:
        """Update a session with SessionRequestDTO"""
        try:
            add_log(f"SessionManager: Updating session: {session_id}")
            result = await self.__service._update_session(session_id, session_data)
            if result:
                add_log(f"SessionManager: Successfully updated session: {session_id}")
            else:
                add_log(f"SessionManager: Session not found for update: {session_id}")
            return result
        except Exception as e:
            add_log(f"SessionManager: Error updating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _update_session_fields(self, session_id: str, update_data: Dict[str, Any]) -> Optional[SessionModel]:
        """
        Update session with any fields.
        
        Args:
            session_id: Session identifier
            update_data: Dictionary of field updates (e.g., {"current_step": "2", "next_step": "3"})
            
        Returns:
            Optional[SessionModel]: Updated session model or None if not found
        """
        try:
            add_log(f"SessionManager: Updating session {session_id} with {update_data}")
            result = await self.__service._update_session_fields(session_id, update_data)
            if result:
                add_log(f"SessionManager: Successfully updated session {session_id}")
            return result
        except Exception as e:
            add_log(f"SessionManager: Error updating session: {str(e)}")
            raise
    
    async def _update_session_status(self, session_id: str, status: str) -> bool:
        """
        Update session status.
        
        Args:
            session_id: Session identifier
            status: New status ('initiated', 'in_progress', 'active', 'failed')
            
        Returns:
            bool: True if updated successfully
        """
        try:
            add_log(f"SessionManager: Updating session {session_id} status to '{status}'")
            result = await self.__service._update_session_status(session_id, status)
            if result:
                add_log(f"SessionManager: Successfully updated session {session_id} status to '{status}'")
            else:
                add_log(f"SessionManager: Failed to update session {session_id} status (session not found)")
            return result
        except Exception as e:
            add_log(f"SessionManager: Error updating session status: {str(e)} | traceback: {traceback.format_exc()}")
            return False

    async def _delete_session(self, session_id: str) -> bool:
        """Delete a session"""
        try:
            add_log(f"SessionManager: Deleting session: {session_id}")
            result = await self.__service._delete_session(session_id)
            if not result:
                add_log(f"SessionManager: Failed to delete session: {session_id}")
                return False
            add_log(f"SessionManager: Successfully deleted session: {session_id}")
            return True
        except Exception as e:
            add_log(f"SessionManager: Error deleting session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def check_session_ownership(self, session_id: str, user_email: str) -> bool:
        """
        Check if a user owns a specific session.
        
        Args:
            session_id: Session ID (UUID string)
            user_email: User email address
            
        Returns:
            bool: True if user owns the session, False otherwise
        """
        try:
            # Get session and check if user email matches
            session = await self._get_session_by_id(session_id)
            if not session:
                add_log(f"SessionManager: Session {session_id} not found for ownership check")
                return False
            
            # Check if session's user email matches
            session_user_email = session.user.email.lower() if session.user and session.user.email else ""
            is_owner = session_user_email == user_email.lower()
            
            if not is_owner:
                add_log(f"SessionManager: Ownership check failed - session user: {session_user_email}, requested user: {user_email.lower()}")
            
            return is_owner
        except Exception as e:
            add_log(f"SessionManager: Error checking session ownership: {str(e)} | traceback: {traceback.format_exc()}")
            return False