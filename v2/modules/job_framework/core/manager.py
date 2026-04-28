"""
Job Manager - Core orchestration logic

Usage:
    manager = create_job_manager()
    job = await manager._create_job(JobCreate(...))
    await manager._execute_job(job.job_id, execute_func=my_async_function)
"""
import asyncio
import time
from typing import Any, Dict, List, Optional, Callable, Awaitable
from datetime import datetime
from v2.modules.job_framework.core.interfaces import JobStore, MilestoneStore
from v2.modules.job_framework.models.job_model import (
    JobCreate, JobResponse, JobUpdate, JobStatus,
    MilestoneCreate, MilestoneResponse, MilestoneUpdate
)
from v2.modules.job_framework.manager.job.job import Job, CancellationError
from v2.common.logger import add_log
import traceback

# Import workflow step configurations
try:
    from v2.modules.connector_framework.config.workflow_steps import (
        CONNECTOR_WORKFLOW_STEPS,
        JOB_TYPE_TO_STEP,
        JOB_DEPENDENCIES,
        DATA_LOADING_JOB_TYPES as CONFIG_DATA_LOADING_JOB_TYPES
    )
    # Use config if available, fallback to local definition
    DATA_LOADING_JOB_TYPES = CONFIG_DATA_LOADING_JOB_TYPES
except ImportError:
    # Fallback if config file doesn't exist yet
    DATA_LOADING_JOB_TYPES = {"data_fetch", "file_upload", "copy_sample_data"}
    CONNECTOR_WORKFLOW_STEPS = {}
    JOB_TYPE_TO_STEP = {}
    JOB_DEPENDENCIES = {}

MILESTONE_STORE_NOT_CONFIGURED_MSG = "MilestoneStore not configured"


def _normalize_connector_type(data_source: str) -> str:
    """
    Normalize data_source string to connector type key.
    
    Handles variations like:
    - "Sample Data" -> "sample_data"
    - "File Upload" -> "file_upload"
    - "Microsoft SQL Server" -> "mssql"
    """
    if not data_source:
        return ""
    
    normalized = data_source.lower().strip()
    
    # Map common variations to connector type keys
    mappings = {
        "sample data": "sample_data",
        "file upload": "file_upload",
        "csv/excel": "file_upload",  # File upload connector uses "csv/excel" as dataSource
        "csv": "file_upload",
        "excel": "file_upload",
        "microsoft sql server": "mssql",
        "ms sql": "mssql",
        "sql server": "mssql",
    }
    
    # Check direct mappings first
    if normalized in mappings:
        return mappings[normalized]
    
    # Replace slashes and spaces with underscores
    normalized = normalized.replace("/", "_").replace(" ", "_")
    
    # Check if it matches a connector type
    try:
        from v2.modules.connector_framework.config.workflow_steps import CONNECTOR_WORKFLOW_STEPS
        if normalized in CONNECTOR_WORKFLOW_STEPS:
            return normalized
    except ImportError:
        pass
    
    return normalized


def _get_step_for_job_type(job_type: str, connector_type: str) -> Optional[str]:
    """
    Get the workflow step number for a job type, considering connector-specific logic.
    
    Connector-specific mappings:
    - copy_sample_data for sample_data connector should map to step "2" (after select)
    - domain_dictionary: file_upload/n8n=step 2, salesforce/acumatica/mssql/mysql=step 3
    - domain_save: file_upload=step 3, n8n/salesforce/acumatica/mssql/mysql=step 4
    """
    try:
        from v2.modules.connector_framework.config.workflow_steps import JOB_TYPE_TO_STEP
    except ImportError:
        return None
    
    # Default mapping (works for most cases)
    default_step = JOB_TYPE_TO_STEP.get(job_type)
    
    # Connector-specific overrides
    if connector_type == "sample_data" and job_type == "copy_sample_data":
        # For sample_data, copy happens after selection (step 1), so it's step 2
        return "2"
    
    if connector_type == "file_upload" and job_type == "file_upload":
        # File upload is step 1 for file_upload connector
        return "1"
    
    # For database connectors, data_fetch is step 2 (after connect)
    if connector_type in ["mssql", "mysql"] and job_type == "data_fetch":
        return "2"
    
    # For API connectors, data_fetch is step 2 (after connect)
    if connector_type in ["salesforce", "acumatica"] and job_type == "data_fetch":
        return "2"
    
    # Domain dictionary connector-specific overrides
    # Domain generation step varies by connector type
    if job_type == "domain_dictionary":
        if connector_type in ["file_upload", "n8n"]:
            # file_upload/n8n: Domain generation is step 2 (after upload/fetch)
            return "2"
        elif connector_type in ["salesforce", "acumatica", "mssql", "mysql"]:
            # salesforce/acumatica/mssql/mysql: Domain generation is step 3 (after connect + select/fetch)
            return "3"
        # For other connectors, use default
    
    # Domain save connector-specific overrides
    # Preview & Edit step varies by connector type
    if job_type == "domain_save":
        if connector_type == "file_upload":
            # file_upload: Preview & Edit is step 3
            return "3"
        elif connector_type in ["n8n", "salesforce", "acumatica", "mssql", "mysql"]:
            # n8n, salesforce, acumatica, mssql, mysql: Preview & Edit is step 4
            return "4"
        # For other connectors, use default or return None
    
    # Return default mapping
    return default_step


class JobManager:
    """
    Manager layer for job operations.
    
    RQ-style pattern:
    - You provide the async function to execute
    - Framework handles status, milestones, results, errors
    """
    
    def __init__(
        self,
        job_store: JobStore,
        milestone_store: Optional[MilestoneStore] = None
    ):
        self._job_store = job_store
        self._milestone_store = milestone_store
        self._lock = asyncio.Lock()
        self._jobs: Dict[int, Job] = {}
        add_log("JobManager: Initialized")
    
    async def _create_job(
        self, 
        job_data: JobCreate, 
        execute_func: Optional[Callable[[], Awaitable[Any]]] = None
    ) -> JobResponse:
        """
        Create a new job.
        
        If execute_func is provided, execution starts automatically in the background.
        If not provided, job is created in PENDING status and can be executed later.
        
        Args:
            job_data: Job creation data
            execute_func: Optional async function that accepts JobContext and returns a dict.
                          If provided, execution starts automatically after creation.
            
        Returns:
            JobResponse: The created job
        """
        try:
            add_log(f"JobManager: Creating job of type: {job_data.job_type}")
            result = await self._job_store._create_job(job_data)
            add_log(f"JobManager: Successfully created job: {result.job_id}")
            
            await self._job_store._update_session_job_ids(result.session_id, result.job_id, 'add')
            
            await self._create_job_object(
                job_id=result.job_id,
                job_config={
                    "user_id": result.user_id,
                    "session_id": result.session_id,
                    "job_type": result.job_type,
                    "label": result.label,
                    "status": result.status,
                    "created_on": result.created_on,
                    "started_at": result.started_at,
                    "completed_at": result.completed_at,
                    "error_message": result.error_message,
                    "error_type": result.error_type,
                    "execution_metadata": result.execution_metadata,
                    "result_metadata": result.result_metadata,
                    "job_config": result.job_config,
                }
            )
            
            # If execute_func provided, check dependencies before starting execution
            if execute_func is not None:
                # Check if dependencies are satisfied before starting execution
                dependencies_satisfied = await self._check_job_dependencies(result.job_id, result.job_type, result.session_id)
                
                if dependencies_satisfied:
                    add_log(f"JobManager: Auto-starting execution for job {result.job_id} (dependencies satisfied)")
                    asyncio.create_task(
                        self._execute_job(result.job_id, execute_func=execute_func)
                    )
                else:
                    add_log(f"JobManager: Job {result.job_id} created but waiting for dependencies. Will auto-start when dependencies complete.")
                    # Store execute_func in job_config for later execution
                    if result.job_config is None:
                        result.job_config = {}
                    result.job_config["_execute_func_ready"] = True
                    # Job will be started automatically when dependencies complete (handled in _check_and_start_dependent_jobs)
            
            return result
        except Exception as e:
            add_log(f"JobManager: Error creating job: {str(e)}")
            raise
    
    async def _get_job_by_id(self, job_id: str) -> Optional[JobResponse]:
        """Get a job by ID"""
        try:
            result = await self._job_store._get_job_by_id(job_id)
            return result
        except Exception as e:
            add_log(f"JobManager: Error getting job {job_id}: {str(e)}")
            raise
    
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
        start_time = time.time()
        
        while True:
            job = await self._get_job_by_id(job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            # Check if job reached terminal state
            if job.status in [JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.CANCELLED]:
                add_log(f"JobManager: Job {job_id} reached terminal state: {job.status.value}")
                return job
            
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
            
            # Wait before next poll
            await asyncio.sleep(poll_interval)
    
    async def _update_job(self, job_id: str, job_data: JobUpdate) -> Optional[JobResponse]:
        """Update a job"""
        try:
            result = await self._job_store._update_job(job_id, job_data)
            if result:
                async with self._lock:
                    if job_id in self._jobs:
                        job_obj = self._jobs[job_id]
                        job_obj.status = result.status
                        job_obj.label = result.label or job_obj.label
                        job_obj.error_message = result.error_message
                        job_obj.error_type = result.error_type
                        job_obj.execution_metadata = result.execution_metadata or job_obj.execution_metadata
                        job_obj.result_metadata = result.result_metadata
                        job_obj.job_config = result.job_config or job_obj.job_config
                        job_obj.started_at = result.started_at
                        job_obj.completed_at = result.completed_at
            return result
        except Exception as e:
            add_log(f"JobManager: Error updating job {job_id}: {str(e)}")
            raise
    
    async def _get_all_jobs(self) -> List[JobResponse]:
        """Get all jobs"""
        return await self._job_store._get_all_jobs()
    
    async def _get_jobs_by_session_id(self, session_id: str, order_direction: str = 'desc') -> List[JobResponse]:
        """
        Get all jobs for a session, ordered by creation time.
        
        Args:
            session_id: Session ID to get jobs for
            order_direction: 'desc' for latest first (default), 'asc' for oldest first
        """
        return await self._job_store._get_jobs_by_session_id(session_id, order_direction)
    
    async def _delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        try:
            result = await self._job_store._delete_job(job_id)
            if result:
                job_response = await self._job_store._get_job_by_id(job_id)
                if job_response:
                    await self._job_store._update_session_job_ids(job_response.session_id, job_id, 'remove')
                
                if self._milestone_store:
                    await self._milestone_store._delete_job_milestones(job_id)
                
                async with self._lock:
                    self._jobs.pop(job_id, None)
            return result
        except Exception as e:
            add_log(f"JobManager: Error deleting job {job_id}: {str(e)}")
            raise
    
    def _create_milestone_callback(self, job_id: str) -> Callable[[str, str, Optional[Dict[str, Any]]], Awaitable[None]]:
        """Create milestone callback for automatic milestone creation"""
        async def milestone_callback(name: str, description: str, data: Optional[Dict[str, Any]] = None) -> None:
            try:
                await self._milestone_store._create_milestone(MilestoneCreate(
                    job_id=job_id,
                    name=name,
                    description=description,
                    data=data or {}
                ))
            except Exception as e:
                add_log(f"JobManager: Error creating milestone '{name}' for job {job_id}: {str(e)}")
        
        return milestone_callback
    
    async def _update_job_status_from_object(self, job_id: str) -> None:
        """Update job status in storage from Job object"""
        async with self._lock:
            if job_id not in self._jobs:
                return
            
            job_obj = self._jobs[job_id]
            update_data = JobUpdate(
                status=job_obj.status,
                completed_at=datetime.now()
            )
            
            if job_obj.error_message:
                update_data.error_message = job_obj.error_message
            if job_obj.error_type:
                update_data.error_type = job_obj.error_type
            if job_obj.result_metadata:
                update_data.result_metadata = job_obj.result_metadata
            
            await self._job_store._update_job(job_id, update_data)
    
    async def _handle_execution_error(self, job_id: str, error: Exception) -> None:
        """Handle execution error"""
        error_type = type(error).__name__
        error_message = str(error)
        
        if isinstance(error, CancellationError):
            status = JobStatus.CANCELLED
        else:
            status = JobStatus.ERROR
        
        await self._job_store._update_job(
            job_id,
            JobUpdate(
                status=status,
                completed_at=datetime.now(),
                error_message=error_message,
                error_type=error_type
            )
        )
    
        # Check session status even on error
        await self._check_and_update_session_status(job_id)
    
    async def _check_and_update_session_status(self, job_id: str):
        """
        Check if all data loading jobs for a session are complete and update session status.
        
        Args:
            job_id: ID of the job that just completed
        """
        try:
            add_log(f"JobManager: Checking session status for completed job {job_id}")
            
            # Get the completed job to check its type and session
            job_response = await self._job_store._get_job_by_id(job_id)
            if not job_response:
                add_log(f"JobManager: Job {job_id} not found, skipping session status check")
                return
            
            # Only process data loading jobs
            if job_response.job_type not in DATA_LOADING_JOB_TYPES:
                add_log(f"JobManager: Job {job_id} type '{job_response.job_type}' is not a data loading job, skipping session status check")
                return
            
            add_log(f"JobManager: Processing data loading job {job_id} of type '{job_response.job_type}'")
            
            # Get original session_id from job_config (string ID)
            job_config = job_response.job_config or {}
            session_id_str = job_config.get("_original_session_id")
            if not session_id_str:
                # Fallback to session_id if original not stored (session_id is now a string UUID)
                session_id_str = str(job_response.session_id)
            
            add_log(f"JobManager: Checking jobs for session {session_id_str}")
            
            # Get all jobs for this session (session_id is now a string UUID)
            session_jobs = await self._job_store._get_jobs_by_session_id(job_response.session_id)
            
            add_log(f"JobManager: Found {len(session_jobs)} total jobs for session {session_id_str}")
            
            # Filter to only data loading jobs
            data_loading_jobs = [
                job for job in session_jobs 
                if job.job_type in DATA_LOADING_JOB_TYPES
            ]
            
            job_statuses = [f'{j.job_id}({j.status.value})' for j in data_loading_jobs]
            add_log(f"JobManager: Found {len(data_loading_jobs)} data loading jobs: {job_statuses}")
            
            # Check if all data loading jobs are complete
            all_complete = all(
                job.status in [JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.CANCELLED]
                for job in data_loading_jobs
            )
            
            add_log(f"JobManager: All data loading jobs complete: {all_complete}")
            
            if all_complete:
                # Check if any data loading job failed
                any_failed = any(
                    job.status == JobStatus.ERROR or job.status == JobStatus.CANCELLED
                    for job in data_loading_jobs
                )
                
                if any_failed:
                    # If any data loading job failed, set status to "failed"
                    final_status = "failed"
                    add_log(f"JobManager: Updating session {session_id_str} status to 'failed' (data loading job failed)")
                    
                    try:
                        from v2.modules.session_framework.manager.session.session_manager import SessionManager
                        session_manager = SessionManager()
                        result = await session_manager._update_session_status(session_id_str, final_status)
                        if result:
                            add_log(f"JobManager: Successfully updated session {session_id_str} status to 'failed'")
                        else:
                            add_log(f"JobManager: Failed to update session {session_id_str} status (session not found)")
                    except Exception as status_error:
                        add_log(f"JobManager: Error updating session status: {str(status_error)} | traceback: {traceback.format_exc()}")
                else:
                    # Data loading succeeded, but workflow may not be complete yet
                    # Status will be updated to "active" only when workflow reaches final step
                    # (handled in _update_session_step when next_step is None)
                    add_log(f"JobManager: Data loading jobs completed successfully. Session remains 'in_progress' until workflow completes.")
            else:
                add_log(f"JobManager: Not all data loading jobs are complete yet, keeping session status as 'in_progress'")
        except Exception as e:
            add_log(f"JobManager: Error checking session status: {str(e)} | traceback: {traceback.format_exc()}")
    
    async def _execute_job(self, job_id: str, execute_func: Callable[[], Awaitable[Any]] = None) -> bool:
        """
        Execute a job with the provided async function.
        
        Args:
            job_id: ID of the job to execute
            execute_func: Async function that accepts JobContext and returns a dict
            
        Returns:
            bool: True if execution succeeds, False otherwise
            
        Example:
            async def fetch_data(ctx: JobContext):
                query = ctx.execution_metadata.get("query")
                data = await db.query(query)
                return {"rows": len(data), "data": data}
            
            success = await manager._execute_job(job.job_id, execute_func=fetch_data)
        """
        try:
            job_response = await self._job_store._get_job_by_id(job_id)
            if not job_response:
                raise ValueError(f"Job {job_id} not found")
            
            if job_response.status == JobStatus.CANCELLED:
                return False
            
            if job_response.status in [JobStatus.COMPLETED, JobStatus.ERROR]:
                return False
            
            add_log(f"JobManager: Executing job {job_id}")
            
            async with self._lock:
                job_obj = await self._get_job_object_internal(job_id)
            
            if self._milestone_store:
                job_obj._set_milestone_callback(self._create_milestone_callback(job_id))
            
            job_obj._execute_func = execute_func
            
            success = await job_obj._execute()
            
            await self._update_job_status_from_object(job_id)
            
            # Check and update session status if this is a data loading job
            await self._check_and_update_session_status(job_id)
            
            # Check and start dependent jobs that were waiting
            await self._check_and_start_dependent_jobs(job_id)
            
            # Update session step based on job completion
            await self._update_session_step(job_id)
            
            add_log(f"JobManager: Job {job_id} execution {'succeeded' if success else 'failed'}")
            return success
            
        except CancellationError as e:
            add_log(f"JobManager: Job {job_id} was cancelled: {str(e)}")
            await self._handle_execution_error(job_id, e)
            return False
        except Exception as e:
            add_log(f"JobManager: Error executing job {job_id}: {str(e)}")
            await self._handle_execution_error(job_id, e)
            raise
    
    async def _cancel_job(self, job_id: str, reason: Optional[str] = None) -> bool:
        """Cancel a running job"""
        try:
            job_response = await self._job_store._get_job_by_id(job_id)
            if not job_response:
                return False
            
            if job_response.status not in [JobStatus.PENDING, JobStatus.INITIALIZED, JobStatus.RUNNING]:
                return False
            
            async with self._lock:
                job_obj = await self._get_job_object_internal(job_id)
                job_obj._request_cancellation(reason)
                
                # For analysis jobs, clean up Cloud Run service if deployed
                if job_response.job_type == "analysis":
                    try:
                        job_config = job_response.job_config or {}
                        deployed_service_name = job_config.get("_deployed_service_name")
                        original_job_id_str = job_config.get("_original_job_id_str", f"JOB_{job_id}")
                        
                        if deployed_service_name:
                            add_log(f"JobManager: Cleaning up Cloud Run service {deployed_service_name} for cancelled analysis job {job_id}")
                            try:
                                from v2.modules.session_framework.services.session.session_service import SessionService
                                session_service = SessionService()
                                session_service.delete_job_service(
                                    service_name=deployed_service_name,
                                    job_id=original_job_id_str
                                )
                                add_log(f"JobManager: Successfully cleaned up Cloud Run service {deployed_service_name}")
                            except Exception as cleanup_err:
                                add_log(f"JobManager: Warning - Failed to cleanup Cloud Run service: {str(cleanup_err)}")
                    except Exception as analysis_cleanup_err:
                        add_log(f"JobManager: Error during analysis job cleanup: {str(analysis_cleanup_err)}")
                
                if job_response.status in [JobStatus.PENDING, JobStatus.INITIALIZED]:
                    await self._job_store._update_job(
                        job_id,
                        JobUpdate(
                            status=JobStatus.CANCELLED,
                            completed_at=datetime.now(),
                            error_message=reason or "Cancelled by user",
                            error_type="CancellationError"
                        )
                    )
                    
                    if self._milestone_store:
                        try:
                            await self._save_milestone(MilestoneCreate(
                                job_id=job_id,
                                name="job_cancelled",
                                description=reason or "Job cancelled by user",
                                data={"cancelled_at": datetime.now().isoformat()}
                            ))
                        except Exception:
                            pass
            
            return True
            
        except Exception as e:
            add_log(f"JobManager: Error cancelling job {job_id}: {str(e)}")
            raise
    
    async def _save_milestone(self, milestone_data: MilestoneCreate) -> MilestoneResponse:
        """Save a new milestone (internal use)"""
        if not self._milestone_store:
            raise ValueError(MILESTONE_STORE_NOT_CONFIGURED_MSG)
        return await self._milestone_store._create_milestone(milestone_data)
    
    async def _update_milestone(self, milestone_id: str, job_id: str, milestone_data: MilestoneUpdate) -> Optional[MilestoneResponse]:
        """Update a milestone"""
        if not self._milestone_store:
            raise ValueError(MILESTONE_STORE_NOT_CONFIGURED_MSG)
        return await self._milestone_store._update_milestone(milestone_id, job_id, milestone_data)
    
    async def _check_milestone(self, milestone_id: str, job_id: str) -> Optional[MilestoneResponse]:
        """Check milestone status"""
        if not self._milestone_store:
            raise ValueError(MILESTONE_STORE_NOT_CONFIGURED_MSG)
        return await self._milestone_store._get_milestone(milestone_id, job_id)
    
    async def _get_job_milestones(self, job_id: str) -> List[MilestoneResponse]:
        """Get all milestones for a job"""
        if not self._milestone_store:
            raise ValueError(MILESTONE_STORE_NOT_CONFIGURED_MSG)
        return await self._milestone_store._get_job_milestones(job_id)
    
    async def _get_job_data_from_store(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job data from storage"""
        try:
            job_response = await self._job_store._get_job_by_id(job_id)
            if job_response:
                return {
                    "user_id": job_response.user_id,
                    "session_id": job_response.session_id,
                    "job_type": job_response.job_type,
                    "label": job_response.label,
                    "status": job_response.status,
                    "created_on": job_response.created_on,
                    "started_at": job_response.started_at,
                    "completed_at": job_response.completed_at,
                    "error_message": job_response.error_message,
                    "error_type": job_response.error_type,
                    "execution_metadata": job_response.execution_metadata,
                    "result_metadata": job_response.result_metadata,
                    "job_config": job_response.job_config,
                }
            return None
        except Exception:
            return None
    
    async def _create_job_object(self, job_id: str, job_config: Optional[Dict[str, Any]] = None) -> Job:
        """Create a new Job object"""
        async with self._lock:
            if job_config is None:
                job_config = await self._get_job_data_from_store(job_id) or {}
            
            job = self._create_job_from_config(job_id, job_config)
            self._jobs[job_id] = job
            return job
    
    def _create_job_from_config(self, job_id: str, job_config: dict) -> Job:
        """Create Job object from config dict"""
        return Job(
                job_id=job_id,
                user_id=job_config.get("user_id") or job_config.get("_original_user_id") or "",  # user_id is now a string (UUID)
                session_id=job_config.get("session_id", ""),  # session_id is now a string (UUID)
                job_type=job_config.get("job_type", ""),
                label=job_config.get("label", ""),
                status=job_config.get("status", JobStatus.PENDING),
                created_on=job_config.get("created_on"),
                started_at=job_config.get("started_at"),
                completed_at=job_config.get("completed_at"),
                error_info={
                    "error_message": job_config.get("error_message"),
                    "error_type": job_config.get("error_type"),
                },
                metadata={
                    "execution_metadata": job_config.get("execution_metadata", {}),
                    "result_metadata": job_config.get("result_metadata"),
                    "job_config": job_config.get("job_config", {}),
                },
            )
    
    async def _get_job_object_internal(self, job_id: str) -> Job:
        """Internal method to get or create job object (assumes lock is held)"""
        job = self._jobs.get(job_id)
        if job is None:
            job_config = await self._get_job_data_from_store(job_id)
            if not job_config:
                raise ValueError(f"Job {job_id} not found")
            
            job = self._create_job_from_config(job_id, job_config)
            self._jobs[job_id] = job
        
        if job.job_id is None or not job._health():
            job_config = await self._get_job_data_from_store(job_id)
            if not job_config:
                raise ValueError(f"Job {job_id} not found")
            
            job = self._create_job_from_config(job_id, job_config)
            self._jobs[job_id] = job
        
        return job
    
    async def _get_job_object(self, job_id: str) -> Job:
        """Fetch existing Job object; recreate if not present"""
        async with self._lock:
            return await self._get_job_object_internal(job_id)
    
    async def _check_job_dependencies(self, job_id: str, job_type: str, session_id: str) -> bool:
        """
        Check if all dependencies for a job are satisfied.
        
        Args:
            job_id: ID of the job to check
            job_type: Type of the job
            session_id: Session ID (UUID string)
            
        Returns:
            bool: True if all dependencies are satisfied, False otherwise
        """
        try:
            # Check if this job type has dependencies
            required_job_types = JOB_DEPENDENCIES.get(job_type, [])
            if not required_job_types:
                add_log(f"JobManager: Job type '{job_type}' has no dependencies, proceeding")
                return True
            
            add_log(f"JobManager: Checking dependencies for job {job_id} (type: {job_type}). Required: {required_job_types}")
            
            # Get all jobs for this session
            session_jobs = await self._job_store._get_jobs_by_session_id(session_id)
            
            # Check if at least ONE of the required job types has completed successfully
            # (Domain generation needs ANY data loading job, not ALL of them)
            satisfied_deps = []
            for required_type in required_job_types:
                # Find jobs of this type for this session
                matching_jobs = [j for j in session_jobs if j.job_type == required_type]
                
                if matching_jobs:
                    # Check if at least one has completed successfully
                    has_completed = any(
                        j.status == JobStatus.COMPLETED 
                        for j in matching_jobs
                    )
                    
                    if has_completed:
                        satisfied_deps.append(required_type)
                        add_log(f"JobManager: Dependency '{required_type}' satisfied")
                    else:
                        # Check statuses for logging
                        statuses = [f"{j.job_id}({j.status.value})" for j in matching_jobs]
                        add_log(f"JobManager: Dependency '{required_type}' not satisfied yet. Jobs: {statuses}")
            
            # If at least one dependency is satisfied, proceed
            if satisfied_deps:
                add_log(f"JobManager: Dependencies satisfied for job {job_id} (found: {satisfied_deps})")
                return True
            else:
                add_log(f"JobManager: No dependencies satisfied for job {job_id}. Required: {required_job_types}")
                return False
            
        except Exception as e:
            add_log(f"JobManager: Error checking dependencies for job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
            # On error, allow execution (fail-safe)
            return True
    
    async def _check_and_start_dependent_jobs(self, completed_job_id: str):
        """
        Check if any pending jobs are waiting for the completed job and start them.
        
        Args:
            completed_job_id: ID of the job that just completed
        """
        try:
            # Get the completed job
            completed_job = await self._job_store._get_job_by_id(completed_job_id)
            if not completed_job:
                return
            
            # Only check if completed job succeeded
            if completed_job.status != JobStatus.COMPLETED:
                return
            
            add_log(f"JobManager: Checking for dependent jobs waiting on job {completed_job_id} (type: {completed_job.job_type})")
            
            # Get all jobs for this session
            session_jobs = await self._job_store._get_jobs_by_session_id(completed_job.session_id)
            
            # Find jobs that depend on the completed job type
            for job in session_jobs:
                # Skip if already running or completed
                if job.status in [JobStatus.RUNNING, JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.CANCELLED]:
                    continue
                
                # Check if this job depends on the completed job type
                required_types = JOB_DEPENDENCIES.get(job.job_type, [])
                if completed_job.job_type not in required_types:
                    continue
                
                # Check if all dependencies are now satisfied
                dependencies_satisfied = await self._check_job_dependencies(job.job_id, job.job_type, job.session_id)
                
                if dependencies_satisfied:
                    add_log(f"JobManager: Dependent job {job.job_id} (type: {job.job_type}) dependencies now satisfied")
                    # Note: We can't directly execute here as execute_func is not stored
                    # The job will need to be manually started or we need a job registry
                    # For now, we log that dependencies are satisfied
            
        except Exception as e:
            add_log(f"JobManager: Error checking dependent jobs: {str(e)} | traceback: {traceback.format_exc()}")
    
    async def _update_session_step(self, job_id: str):
        """
        Update session current_step and next_step based on job completion.
        
        Logic:
        - Get job type and map to workflow step
        - Get session's data_source to determine connector workflow
        - Update current_step to the step this job represents
        - Update next_step based on workflow definition
        """
        try:
            job_response = await self._job_store._get_job_by_id(job_id)
            if not job_response:
                add_log(f"JobManager: Job {job_id} not found for step update")
                return
            
            # Only update steps for completed jobs
            if job_response.status != JobStatus.COMPLETED:
                add_log(f"JobManager: Job {job_id} status is '{job_response.status.value}', skipping step update")
                return
            
            # Get session info
            job_config = job_response.job_config or {}
            session_id_str = job_config.get("_original_session_id") or str(job_response.session_id)
            
            # Get session to determine connector type
            from v2.modules.session_framework.manager.session.session_manager import SessionManager
            session_manager = SessionManager()
            session = await session_manager._get_session_by_id(session_id_str)
            
            if not session:
                add_log(f"JobManager: Session {session_id_str} not found for step update")
                return
            
            # Get connector type from session's data_source and normalize it
            data_source = (session.dataSource or "").strip()
            connector_type = _normalize_connector_type(data_source)
            if not connector_type or connector_type not in CONNECTOR_WORKFLOW_STEPS:
                add_log(f"JobManager: Unknown connector type '{data_source}' (normalized: '{connector_type}') for step update (session: {session_id_str})")
                return
            
            # Get workflow steps for this connector
            workflow = CONNECTOR_WORKFLOW_STEPS[connector_type]
            
            # Map job type to step number (dynamic based on connector type)
            target_step = _get_step_for_job_type(job_response.job_type, connector_type)
            if not target_step:
                add_log(f"JobManager: No step mapping for job type '{job_response.job_type}' with connector '{connector_type}', skipping step update")
                return
            
            # Determine next step from the job's step (where we should advance to)
            step_info = workflow.get(target_step)
            next_step_from_job = step_info.get("next") if step_info else None
            
            # When a job completes, we advance to the step the job represents
            # The job's step represents what step the job belongs to
            # When it completes, current_step becomes that step, and next_step becomes the step after it
            # If the next step is the final step (has no next), advance to it immediately
            if next_step_from_job:
                # Check if the next step is the final step (has no next step itself)
                next_step_info = workflow.get(next_step_from_job)
                next_step_has_next = next_step_info.get("next") if next_step_info else None
                
                if next_step_has_next is None:
                    # Next step is the final step - advance to it immediately
                    new_current_step = next_step_from_job
                    new_next_step = None
                else:
                    # Job completed - current_step becomes the job's step
                    new_current_step = target_step
                    # Next step is the step after the job's step
                    new_next_step = next_step_from_job
            else:
                # Job's step is the final step, stay there
                new_current_step = target_step
                new_next_step = None
            
            # Update session steps and status
            # Status becomes "active" only when workflow reaches final step (new_next_step is None)
            # Otherwise, keep it as "in_progress"
            update_data = {
                "current_step": new_current_step,
                "next_step": new_next_step  # Set to None for final step, not current_step
            }
            
            if new_next_step is None:
                # This is the final step - workflow is complete
                update_data["status"] = "active"
                add_log(f"JobManager: Workflow complete for session {session_id_str} (final step: {new_current_step}). Setting status to 'active'.")
            else:
                # Workflow still in progress - ensure status is "in_progress"
                update_data["status"] = "in_progress"
                add_log(f"JobManager: Workflow in progress for session {session_id_str} (job step: {target_step}, advanced to: {new_current_step}, next: {new_next_step}). Keeping status as 'in_progress'.")
            
            # Update session steps in Firestore
            updated_session = await session_manager._update_session_fields(session_id_str, update_data)
            
            if updated_session:
                # Verify the update succeeded by checking the returned session
                if updated_session.currentStep == new_current_step and updated_session.nextStep == new_next_step:
                    add_log(f"JobManager: Successfully updated session {session_id_str} to step {new_current_step} (next: {new_next_step}) for connector '{connector_type}'")
                else:
                    add_log(f"JobManager: WARNING - Step update may have failed for session {session_id_str}. Expected step {new_current_step}, got {updated_session.currentStep}")
            else:
                add_log(f"JobManager: ERROR - Failed to update session {session_id_str} steps (update returned None)")
            
        except Exception as e:
            add_log(f"JobManager: ERROR updating session step: {str(e)} | traceback: {traceback.format_exc()}")
            # Don't raise - allow job execution to complete even if step update fails

