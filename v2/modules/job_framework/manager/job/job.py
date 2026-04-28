"""
Job object for JobManager - Simple async job execution framework.

RQ-style pattern:
- Execute functions receive a JobContext with config and cancellation support
- Framework handles lifecycle (status, milestones, errors) automatically
"""
from typing import Optional, Dict, Any, Callable, Awaitable
from datetime import datetime
from v2.common.logger import add_log
from v2.modules.job_framework.models.job_model import JobStatus


class CancellationError(Exception):
    """Exception raised when a job is cancelled"""
    pass


DEFAULT_CANCELLATION_REASON = "Job cancelled"


class JobContext:
    """
    Context object passed to execute functions.
    
    Provides:
    - job_config: Your job configuration
    - execution_metadata: Execution parameters
    - is_cancelled(): Check if job was cancelled
    - check_cancellation(): Raise error if cancelled
    - create_milestone(): Track progress for long-running operations
    
    Usage:
        async def my_function(ctx: JobContext):
            tables = ctx.job_config.get("tables", [])
            
            for i, table in enumerate(tables):
                # Check cancellation before each step
                ctx.check_cancellation()
                
                # Report progress via milestone
                await ctx.create_milestone(
                    f"Fetching {table} ({i+1}/{len(tables)})",
                    data={"table": table, "progress": i+1, "total": len(tables)}
                )
                
                # Do the actual work
                data = await fetch_table(table)
            
            return {"fetched": len(tables)}
    """
    
    def __init__(
        self,
        job_id: str,  # Changed from int
        user_id: str,  # Changed from int to str (UUID string)
        session_id: str,  # Changed from int to str (UUID)
        job_type: str,
        label: str,
        job_config: Dict[str, Any],
        execution_metadata: Dict[str, Any],
        cancellation_checker: Callable[[], bool],
        milestone_creator: Optional[Callable[[str, str, Optional[Dict[str, Any]]], Awaitable[None]]] = None
    ):
        self.job_id = job_id
        self.user_id = user_id
        self.session_id = session_id
        self.job_type = job_type
        self.label = label
        self.job_config = job_config or {}
        self.execution_metadata = execution_metadata or {}
        self._cancellation_checker = cancellation_checker
        self._milestone_creator = milestone_creator
    
    def is_cancelled(self) -> bool:
        """Check if job cancellation has been requested"""
        return self._cancellation_checker()
    
    def check_cancellation(self) -> None:
        """Check cancellation and raise CancellationError if requested"""
        if self._cancellation_checker():
            raise CancellationError("Job cancelled by user")
    
    async def create_milestone(self, description: str, data: Optional[Dict[str, Any]] = None) -> None:
        """
        Create a milestone to track progress.
        
        Milestones are used for:
        - Real-time progress tracking (frontend polling)
        - Resume capability (pick up from last completed milestone)
        - Audit trail (detailed execution history)
        
        Args:
            description: Human-readable description (e.g., "Fetching Orders table (3/10)")
            data: Optional metadata (e.g., {"table": "Orders", "rows": 5000})
        """
        if self._milestone_creator:
            try:
                # Generate unique milestone name from description
                milestone_name = f"milestone_{self.job_id}_{description[:50].replace(' ', '_').lower()}"
                await self._milestone_creator(milestone_name, description, data)
                add_log(f"[JobContext] Milestone created: {description}")
            except Exception as e:
                add_log(f"[JobContext] Error creating milestone: {str(e)}")


class Job:
    """
    Job object that can be executed asynchronously.
    NO storage dependencies - pure business logic object.
    """
    
    def __init__(
        self,
        job_id: str,  # Changed from int
        user_id: str,  # Changed from int to str (UUID string)
        session_id: str,  # Changed from int to str (UUID)
        job_type: str,
        label: str,
        status: JobStatus = JobStatus.PENDING,
        created_on: Optional[datetime] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        error_info: Optional[Dict[str, Optional[str]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        execute_func: Optional[Callable[[], Awaitable[Any]]] = None,
    ) -> None:
        self.job_id: str = job_id  # Changed from int
        self.user_id: str = user_id  # Changed from int to str (UUID string)
        self.session_id: str = session_id  # Changed from int to str (UUID)
        self.job_type: str = job_type
        self.label: str = label
        self.status: JobStatus = status
        self.created_on: datetime = created_on or datetime.now()
        self.started_at: Optional[datetime] = started_at
        self.completed_at: Optional[datetime] = completed_at
        
        if error_info is None:
            error_info = {}
        self.error_message: Optional[str] = error_info.get("error_message")
        self.error_type: Optional[str] = error_info.get("error_type")
        
        if metadata is None:
            metadata = {}
        self.execution_metadata: Dict[str, Any] = metadata.get("execution_metadata", {})
        self.result_metadata: Optional[Dict[str, Any]] = metadata.get("result_metadata")
        self.job_config: Dict[str, Any] = metadata.get("job_config", {})
        
        self._execute_func: Optional[Callable[[], Awaitable[Any]]] = execute_func
        self._cancellation_requested: bool = False
        self._cancellation_reason: Optional[str] = None
        self._milestone_callback: Optional[Callable[[str, str, Optional[Dict[str, Any]]], Awaitable[None]]] = None
    
    def _get_cancellation_reason(self) -> str:
        return self._cancellation_reason or DEFAULT_CANCELLATION_REASON
    
    def _health(self) -> bool:
        add_log(f"[Job] health: job_{self.job_id} is healthy")
        return True
    
    def _request_cancellation(self, reason: Optional[str] = None) -> None:
        self._cancellation_requested = True
        self._cancellation_reason = reason or "Cancelled by user"
        add_log(f"[Job] Cancellation requested for job_{self.job_id}: {self._cancellation_reason}")
    
    def _is_cancellation_requested(self) -> bool:
        return self._cancellation_requested
    
    def _set_milestone_callback(self, callback: Callable[[str, str, Optional[Dict[str, Any]]], Awaitable[None]]) -> None:
        self._milestone_callback = callback
    
    async def _create_milestone(self, name: str, description: str, data: Optional[Dict[str, Any]] = None) -> None:
        if self._milestone_callback:
            try:
                await self._milestone_callback(name, description, data)
            except Exception as e:
                add_log(f"[Job] Error creating milestone '{name}': {str(e)}")
    
    def _create_context(self) -> JobContext:
        return JobContext(
            job_id=self.job_id,
            user_id=self.user_id,
            session_id=self.session_id,
            job_type=self.job_type,
            label=self.label,
            job_config=self.job_config,
            execution_metadata=self.execution_metadata,
            cancellation_checker=self._is_cancellation_requested,
            milestone_creator=self._milestone_callback
        )
    
    async def _execute(self) -> bool:
        """Execute the job asynchronously"""
        if self._cancellation_requested:
            raise CancellationError(f"{self._get_cancellation_reason()} before execution")
        
        self.status = JobStatus.RUNNING
        self.started_at = datetime.now()
        
        await self._create_milestone("job_started", "Job execution started", {"started_at": self.started_at.isoformat()})
        
        ctx = self._create_context()
        
        try:
            if self._execute_func:
                result = await self._execute_with_context(self._execute_func, ctx)
                self.result_metadata = result if isinstance(result, dict) else {"result": result}
            else:
                await self._execute_default_with_checks()
            
            self.status = JobStatus.COMPLETED
            self.completed_at = datetime.now()
            
            await self._create_milestone("job_completed", "Job execution completed successfully", {
                "completed_at": self.completed_at.isoformat(),
                "result": self.result_metadata
            })
            
            add_log(f"[Job] execute: job_{self.job_id} completed successfully")
            return True
            
        except CancellationError as e:
            self.status = JobStatus.CANCELLED
            self.error_message = str(e)
            self.error_type = "CancellationError"
            self.completed_at = datetime.now()
            
            await self._create_milestone("job_cancelled", f"Job cancelled: {str(e)}", {
                "cancelled_at": self.completed_at.isoformat(),
                "reason": self._cancellation_reason
            })
            
            add_log(f"[Job] execute: job_{self.job_id} was cancelled")
            return False
            
        except Exception as e:
            self.status = JobStatus.ERROR
            self.error_message = str(e)
            self.error_type = type(e).__name__
            self.completed_at = datetime.now()
            
            await self._create_milestone("job_failed", f"Job execution failed: {str(e)}", {
                "failed_at": self.completed_at.isoformat(),
                "error_type": self.error_type
            })
            
            add_log(f"[Job] execute: job_{self.job_id} failed with error: {str(e)}")
            return False
    
    async def _execute_with_context(self, execute_func: Callable, ctx: JobContext) -> Any:
        if self._cancellation_requested:
            raise CancellationError(self._get_cancellation_reason())
        
        result = await execute_func(ctx)
        
        if self._cancellation_requested:
            raise CancellationError(self._get_cancellation_reason())
        
        return result
    
    async def _execute_default_with_checks(self) -> None:
        import asyncio
        
        for _ in range(10):
            if self._cancellation_requested:
                raise CancellationError(self._get_cancellation_reason())
            await asyncio.sleep(0.01)
        
        self.result_metadata = {"status": "completed", "job_id": self.job_id}
    
    def _to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "job_type": self.job_type,
            "status": self.status.value,
            "label": self.label,
            "created_on": self.created_on.isoformat() if self.created_on else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "execution_metadata": self.execution_metadata,
            "result_metadata": self.result_metadata,
            "job_config": self.job_config
        }

