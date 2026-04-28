"""
Job Framework - Universal Async Job Execution Framework

=============================================================================
CORE PRINCIPLE: The framework is completely function-agnostic.
It can execute ANY async function. It doesn't know or care what the function does.
=============================================================================

Usage:
    from v2.modules.job_framework import create_job_manager, JobCreate, JobContext
    
    # 1. Create manager
    manager = create_job_manager()
    
    # 2. Create job
    job = await manager._create_job(JobCreate(
        user_id=1,
        session_id=123,
        job_type="your_custom_type",  # Informational only - not enforced
        label="My Custom Job",
        job_config={"param1": "value1"}  # Your parameters
    ))
    
    # 3. Define YOUR async function (framework knows nothing about this)
    async def my_custom_function(ctx: JobContext):
        # Check cancellation between steps
        ctx.check_cancellation()
        
        # Report progress via milestones
        await ctx.create_milestone("Step 1 complete", data={"progress": 50})
        
        # Access your config
        param1 = ctx.job_config.get("param1")
        
        # Do your work...
        result = await do_something(param1)
        
        # Return result (becomes job.result_metadata)
        return {"status": "success", "data": result}
    
    # 4. Execute with YOUR function
    success = await manager._execute_job(job.job_id, execute_func=my_custom_function)

The framework provides:
    - Job lifecycle management (PENDING → RUNNING → COMPLETED/ERROR/CANCELLED)
    - Milestone tracking for progress reporting
    - Cancellation support
    - Result storage
    - Error handling

You provide:
    - The async function to execute
    - Whatever business logic that function contains
"""
from v2.modules.job_framework.core.factory import create_job_manager
from v2.modules.job_framework.models.job_model import JobCreate, JobUpdate, JobResponse, JobStatus
from v2.modules.job_framework.manager.job.job import JobContext, Job, CancellationError

__all__ = [
    # Factory
    "create_job_manager",
    
    # Models
    "JobCreate",
    "JobUpdate", 
    "JobResponse",
    "JobStatus",
    
    # Job execution
    "JobContext",  # Passed to your execute function
    "Job",
    "CancellationError",
]
