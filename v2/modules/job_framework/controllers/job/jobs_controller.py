"""
Jobs controller for FastAPI v2

Provides REST API endpoints for job management.

ARCHITECTURE:
- Job creation happens INTERNALLY by business logic (connectors, analysis, QnA)
- Frontend reads status, milestones; can CANCEL or DELETE jobs
- All job creation is handled by backend modules calling:
    await job_manager._create_job(job_data, execute_func=handler)

This controller handles:
- Getting job status (polling)
- Getting milestones (progress tracking)
- Cancelling jobs (user-initiated)
- Deleting jobs (removes all job data: job record, milestones, session reference)
- Listing jobs by session (dashboard/history view) with UI-friendly computed fields
- Creating milestones (internal: execution layer calls POST /internal/milestones)
"""
from typing import Optional, Any, Dict
from fastapi import APIRouter, HTTPException, Depends, Request, Header
from pydantic import BaseModel, Field
from v2.common.model.api_response import ApiResponse
from v2.common.helper import to_dict
from v2.common.logger import add_log
from v2.modules.job_framework.manager.job.job_manager import JobManager
from v2.modules.job_framework.models.job_model import MilestoneCreate
from v2.modules.job_framework.utils.job_ui_helper import enrich_job_for_ui
import os

router = APIRouter(prefix="/jobs")
internal_router = APIRouter(prefix="/internal", tags=["Internal"])

# Constants
JOB_NOT_FOUND_MSG = "Job not found"


def get_job_manager() -> JobManager:
    """Dependency to get job manager instance"""
    return JobManager()


class CreateMilestoneRequest(BaseModel):
    """Request body for execution layer to create a milestone"""
    job_id: str = Field(..., description="Job ID (Firestore doc ID or JOB_<id>)")
    name: str = Field(..., description="Milestone name (e.g. eda_plan_created)")
    description: str = Field(..., description="Human-readable description for reports")
    data: Optional[Dict[str, Any]] = Field(default=None, description="Optional metadata")


@internal_router.post("/milestones", response_model=ApiResponse)
async def create_milestone_internal(
    req: CreateMilestoneRequest,
    request: Request,
    x_execution_layer_secret: Optional[str] = Header(default=None, alias="X-Execution-Layer-Secret"),
    manager: JobManager = Depends(get_job_manager),
):
    """
    Create a milestone for a job. Used by execution layer to feed granular progress.

    When EXECUTION_LAYER_SECRET is set, requires X-Execution-Layer-Secret header to match.
    When not set, accepts all requests (convenient for local/dev).
    Job ID can be "JOB_<uuid>" or raw Firestore doc ID.
    """
    expected_secret = os.getenv("EXECUTION_LAYER_SECRET", "").strip()
    if expected_secret and x_execution_layer_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Execution-Layer-Secret")

    job_id = req.job_id.strip()
    if job_id.startswith("JOB_"):
        job_id = job_id[4:]

    job = await manager._get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    try:
        milestone_data = MilestoneCreate(
            job_id=job_id,
            name=req.name,
            description=req.description,
            data=req.data,
        )
        created = await manager._save_milestone(milestone_data)
        add_log(f"JobsController: Created milestone from execution layer: {req.description[:60]}...")
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Milestone created",
            data={"milestone_id": created.milestone_id},
        )
    except Exception as e:
        add_log(f"JobsController: Error creating milestone: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}/latest-milestone", response_model=ApiResponse)
async def get_latest_milestone_and_status(
    job_id: str,
    manager: JobManager = Depends(get_job_manager)
):
    """
    Get the latest milestone and current status of a job.
    
    This endpoint is optimized for polling job progress. It returns:
    - Latest milestone (most recent progress update)
    - Current job status (pending, running, completed, error, cancelled)
    - Basic job information
    
    Use this endpoint to efficiently track job progress without fetching all milestones.
    Works for jobs in any state (pending, running, completed, error, cancelled).
    
    Args:
        job_id: Job ID to fetch latest milestone and status for
    
    Returns:
        ApiResponse with:
        - latest_milestone: Latest milestone object (or null if no milestones exist)
        - job_status: Current job status
        - job_id: Job ID
        - job_type: Type of job
        - label: Job label
    """
    try:
        # Get job to verify it exists and get status
        job = await manager._get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
        
        # Get all milestones for the job
        milestones = []
        latest_milestone = None
        try:
            milestones = await manager._get_job_milestones(job_id)
            # Milestones are ordered by created_at (ascending), so latest is the last one
            if milestones and len(milestones) > 0:
                latest = milestones[-1]
                latest_milestone = {
                    "milestone_id": latest.milestone_id,
                    "name": latest.name,
                    "description": latest.description,
                    "job_status": latest.job_status if hasattr(latest, 'job_status') else None,
                    "created_at": latest.created_at.isoformat() if latest.created_at else None,
                    "updated_at": latest.updated_at.isoformat() if latest.updated_at else None,
                    "data": latest.data if latest.data else None
                }
        except Exception as e:
            # If milestone fetching fails, continue without milestones (job might not have any yet)
            add_log(f"JobsController: Could not fetch milestones for job {job_id}: {str(e)}")
        
        # Prepare response data
        response_data = {
            "job_id": job.job_id,
            "job_type": job.job_type,
            "label": job.label,
            "status": job.status.value if hasattr(job.status, 'value') else str(job.status),
            "latest_milestone": latest_milestone,
            "milestone_count": len(milestones),
            "created_on": job.created_on.isoformat() if job.created_on else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "error_message": job.error_message
        }
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Latest milestone and job status fetched successfully",
            data=response_data
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}/milestones", response_model=ApiResponse)
async def get_job_milestones(job_id: str, manager: JobManager = Depends(get_job_manager)):  # Changed int to str
    """
    Get all milestones for a job.
    
    Milestones track progress of long-running jobs.
    Your execute function creates milestones via ctx.create_milestone().
    
    Poll this endpoint for detailed progress.
    """
    try:
        job = await manager._get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
        
        milestones = await manager._get_job_milestones(job_id)
        milestones_data = to_dict(milestones)
        return ApiResponse(
            status="success",
            statusCode=200,
            message=f"Fetched {len(milestones)} milestones",
            data=milestones_data
        )
    except HTTPException:
        raise
    except ValueError as e:
        # MilestoneStore not configured
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Milestone tracking not configured",
            data=[]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{job_id}", response_model=ApiResponse)
async def delete_job(
    job_id: str,
    manager: JobManager = Depends(get_job_manager)
):
    """
    Delete a job and all its data (job record, milestones).
    
    Permanently removes the job from storage and updates the session's job list.
    Use with caution - this operation cannot be undone.
    """
    try:
        job = await manager._get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
        
        success = await manager._delete_job(job_id)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to delete job")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Job deleted successfully",
            data={"job_id": job_id}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{job_id}/cancel", response_model=ApiResponse)
async def cancel_job(
    job_id: str,  # Changed int to str
    reason: Optional[str] = None,
    manager: JobManager = Depends(get_job_manager)
):
    """
    Cancel a running job.
    
    Sets cancellation flag. Your execute function should check
    ctx.check_cancellation() periodically to honor this.
    """
    try:
        success = await manager._cancel_job(job_id, reason)
        if not success:
            raise HTTPException(
                status_code=400,
                detail="Job cannot be cancelled (not found or already completed/failed/cancelled)"
            )
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Job cancellation requested successfully",
            data={"job_id": job_id, "status": "cancellation_requested"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}", response_model=ApiResponse)
async def get_job(
    job_id: str,
    include_milestones: bool = False,
    manager: JobManager = Depends(get_job_manager)
):
    """
    Get a specific job by ID with UI-friendly computed fields.
    
    Use this to poll job status during execution.
    Returns enriched job data with computed fields for UI consumption.
    
    Args:
        job_id: Job ID to fetch
        include_milestones: If True, includes milestones for progress tracking (default: False)
    """
    try:
        job = await manager._get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
        
        # Fetch milestones if requested
        milestones = None
        if include_milestones:
            try:
                milestones = await manager._get_job_milestones(job_id)
            except Exception:
                milestones = []
        
        # Enrich job with UI-friendly computed fields
        enriched_job = enrich_job_for_ui(job, milestones)
        
        # Add original string IDs from job_config for reverse mapping
        if job.job_config:
            if '_original_user_id' in job.job_config:
                enriched_job['original_user_id'] = job.job_config['_original_user_id']
            if '_original_session_id' in job.job_config:
                enriched_job['original_session_id'] = job.job_config['_original_session_id']
        
        # Include milestones if requested
        if include_milestones and milestones:
            enriched_job['milestones'] = to_dict(milestones)
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Job fetched successfully (enriched with UI fields)",
            data=enriched_job
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/session/{session_id}", response_model=ApiResponse)
async def get_jobs_by_session(
    session_id: str,
    include_milestones: bool = False,
    manager: JobManager = Depends(get_job_manager)
):
    """
    Get all jobs for a specific session, ordered by latest first.
    
    Returns jobs with complete information and UI-friendly computed fields:
    - Duration calculations (seconds and human-readable format)
    - Status display info (human-readable name and color)
    - Progress percentage (based on milestones)
    - UI-friendly metadata organization
    - Computed flags (is_running, is_completed, can_cancel, etc.)
    
    Args:
        session_id: Session ID to get jobs for
        include_milestones: If True, includes milestones for each job (default: False)
    
    Returns:
        List of jobs ordered by creation time (latest first), enriched with UI-friendly computed fields.
        If include_milestones=True, each job will have a 'milestones' field.
    """
    try:
        # Get jobs ordered by latest first (descending order)
        jobs = await manager._get_jobs_by_session_id(session_id, order_direction='desc')
        
        # Enrich each job with UI-friendly computed fields
        enriched_jobs = []
        for index, job in enumerate(jobs):
            # Fetch milestones if requested
            milestones = None
            if include_milestones:
                try:
                    milestones = await manager._get_job_milestones(job.job_id)
                except Exception:
                    milestones = []
            
            # Enrich job with UI-friendly fields
            enriched_job = enrich_job_for_ui(job, milestones, index=index)
            
            # Add original string IDs from job_config for reverse mapping
            if job.job_config:
                if '_original_user_id' in job.job_config:
                    enriched_job['original_user_id'] = job.job_config['_original_user_id']
                if '_original_session_id' in job.job_config:
                    enriched_job['original_session_id'] = job.job_config['_original_session_id']
            
            # Include milestones if requested
            if include_milestones and milestones:
                enriched_job['milestones'] = to_dict(milestones)
            
            enriched_jobs.append(enriched_job)
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message=f"Fetched {len(enriched_jobs)} jobs for session (ordered by latest first, enriched with UI fields)",
            data=enriched_jobs
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
