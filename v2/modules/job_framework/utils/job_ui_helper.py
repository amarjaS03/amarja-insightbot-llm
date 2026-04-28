"""
UI Helper utilities for job framework
Provides computed fields and UI-friendly data organization for frontend consumption
"""
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from v2.modules.job_framework.models.job_model import JobResponse, JobStatus
from v2.modules.job_framework.models.job_model import MilestoneResponse


def format_duration(seconds: Optional[float]) -> Optional[str]:
    """
    Format duration in seconds to human-readable string.
    
    Examples:
        - 65 -> "1m 5s"
        - 3665 -> "1h 1m 5s"
        - 30 -> "30s"
    """
    if seconds is None or seconds < 0:
        return None
    
    if seconds < 60:
        return f"{int(seconds)}s"
    
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    
    if minutes < 60:
        if remaining_seconds > 0:
            return f"{minutes}m {remaining_seconds}s"
        return f"{minutes}m"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    
    parts = [f"{hours}h"]
    if remaining_minutes > 0:
        parts.append(f"{remaining_minutes}m")
    if remaining_seconds > 0:
        parts.append(f"{remaining_seconds}s")
    
    return " ".join(parts)


def get_status_display_info(status: JobStatus) -> Dict[str, str]:
    """
    Get UI-friendly status display information.
    
    Returns:
        Dict with 'display' (human-readable name) and 'color' (UI color indicator)
    """
    status_config = {
        JobStatus.COMPLETED: {"display": "Completed", "color": "success"},
        JobStatus.RUNNING: {"display": "Running", "color": "info"},
        JobStatus.PENDING: {"display": "Pending", "color": "warning"},
        JobStatus.INITIALIZED: {"display": "Initialized", "color": "info"},
        JobStatus.ERROR: {"display": "Failed", "color": "error"},
        JobStatus.CANCELLED: {"display": "Cancelled", "color": "secondary"},
    }
    
    return status_config.get(status, {"display": status.value.title(), "color": "default"})


def get_job_type_display(job_type: str) -> str:
    """
    Get human-readable job type display name.
    
    Examples:
        - "file_upload" -> "File Upload"
        - "domain_dictionary" -> "Domain Dictionary"
        - "data_fetch" -> "Data Fetch"
    """
    type_mapping = {
        "file_upload": "File Upload",
        "domain_dictionary": "Domain Dictionary",
        "data_fetch": "Data Fetch",
        "analysis": "Analysis",
        "qna": "Q&A",
    }
    
    return type_mapping.get(job_type, job_type.replace("_", " ").title())


def calculate_progress(milestones: Optional[List[MilestoneResponse]]) -> Optional[float]:
    """
    Calculate job progress percentage based on milestones.
    
    Args:
        milestones: List of milestones for the job
        
    Returns:
        Progress percentage (0-100) or None if no milestones
    """
    if not milestones:
        return None
    
    # Count completed milestones (milestones with job_status='completed')
    # Support both job_status (new) and status (old) for backward compatibility
    completed_count = sum(
        1 for m in milestones 
        if (hasattr(m, 'job_status') and m.job_status == 'completed') or
           (hasattr(m, 'status') and getattr(m, 'status', None) == 'completed')
    )
    
    if completed_count == 0:
        return 0.0
    
    return min(100.0, (completed_count / len(milestones)) * 100)


def organize_job_metadata(job: JobResponse) -> Dict[str, Any]:
    """
    Organize job metadata into UI-friendly and technical sections.
    
    Returns:
        Dict with 'display_metadata' (UI-friendly) and 'technical_metadata' (raw technical details)
    """
    display_metadata = {}
    technical_metadata = {}
    
    # Extract UI-friendly fields from job_config
    if job.job_config:
        ui_fields = ["filename", "domain", "tables", "query", "model", "data_set_files"]
        for field in ui_fields:
            if field in job.job_config:
                display_metadata[field] = job.job_config[field]
        
        # Keep technical fields separate (excluding UI fields and internal fields)
        technical_metadata["job_config"] = {
            k: v for k, v in job.job_config.items() 
            if k not in ui_fields and not k.startswith("_")
        }
    
    # Organize execution metadata
    if job.execution_metadata:
        # Extract UI-friendly execution info
        ui_execution_fields = ["container_port", "input_dir", "output_dir"]
        display_execution = {}
        technical_execution = {}
        
        for field in ui_execution_fields:
            if field in job.execution_metadata:
                display_execution[field] = job.execution_metadata[field]
        
        # Keep rest as technical
        technical_execution = {
            k: v for k, v in job.execution_metadata.items()
            if k not in ui_execution_fields
        }
        
        if display_execution:
            display_metadata["execution"] = display_execution
        if technical_execution:
            technical_metadata["execution"] = technical_execution
    
    # Organize result metadata
    if job.result_metadata:
        # Result metadata is generally UI-friendly
        display_metadata["result"] = job.result_metadata
        technical_metadata["result_details"] = job.result_metadata
    
    return {
        "display_metadata": display_metadata,
        "technical_metadata": technical_metadata
    }


def enrich_job_for_ui(
    job: JobResponse, 
    milestones: Optional[List[MilestoneResponse]] = None,
    index: Optional[int] = None
) -> Dict[str, Any]:
    """
    Enrich job response with UI-friendly computed fields for frontend consumption.
    
    Args:
        job: Job response object
        milestones: Optional list of milestones for progress calculation
        index: Optional sequential index for stable ordering
        
    Returns:
        Dict with enriched job data including computed fields
    """
    # Calculate duration
    duration_seconds = None
    if job.started_at and job.completed_at:
        duration_seconds = (job.completed_at - job.started_at).total_seconds()
    elif job.started_at:
        # Job is still running
        now = datetime.now(timezone.utc) if job.started_at.tzinfo else datetime.now()
        if job.started_at.tzinfo:
            duration_seconds = (now - job.started_at).total_seconds()
        else:
            # Handle naive datetime
            duration_seconds = (datetime.now() - job.started_at).total_seconds()
    
    # Get status display info
    status_info = get_status_display_info(job.status)
    
    # Calculate progress
    progress_percentage = calculate_progress(milestones)
    
    # Get latest milestone
    latest_milestone = None
    if milestones and len(milestones) > 0:
        # Milestones are already ordered by created_at (ascending)
        latest = milestones[-1]
        latest_milestone = {
            "milestone_id": latest.milestone_id,
            "name": latest.name,
            "description": latest.description,
            "job_status": latest.job_status if hasattr(latest, 'job_status') else (latest.status if hasattr(latest, 'status') else None),
            "created_at": latest.created_at.isoformat() if latest.created_at else None,
            "updated_at": latest.updated_at.isoformat() if latest.updated_at else None,
        }
    
    # Organize metadata
    metadata_organized = organize_job_metadata(job)
    
    # Build enriched job dict
    enriched_job = {
        # Core identifiers (for reverse mapping)
        "job_id": job.job_id,
        "session_id": job.session_id,
        "user_id": job.user_id,
        
        # Display fields
        "job_type": job.job_type,
        "job_type_display": get_job_type_display(job.job_type),
        "status": job.status.value,
        "status_display": status_info["display"],
        "status_color": status_info["color"],
        "label": job.label,
        
        # Timestamps (ISO format for UI)
        "created_on": job.created_on.isoformat() if job.created_on else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        
        # Computed fields for UI
        "duration_seconds": duration_seconds,
        "duration_display": format_duration(duration_seconds),
        "is_running": job.status == JobStatus.RUNNING,
        "is_completed": job.status == JobStatus.COMPLETED,
        "is_failed": job.status == JobStatus.ERROR,
        "can_cancel": job.status in [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INITIALIZED],
        
        # Progress tracking
        "progress_percentage": progress_percentage,
        "milestone_count": len(milestones) if milestones else 0,
        "latest_milestone": latest_milestone,
        
        # Error information (if failed)
        "error_summary": job.error_message[:100] if job.error_message else None,
        "error_message": job.error_message,
        "error_type": job.error_type,
        
        # Metadata (organized for UI)
        "display_metadata": metadata_organized["display_metadata"],
        "technical_metadata": metadata_organized["technical_metadata"],
        
        # Ordering index (for consistent UI ordering)
        "sort_index": index,
    }
    
    return enriched_job
