"""
Analysis execution API (v2/modules/analysis).

Uses Job Framework for:
- Progress tracking via milestones
- Cancellation support
- Status polling
- Persistent job state

ARCHITECTURE:
Long-running analysis operations are wrapped with the Job Framework.

Lifecycle milestones (created directly in handler):
  Analysis job started → Deploying Cloud Run → Deployed → Calling execution →
  Execution completed → Cleaning up → Cleaned up

Granular sub-phase milestones (from execution_layer via SocketIO progress events):
  Query analysis → EDA started → EDA in progress → Hypothesis testing →
  Story/narrative generation → Report generation → Analysis complete
  (socketio_manager._maybe_create_progress_milestone)
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union
import traceback
import requests

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

from v2.common.model.api_response import ApiResponse
from v2.common.logger import add_log
from v2.modules.job_framework.manager.job.job_manager import JobManager
from v2.modules.job_framework.models.job_model import JobCreate, JobStatus
from v2.modules.job_framework.manager.job.job import JobContext, CancellationError
from v2.modules.session_framework.manager.session.session_manager import SessionManager
import asyncio


router = APIRouter(prefix="/analysis", tags=["Analysis"])


def _convert_id_to_int(id_value: Union[str, int]) -> int:
    """
    Convert user_id or session_id to integer.
    
    Handles:
    - Integer values: returns as-is
    - Numeric strings: converts to int
    - Non-numeric strings (emails, UUIDs): uses hash to generate deterministic integer
    """
    if id_value is None:
        return 0
    if isinstance(id_value, int):
        return id_value
    if isinstance(id_value, str):
        if id_value.isdigit() or (id_value.startswith('-') and id_value[1:].isdigit()):
            return int(id_value)
        hashed = abs(hash(id_value))
        return hashed % (2**31 - 1)
    return abs(hash(str(id_value))) % (2**31 - 1)


def get_job_manager() -> JobManager:
    """Dependency to get job manager instance"""
    return JobManager()


def get_session_manager() -> SessionManager:
    """Dependency to get session manager instance"""
    return SessionManager()


class StartAnalysisRequest(BaseModel):
    user_id: str = Field(..., min_length=1, description="User ID (for Cloud Run service naming and GCS paths)")
    session_id: str = Field(..., min_length=1)
    query: str = Field(..., min_length=1)
    model: Optional[str] = Field(default="gpt-5.4")
    job_type: Optional[str] = Field(default="standard", description="Job type: 'standard' or 'simpleqna'")


@router.post("/start", response_model=ApiResponse[dict])
async def start_analysis(
    req: StartAnalysisRequest,
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Start analysis job.
    
    This is a long-running operation wrapped with Job Framework for:
    - Progress tracking via milestones
    - Cancellation support
    - Status polling
    
    Flow:
    1. Create job (generates job_id)
    2. Deploy Cloud Run service
    3. Call execution endpoint (logs streamed via socket)
    4. Monitor logs and update milestones
    5. Save reports to bucket
    6. Clean up Cloud Run service
    
    Returns:
        job_id: ID to poll for status at GET /api/v2/jobs/{job_id}
        milestones: Poll at GET /api/v2/jobs/{job_id}/milestones
    """
    try:
        add_log(f"[ANALYSIS] Starting analysis for session: {req.session_id}")

        # Token check: ensure user has sufficient tokens before starting
        from v2.modules.utility.services.token_service import check_can_proceed, get_user_tokens, add_used_tokens
        can_proceed, token_msg = await check_can_proceed(req.user_id, "analysis")
        if not can_proceed:
            raise HTTPException(status_code=402, detail=token_msg)
        add_log(f"[ANALYSIS] Token check passed: {token_msg}")
        user_token_info = await get_user_tokens(req.user_id)
        
        # Get job_manager reference for log monitoring
        job_manager_ref = job_manager
        
        # Define handler (the actual work)
        async def analysis_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for analysis execution with progress tracking"""
            session_id_str = req.session_id
            user_id_str = req.user_id
            query = req.query
            model = req.model or "gpt-5.4"
            job_type = req.job_type or "standard"
            
            # Store original job_id string for log monitoring
            original_job_id_str = f"JOB_{ctx.job_id}"  # Match v1 pattern for log compatibility
            
            await ctx.create_milestone("Analysis: Job started", {"dependency": "sequential", "is_llm_call": False})
            
            deployed_service_name = None
            try:
                ctx.check_cancellation()
                
                # Step 1: Deploy Cloud Run service
                await ctx.create_milestone("Analysis: Deploying Cloud Run service", {"dependency": "sequential", "is_llm_call": False})
                from v2.modules.session_framework.services.session.session_service import SessionService
                session_service = SessionService()

                # IMPORTANT: deploy_job_service is synchronous and can block the FastAPI event loop.
                # Run it in a worker thread so the API (and Socket.IO) stays responsive.
                import asyncio
                deploy = await asyncio.to_thread(
                    session_service.deploy_job_service,
                    job_id=original_job_id_str,
                    session_id=session_id_str,
                    user_id=user_id_str
                )
                deployed_service_name = deploy.get("service_name")
                service_url = deploy.get("service_url")
                
                if not service_url:
                    raise RuntimeError(f"Cloud Run service deployed but no URL returned for service: {deployed_service_name}")
                
                path = '/analyze_job' if job_type != 'simpleqna' else '/simpleqna/analyze_job'
                container_url = f"{service_url.rstrip('/')}{path}"
                health_url = f"{service_url.rstrip('/')}/health"
                await ctx.create_milestone(f"Analysis: Cloud Run deployed: {deployed_service_name}", {"dependency": "sequential", "is_llm_call": False})
                
                ctx.check_cancellation()
                
                # Step 2: Milestones come from execution_layer via SocketIO progress events
                # (socketio_manager._maybe_create_progress_milestone). No log parsing needed.
                
                # Step 3: Call execution endpoint
                await ctx.create_milestone("Analysis: Calling execution service", {"dependency": "sequential", "is_llm_call": False})
                
                # Check cancellation before making the request
                ctx.check_cancellation()
                
                # Store deployed_service_name in job_config for cancellation cleanup
                # This allows cancel endpoint to access it
                # Update the job_config in storage
                current_job = await job_manager_ref._get_job_by_id(ctx.job_id)
                if current_job:
                    from v2.modules.job_framework.models.job_model import JobUpdate
                    updated_config = current_job.job_config or {}
                    updated_config["_deployed_service_name"] = deployed_service_name
                    updated_config["_original_job_id_str"] = original_job_id_str
                    await job_manager_ref._update_job(
                        ctx.job_id,
                        JobUpdate(job_config=updated_config)
                    )
                    # Also update local context
                    ctx.job_config.update({
                        "_deployed_service_name": deployed_service_name,
                        "_original_job_id_str": original_job_id_str
                    })
                
                # Make the HTTP request
                # Note: requests library doesn't support easy cancellation mid-request
                # Cancellation will be checked before request and cleanup will happen in finally
                def _wait_for_container_health(max_attempts: int = 10) -> None:
                    """Wait until Cloud Run instance is reachable before sending analysis request."""
                    backoff = 1.0
                    for attempt in range(1, max_attempts + 1):
                        try:
                            resp = requests.get(health_url, timeout=10)
                            if resp.status_code < 500:
                                return
                        except Exception:
                            pass
                        if attempt < max_attempts:
                            time.sleep(backoff)
                            backoff = min(backoff * 1.5, 5.0)
                    raise RuntimeError(f"Execution service health check failed: {health_url}")

                # Backward-compatible migration:
                # Older sessions may have domain_dictionary.json only. Standardize to domain_directory.json
                # so the execution layer can discover datasets reliably.
                try:
                    from v2.common.gcp import GcpManager
                    from v2.utils.env import init_env

                    constants = init_env()
                    bucket_name = constants.get("storage_bucket")
                    if bucket_name and user_id_str and session_id_str:
                        gcp_manager = GcpManager._get_instance()
                        storage_service = gcp_manager._storage_service
                        base_prefix = f"{user_id_str}/{session_id_str}/input_data"
                        domain_new = f"{base_prefix}/domain_directory.json"
                        domain_legacy = f"{base_prefix}/domain_dictionary.json"

                        if (not storage_service._file_exists(bucket_name, domain_new)) and storage_service._file_exists(bucket_name, domain_legacy):
                            legacy_bytes = await asyncio.to_thread(
                                storage_service._download_bytes,
                                bucket_name,
                                domain_legacy,
                            )
                            if legacy_bytes:
                                await asyncio.to_thread(
                                    storage_service._upload_bytes,
                                    bucket_name,
                                    legacy_bytes,
                                    domain_new,
                                    "application/json",
                                )
                                add_log(f"[ANALYSIS] Migrated legacy domain_dictionary.json -> domain_directory.json for session {session_id_str}")
                except Exception as migrate_err:
                    add_log(f"[ANALYSIS] Domain file migration check failed (non-fatal): {migrate_err}")

                # Pseudonymize query before execution if session has pseudonymized=true
                query_to_send = query
                try:
                    from v2.modules.pseudonymization.services.pseudonymization_service import PseudonymizationService
                    pseudo_svc = PseudonymizationService()
                    query_to_send = await pseudo_svc.pseudonymize_query_for_execution(
                        user_id=user_id_str,
                        session_id=session_id_str,
                        query=query,
                    )
                except Exception as pseudo_err:
                    add_log(f"[ANALYSIS] Query pseudonymization failed (using original): {pseudo_err}")

                def _call_container_with_retries():
                    payload = {
                        'job_id': original_job_id_str,
                        'query': query_to_send,
                        'model': model,
                        'session_id': session_id_str,
                        'user_id': user_id_str,
                        'user_email': user_id_str,
                        'user_email_sanitized': user_id_str,
                        'user_token_info': user_token_info,
                    }
                    session_req = requests.Session()
                    max_attempts = 3
                    backoff = 2
                    last_exc: Exception | None = None
                    for attempt in range(1, max_attempts + 1):
                        try:
                            response = session_req.post(container_url, json=payload, timeout=3600)
                            if response.status_code in (502, 503, 504) and attempt < max_attempts:
                                time.sleep(backoff)
                                backoff *= 2
                                continue
                            return response
                        except requests.RequestException as req_err:
                            last_exc = req_err
                            if attempt < max_attempts:
                                time.sleep(backoff)
                                backoff *= 2
                                continue
                            raise
                    if last_exc:
                        raise last_exc
                    raise RuntimeError("Execution service call failed unexpectedly")

                # Run the blocking HTTP call in a thread to avoid freezing the API server.
                await asyncio.to_thread(_wait_for_container_health)
                container_response = await asyncio.to_thread(_call_container_with_retries)
                
                if container_response.status_code == 200:
                    result = container_response.json()
                    await ctx.create_milestone("Analysis: Execution completed", {"dependency": "sequential", "is_llm_call": False})

                    # Update user tokens: add total_used to used_token in Firestore
                    metrics = result.get("metrics", {})
                    total_used = metrics.get("total_tokens", 0)
                    if total_used > 0 and user_id_str:
                        await add_used_tokens(user_id_str, total_used)
                        add_log(f"[ANALYSIS] Added {total_used:,} used tokens for user {user_id_str}")
                    
                    # Get public URL of analysis_report.html and save to job
                    report_url = await _get_report_url(
                        user_id=user_id_str,
                        session_id=session_id_str,
                        job_id=original_job_id_str
                    )
                    
                    # Update job with report_url
                    if report_url:
                        try:
                            current_job = await job_manager_ref._get_job_by_id(ctx.job_id)
                            if current_job:
                                from v2.modules.job_framework.models.job_model import JobUpdate
                                updated_config = current_job.job_config or {}
                                updated_config["report_url"] = report_url
                                await job_manager_ref._update_job(
                                    ctx.job_id,
                                    JobUpdate(job_config=updated_config)
                                )
                                add_log(f"[ANALYSIS] Saved report_url to job {ctx.job_id}: {report_url}")
                        except Exception as url_err:
                            add_log(f"[ANALYSIS] Warning: Failed to save report_url: {str(url_err)}")
                    
                    return {
                        "status": "success",
                        "message": "Analysis completed",
                        "result": result
                    }
                else:
                    error_msg = f"Execution service returned status {container_response.status_code}: {container_response.text}"
                    await ctx.create_milestone(f"Analysis: Execution failed: {error_msg[:100]}", {"dependency": "sequential", "is_llm_call": False})
                    # Add tokens even on failure - LLM calls were made, tokens were consumed
                    try:
                        err_body = container_response.json()
                        err_metrics = err_body.get("metrics", {})
                        total_used = err_metrics.get("total_tokens", 0)
                        if total_used > 0 and user_id_str:
                            await add_used_tokens(user_id_str, total_used)
                            add_log(f"[ANALYSIS] Added {total_used:,} used tokens (on failure) for user {user_id_str}")
                    except Exception:
                        pass
                    raise RuntimeError(error_msg)
                    
            except CancellationError as cancel_err:
                # Job was cancelled - cleanup and exit gracefully
                error_msg = f"Analysis job cancelled: {str(cancel_err)}"
                await ctx.create_milestone(f"Analysis: Cancelled", {"dependency": "sequential", "is_llm_call": False})
                add_log(f"[ANALYSIS] Job {ctx.job_id} cancelled: {str(cancel_err)}")
                # Cleanup will happen in finally block
                raise
            except Exception as e:
                error_msg = f"Analysis execution failed: {str(e)} | traceback: {traceback.format_exc()}"
                await ctx.create_milestone(f"Analysis: Error", {"dependency": "sequential", "is_llm_call": False})
                raise
            finally:
                # Always cleanup Cloud Run service. Use timeout so we don't block forever if GCP API hangs;
                # job status will still be persisted after handler returns.
                if deployed_service_name:
                    try:
                        await ctx.create_milestone("Analysis: Cleaning up Cloud Run", {"dependency": "sequential", "is_llm_call": False})
                        from v2.modules.session_framework.services.session.session_service import SessionService
                        session_service = SessionService()
                        try:
                            await asyncio.wait_for(
                                asyncio.to_thread(
                                    session_service.delete_job_service,
                                    service_name=deployed_service_name,
                                    job_id=original_job_id_str
                                ),
                                timeout=60.0
                            )
                        except asyncio.TimeoutError:
                            add_log(f"[ANALYSIS] Warning: Cloud Run cleanup timed out after 60s for {deployed_service_name}")
                        await ctx.create_milestone("Analysis: Cloud Run cleaned up", {"dependency": "sequential", "is_llm_call": False})
                        add_log(f"[ANALYSIS] Cleaned up Cloud Run service: {deployed_service_name}")
                    except Exception as cleanup_err:
                        add_log(f"[ANALYSIS] Warning: Failed to cleanup Cloud Run service: {str(cleanup_err)}")
        
        # Create job with handler - execution starts automatically!
        # user_id and session_id are now always strings (UUIDs)
        job_data = JobCreate(
            user_id=req.user_id,  # user_id is now always a string (UUID)
            session_id=req.session_id,  # session_id is now always a string (UUID)
            job_type="analysis",
            label=f"Analysis: {req.query[:50]}",
            job_config={
                "query": req.query,
                "model": req.model or "gpt-5.4",
                "job_type": req.job_type or "standard",
                "_original_user_id": req.user_id,
                "_original_session_id": req.session_id
            }
        )
        
        # Create job and start execution (non-blocking)
        job = await job_manager._create_job(job_data, execute_func=analysis_handler)
        
        # Return immediately with job_id (execution happens in background)
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Analysis job started successfully",
            data={
                "job_id": job.job_id,
                "status": "started",
                "message": "Job is running in background. Poll /jobs/{job_id} for status."
            }
        )
            
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"[ANALYSIS] start_analysis failed: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# _monitor_analysis_logs removed: milestones now come from execution_layer
# via SocketIO progress events (socketio_manager._maybe_create_progress_milestone)


@router.get("/report/{job_id}")
async def get_analysis_report(
    job_id: str,
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    Get analysis report HTML for a completed analysis job.
    
    Args:
        job_id: Job ID to get report for
        
    Returns:
        HTML content of the analysis report
        
    Raises:
        404: If job not found, not an analysis job, not completed, or no report_url
        500: If error fetching HTML from report_url
    """
    try:
        # Get job by ID
        job = await job_manager._get_job_by_id(job_id)
        
        if not job:
            raise HTTPException(
                status_code=404,
                detail=f"Job with ID '{job_id}' not found"
            )
        
        # Check if job type is analysis
        if job.job_type != "analysis":
            raise HTTPException(
                status_code=404,
                detail=f"Job '{job_id}' is not an analysis job (type: {job.job_type})"
            )
        
        # Check if job status is completed
        if job.status != JobStatus.COMPLETED:
            raise HTTPException(
                status_code=404,
                detail=f"Job '{job_id}' is not completed (status: {job.status.value})"
            )
        
        # Check if report_url exists in job_config
        job_config = job.job_config or {}
        report_url = job_config.get("report_url")
        
        if not report_url:
            raise HTTPException(
                status_code=404,
                detail=f"Job '{job_id}' does not have a report_url"
            )
        
        # Fetch HTML from report_url
        try:
            # Use asyncio.to_thread to make HTTP request without blocking
            def fetch_html():
                response = requests.get(report_url, timeout=30)
                response.raise_for_status()
                return response.content.decode("utf-8", errors="replace")
            
            html_content = await asyncio.to_thread(fetch_html)
            
            # Return HTML content with proper content type (no-cache so report refreshes)
            return Response(
                content=html_content,
                media_type="text/html; charset=utf-8",
                headers={
                    "Content-Disposition": f'inline; filename="analysis_report_{job_id}.html"',
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                }
            )
            
        except requests.RequestException as fetch_err:
            add_log(f"[ANALYSIS] Error fetching HTML from report_url '{report_url}': {str(fetch_err)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch report HTML from URL: {str(fetch_err)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"[ANALYSIS] Error getting analysis report for job {job_id}: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


async def _get_report_url(user_id: str, session_id: str, job_id: str) -> Optional[str]:
    """
    Get public URL for analysis_report.html from GCS bucket.
    
    Args:
        user_id: User ID (string UUID)
        session_id: Session ID (string UUID)
        job_id: Job ID (string, e.g., "JOB_..." or Firestore doc ID)
        
    Returns:
        Public URL string if found, None otherwise
    """
    try:
        from v2.common.gcp import GcpManager
        from v2.utils.env import init_env
        
        # Get bucket name from constants
        constants = init_env()
        bucket_name = constants.get('storage_bucket')
        
        if not bucket_name:
            add_log(f"[ANALYSIS] No storage_bucket configured, cannot get report URL")
            return None
        
        # Try multiple job_id formats (container may use JOB_ prefix or raw Firestore doc ID)
        job_id_variants = [job_id]
        if job_id.startswith("JOB_"):
            job_id_variants.append(job_id[4:])  # Without JOB_ prefix
        else:
            job_id_variants.append(f"JOB_{job_id}")
        
        gcp_manager = GcpManager._get_instance()
        storage_client = gcp_manager._storage_service._client
        bucket = storage_client.bucket(bucket_name)
        
        # Retry with short delay (GCS FUSE may have sync lag after container writes)
        for attempt in range(3):
            for jid in job_id_variants:
                gcs_blob_path = f"{user_id}/{session_id}/output_data/{jid}/analysis_report.html"
                blob = bucket.blob(gcs_blob_path)
                exists = await asyncio.to_thread(blob.exists)
                if exists:
                    try:
                        await asyncio.to_thread(blob.make_public)
                    except Exception as make_public_err:
                        add_log(f"[ANALYSIS] Note: Could not make blob public (may already be public): {str(make_public_err)}")
                    public_url = blob.public_url
                    add_log(f"[ANALYSIS] Found report URL: {public_url}")
                    return public_url
            if attempt < 2:
                await asyncio.sleep(1.0 + attempt)  # 1s, 2s delays
        
        add_log(f"[ANALYSIS] Report file not found at any of: {[f'...output_data/{j}/analysis_report.html' for j in job_id_variants]}")
        return None
        
    except Exception as e:
        add_log(f"[ANALYSIS] Error getting report URL: {str(e)} | traceback: {traceback.format_exc()}")
        return None

