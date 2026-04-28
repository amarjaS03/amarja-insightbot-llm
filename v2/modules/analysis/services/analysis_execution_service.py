"""
Analysis execution service - creates job, deploys Cloud Run and calls execution layer.
Matches v1's job_manager.start_job_execution pattern.
"""
import threading
import requests
import traceback
from typing import Dict, Any, Optional
from v2.modules.session_framework.services.session.session_service import SessionService
from v2.modules.analysis.services.analysis_job_manager import AnalysisJobManager, JobStatus
from v2.common.logger import add_log


class AnalysisExecutionService:
    """Service for executing analysis jobs via Cloud Run"""
    
    def __init__(self):
        self.session_service = SessionService()
        self.job_manager = AnalysisJobManager()
    
    def start_job_execution(
        self,
        session_id: str,
        user_id: str,
        query: str,
        model: str = "gpt-5.4",
        job_type: str = 'standard'
    ) -> Optional[str]:
        """
        Start asynchronous job execution.
        Flow: Create job → Deploy Cloud Run → Call execution → Stream logs → Save reports → Cleanup
        
        Args:
            session_id: Session identifier
            user_id: User ID (for Cloud Run service naming and GCS paths)
            query: Analysis query
            model: Model name (default: gpt-5.4)
            job_type: Job type ('standard' or 'simpleqna')
            
        Returns:
            str: job_id if execution started successfully, None otherwise
        """
        # Step 1: Create job (generates job_id and creates job record)
        job_id, job_info = self.job_manager.create_job(
            session_id=session_id,
            user_id=user_id,
            query=query,
            model=model
        )
        
        # Socket room is implicitly created when job is created (room name: session_{session_id}_job_{job_id})
        # Clients can join via 'join_job' event
        
        def execute_job():
            deployed_service_name = None
            try:
                add_log(f"[ANALYSIS_EXECUTION] Starting job execution: {job_id}", job_id=job_id)
                
                # Update job status to RUNNING
                self.job_manager.update_job_status(job_id, JobStatus.RUNNING)
                
                # Step 2: Deploy Cloud Run service for this job
                try:
                    add_log(f"[ANALYSIS_EXECUTION] Deploying Cloud Run service for job: {job_id}", job_id=job_id)
                    deploy = self.session_service.deploy_job_service(
                        job_id=job_id,
                        session_id=session_id,
                        user_id=user_id
                    )
                    deployed_service_name = deploy.get("service_name")
                    service_url = deploy.get("service_url")
                    if not service_url:
                        raise RuntimeError(f"Cloud Run service deployed but no URL returned for service: {deployed_service_name}")
                    
                    path = '/analyze_job' if job_type != 'simpleqna' else '/simpleqna/analyze_job'
                    container_url = f"{service_url.rstrip('/')}{path}"
                    add_log(f"[ANALYSIS_EXECUTION] Cloud Run service deployed successfully: {container_url}", job_id=job_id)
                except Exception as deploy_err:
                    error_msg_deploy = f"Failed to deploy Cloud Run service: {str(deploy_err)} | traceback: {traceback.format_exc()}"
                    add_log(f"[ANALYSIS_EXECUTION] {error_msg_deploy}", job_id=job_id)
                    return
                
                # Create requests session with timeout
                session_req = requests.Session()
                
                try:
                    # Send job execution request to Cloud Run service
                    add_log(f"[ANALYSIS_EXECUTION] Calling execution service: {container_url}", job_id=job_id)
                    # Send user_id as both user_id and user_email for execution layer compatibility
                    # Execution layer expects user_email/user_email_sanitized for path resolution
                    container_response = session_req.post(
                        container_url,
                        json={
                            'job_id': job_id,
                            'query': query,
                            'model': model,
                            'session_id': session_id,
                            'user_id': user_id,
                            'user_email': user_id,  # Execution layer compatibility: use user_id as email
                            'user_email_sanitized': user_id,  # Execution layer uses this for GCS paths
                        },
                        timeout=3600  # 1 hour timeout for analysis
                    )
                    
                    if container_response.status_code == 200:
                        result = container_response.json()
                        # Step 4: Reports are saved to bucket by execution layer
                        # Step 5: Update job status to COMPLETED
                        self.job_manager.update_job_status(job_id, JobStatus.COMPLETED)
                        add_log(f"[ANALYSIS_EXECUTION] Job {job_id} completed successfully", job_id=job_id)
                        print(f"[ANALYSIS_EXECUTION] Job completed: {result}")
                    else:
                        error_msg = f"Execution service returned status {container_response.status_code}: {container_response.text}"
                        self.job_manager.update_job_status(job_id, JobStatus.FAILED, error_msg)
                        add_log(f"[ANALYSIS_EXECUTION] Job {job_id} failed: {error_msg}", job_id=job_id)
                        print(f"[ANALYSIS_EXECUTION] Job failed: {error_msg}")
                        
                except requests.RequestException as e:
                    error_msg = f"Error communicating with execution service: {str(e)} | traceback: {traceback.format_exc()}"
                    self.job_manager.update_job_status(job_id, JobStatus.FAILED, error_msg)
                    add_log(f"[ANALYSIS_EXECUTION] Job {job_id} failed: {error_msg}", job_id=job_id)
                    print(f"[ANALYSIS_EXECUTION] Request error: {error_msg}")
                    
            except Exception as e:
                error_msg = f"Job execution failed: {str(e)} | traceback: {traceback.format_exc()}"
                self.job_manager.update_job_status(job_id, JobStatus.FAILED, error_msg)
                add_log(f"[ANALYSIS_EXECUTION] {error_msg}", job_id=job_id)
                print(f"[ANALYSIS_EXECUTION] Execution error: {error_msg}")
            finally:
                # Step 6: Cleanup - delete Cloud Run service after job completes
                if deployed_service_name:
                    try:
                        add_log(f"[ANALYSIS_EXECUTION] Cleaning up Cloud Run service: {deployed_service_name}", job_id=job_id)
                        self.session_service.delete_job_service(service_name=deployed_service_name, job_id=job_id)
                    except Exception as cleanup_err:
                        add_log(f"[ANALYSIS_EXECUTION] Warning: Failed to cleanup Cloud Run service {deployed_service_name}: {str(cleanup_err)}", job_id=job_id)
        
        # Start job execution in background thread
        job_thread = threading.Thread(target=execute_job, daemon=True)
        job_thread.start()
        
        return job_id
