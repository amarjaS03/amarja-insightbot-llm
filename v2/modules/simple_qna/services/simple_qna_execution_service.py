"""
Simple QnA Execution Service for v2

Orchestrates query execution flow:
1. Validate chat is open
2. Create query document (pending)
3. Update job.last_query_at
4. Deploy Cloud Run service
5. Invoke /simpleqna/analyze_job
6. Receive result
7. Persist answer
8. Update milestones
9. Tear down Cloud Run
"""
import asyncio
import requests
import traceback
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from v2.modules.simple_qna.services.chat_job_service import ChatJobService, ChatContext
from v2.modules.simple_qna.services.query_store_service import QueryStoreService
from v2.modules.session_framework.services.session.session_service import SessionService
from v2.modules.job_framework.manager.job.job_manager import JobManager
from v2.modules.job_framework.models.job_model import MilestoneCreate, JobUpdate, JobStatus
from v2.common.logger import add_log


class SimpleQnaExecutionService:
    """
    Service for executing Simple QnA queries via Cloud Run.
    Each query executes in an ephemeral Cloud Run container.
    """
    
    def __init__(self):
        self.chat_job_service = ChatJobService()
        self.query_store_service = QueryStoreService()
        self.session_service = SessionService()
        self.job_manager = JobManager()
    
    async def execute_query(
        self,
        chat_id: str,
        query: str,
        model: str = None,
        session_id: str = None,
        user_id: str = None
    ) -> tuple[Optional[str], bool]:
        """
        Execute a query within a chat (synchronous wait for completion).
        
        Flow:
        1. Check query count - rollover if needed
        2. Validate chat is open
        3. Create query document (pending)
        4. Update job.last_query_at
        5. Get Cloud Run service URL (deployed during chat creation)
        6. Invoke /simpleqna/analyze_job
        7. Wait for execution to complete
        8. Receive result
        9. Persist answer
        10. Update milestones
        
        Args:
            chat_id: Chat ID (job_id)
            query: User question
            model: LLM model to use
            session_id: Session ID (optional, fetched from job if not provided)
            user_id: User ID (optional, fetched from job if not provided)
            
        Returns:
            tuple: (query_id, rolled_over)
                - query_id: query_id if execution completed successfully, None otherwise
                - rolled_over: True if chat was rolled over, False otherwise
        """
        # Get default model from environment variable if not provided
        import os
        if not model:
            model = os.getenv("MODEL_NAME", "gpt-5.4")
        
        original_chat_id = chat_id
        query_id = None  # Initialize to ensure it's in scope
        rolled_over = False
        container_already_checked = False  # Track if we already checked container health (for optimization)
        
        try:
            # Get chat job to extract session_id and user_id if not provided
            chat_job = await self.chat_job_service.get_chat_job(chat_id)
            if not chat_job:
                error_msg = f"Chat {chat_id} not found"
                add_log(f"SimpleQnaExecutionService: {error_msg}")
                raise ValueError(error_msg)
            
            if not session_id:
                session_id = chat_job.session_id
            if not user_id:
                user_id = chat_job.user_id

            # Token check: ensure user has sufficient tokens before starting
            from v2.modules.utility.services.token_service import check_can_proceed, get_user_tokens, add_used_tokens
            can_proceed, token_msg = await check_can_proceed(user_id, "simple_qna")
            if not can_proceed:
                raise ValueError(token_msg)
            add_log(f"[SIMPLE_QNA_EXECUTION] Token check passed: {token_msg}", job_id=chat_id)
            user_token_info = await get_user_tokens(user_id)
            
            # Step 0.1: Check rollover policy (centralized in ChatJobService)
            chat_id, rolled_over = await self.chat_job_service.get_or_rollover_chat(
                chat_id=chat_id,
                user_id=user_id,
                session_id=session_id,
                model=model,
                label=chat_job.label
            )
            
            if rolled_over:
                add_log(f"SimpleQnaExecutionService: Chat rolled over from {original_chat_id} to {chat_id}")
            
            # Step 1: Validate chat is open (or reopen if completed)
            is_open = await self.chat_job_service.validate_chat_open(chat_id)
            
            if not is_open:
                # Check if chat is completed - if so, try to reopen it
                job = await self.job_manager._get_job_by_id(chat_id)
                if job:
                    status = job.status.value if hasattr(job.status, 'value') else str(job.status)
                    if status == 'completed':
                        # Try to reopen the completed chat
                        add_log(f"SimpleQnaExecutionService: Chat {chat_id} is completed - attempting to reopen...")
                        reopened = await self.chat_job_service.reopen_completed_chat(chat_id)
                        if reopened:
                            add_log(f"SimpleQnaExecutionService: Successfully reopened completed chat {chat_id}")
                            # After reopening, proactively check if container exists and is healthy
                            # If container is missing or unhealthy, redeploy it
                            # Use shorter timeout (5s) for faster failure detection
                            add_log(f"SimpleQnaExecutionService: Checking container status for reopened chat {chat_id}...")
                            container_ready = await self.chat_job_service.check_and_ensure_service_healthy(chat_id, timeout=5)
                            container_already_checked = True  # Mark that we already checked
                            if container_ready:
                                add_log(f"SimpleQnaExecutionService: Container is ready for reopened chat {chat_id}")
                            else:
                                add_log(f"SimpleQnaExecutionService: Container check/redeploy failed for reopened chat {chat_id}, but continuing - will retry during execution")
                        else:
                            error_msg = f"Chat {chat_id} is completed and could not be reopened"
                            add_log(f"SimpleQnaExecutionService: {error_msg}")
                            raise ValueError(error_msg)
                    else:
                        # Chat is in error or cancelled state - cannot proceed
                        error_msg = f"Chat {chat_id} is not open for new queries (status: {status})"
                        add_log(f"SimpleQnaExecutionService: {error_msg}")
                        raise ValueError(error_msg)
                else:
                    error_msg = f"Chat {chat_id} not found"
                    add_log(f"SimpleQnaExecutionService: {error_msg}")
                    raise ValueError(error_msg)
            
            # SEQUENTIAL EXECUTION ENFORCEMENT: Check if any query is currently running
            has_running = await self.chat_job_service.has_running_queries(chat_id)
            if has_running:
                error_msg = f"Chat {chat_id} has queries currently running. Please wait for them to complete before adding a new query."
                add_log(f"SimpleQnaExecutionService: {error_msg}")
                raise ValueError(error_msg)
            
            # Step 2: Create query document (pending) - can be done in parallel with container check
            # Note: chat_id may have changed due to rollover
            from v2.modules.simple_qna.models.simple_qna_models import QueryCreate
            query_data = QueryCreate(query=query)
            query_doc = await self.query_store_service.create_query(chat_id, query_data, model=model)
            query_id = query_doc.query_id
            
            # Update job status to running if this is the first query
            # Get current chat_job (may have changed after rollover)
            current_chat_job = await self.chat_job_service.get_chat_job(chat_id)
            if current_chat_job:
                chat_status = current_chat_job.status
                # Check if status is 'pending' (Job Framework initial status)
                if chat_status == 'pending':
                    await self.job_manager._update_job(
                        chat_id,
                        JobUpdate(status=JobStatus.RUNNING)
                    )
            
            # Step 3: Update job.last_query_at
            await self.chat_job_service.update_chat_metadata(
                chat_id,
                last_query_at=datetime.now(timezone.utc)
            )
            
            # Create milestone: QUERY_CREATED (Progress format for report consistency)
            try:
                milestone = MilestoneCreate(
                    job_id=chat_id,
                    name="QUERY_CREATED",
                    description="Progress: Query created",
                    data={"query_id": query_id, "query": query}
                )
                await self.job_manager._save_milestone(milestone)
            except Exception as e:
                add_log(f"SimpleQnaExecutionService: Failed to create QUERY_CREATED milestone: {str(e)}")
            
            add_log(f"SimpleQnaExecutionService: Created query {query_id} for chat {chat_id}")
            
        except Exception as e:
            error_msg = f"SimpleQnaExecutionService: Error in setup phase: {str(e)} | traceback: {traceback.format_exc()}"
            add_log(error_msg)
            # Re-raise with more context for better error messages
            raise RuntimeError(f"Failed to start query execution: {str(e)}") from e
        
        # Only start execution if query_id was successfully created
        if not query_id:
            add_log(f"SimpleQnaExecutionService: Cannot start execution - query_id not created")
            return None, rolled_over
        
        # Mark query as running before starting execution
        await self.query_store_service.mark_query_running(query_id, chat_id)
        
        # Create milestone: QUERY_EXECUTION_STARTED (Progress format for report consistency)
        try:
            milestone = MilestoneCreate(
                job_id=chat_id,
                name="QUERY_EXECUTION_STARTED",
                description="Progress: Query execution started",
                data={"query_id": query_id}
            )
            await self.job_manager._save_milestone(milestone)
        except Exception as e:
            add_log(f"SimpleQnaExecutionService: Failed to create QUERY_EXECUTION_STARTED milestone: {str(e)}")
        
        # Get Cloud Run service URL from chat job (deployed during chat creation)
        job_obj = await self.job_manager._get_job_by_id(chat_id)
        if not job_obj:
            error_msg = f"Chat job {chat_id} not found"
            await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
            raise RuntimeError(error_msg)
        
        job_config = job_obj.job_config or {}
        service_name = job_config.get("cloud_run_service_name")
        service_url = job_config.get("cloud_run_service_url")
        container_url = job_config.get("cloud_run_container_url")
        
        add_log(f"[SIMPLE_QNA_EXECUTION] Retrieving Cloud Run URL for chat {chat_id}:", job_id=chat_id)
        add_log(f"  - Service Name: {service_name}", job_id=chat_id)
        add_log(f"  - Service URL: {service_url}", job_id=chat_id)
        add_log(f"  - Container URL: {container_url}", job_id=chat_id)
        
        if not container_url:
            # Check if Cloud Run deployment is still in progress
            if service_name:
                # Try to redeploy the Cloud Run service (may have been created before OPENAI_API_KEY fix)
                add_log(f"[SIMPLE_QNA_EXECUTION] Container URL missing, attempting to redeploy Cloud Run service for chat {chat_id}", job_id=chat_id)
                redeployed = await self.chat_job_service.redeploy_cloud_run_service(chat_id)
                if redeployed:
                    # Re-fetch job to get updated container_url
                    job_obj = await self.job_manager._get_job_by_id(chat_id)
                    if job_obj:
                        job_config = job_obj.job_config or {}
                        container_url = job_config.get("cloud_run_container_url")
                        if container_url:
                            add_log(f"[SIMPLE_QNA_EXECUTION] Successfully redeployed and retrieved container URL: {container_url}", job_id=chat_id)
                        else:
                            error_msg = f"Cloud Run service redeployed but container URL still not found in job_config for chat {chat_id}."
                            await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
                            raise RuntimeError(error_msg)
                    else:
                        error_msg = f"Cloud Run service redeployed but chat job {chat_id} not found."
                        await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
                        raise RuntimeError(error_msg)
                else:
                    error_msg = f"Cloud Run service '{service_name}' exists but container URL not found in job_config for chat {chat_id}. Redeployment failed."
                    await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
                    raise RuntimeError(error_msg)
            else:
                error_msg = f"Cloud Run service not deployed for chat {chat_id}. The chat may have been created before Cloud Run deployment completed. Please try again in a few moments."
                await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
                raise RuntimeError(error_msg)
        
        add_log(f"[SIMPLE_QNA_EXECUTION] Using Cloud Run service: {container_url}", job_id=chat_id)
        
        # Check Cloud Run service health before executing query
        # Skip if we already checked after reopening (optimization: avoid redundant check)
        if not container_already_checked:
            try:
                add_log(f"[SIMPLE_QNA_EXECUTION] Checking Cloud Run service health before query execution...", job_id=chat_id)
                is_healthy = await self.chat_job_service.check_and_ensure_service_healthy(chat_id, timeout=5)
                
                if not is_healthy:
                    error_msg = f"Cloud Run service is unhealthy for chat {chat_id} and redeployment failed"
                    await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
                    raise RuntimeError(error_msg)
                
                # Re-fetch job to get potentially updated container_url after redeployment
                job_obj = await self.job_manager._get_job_by_id(chat_id)
                if job_obj:
                    updated_config = job_obj.job_config or {}
                    updated_container_url = updated_config.get("cloud_run_container_url")
                    if updated_container_url and updated_container_url != container_url:
                        container_url = updated_container_url
                        add_log(f"[SIMPLE_QNA_EXECUTION] Updated container URL after health check/redeploy: {container_url}", job_id=chat_id)
                    else:
                        add_log(f"[SIMPLE_QNA_EXECUTION] Cloud Run service is healthy (URL unchanged)", job_id=chat_id)
            except RuntimeError:
                # Re-raise RuntimeError (service unhealthy and redeploy failed)
                raise
            except Exception as health_check_err:
                # Don't fail the query if health check fails - log warning and continue with existing service
                add_log(f"[SIMPLE_QNA_EXECUTION] Warning: Health check failed (continuing with existing service): {str(health_check_err)}", job_id=chat_id)
        else:
            add_log(f"[SIMPLE_QNA_EXECUTION] Skipping redundant health check (already checked after reopen)", job_id=chat_id)
        
        # Backward-compatible migration:
        # Older sessions may have domain_dictionary.json only. Standardize to domain_directory.json
        # so the execution layer can discover datasets reliably.
        try:
            from v2.common.gcp import GcpManager
            from v2.utils.env import init_env

            constants = init_env()
            bucket_name = constants.get("storage_bucket")
            if bucket_name and user_id and session_id:
                gcp_manager = GcpManager._get_instance()
                storage_service = gcp_manager._storage_service
                base_prefix = f"{user_id}/{session_id}/input_data"
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
                        add_log(f"[SIMPLE_QNA_EXECUTION] Migrated legacy domain_dictionary.json -> domain_directory.json for session {session_id}", job_id=chat_id)
        except Exception as migrate_err:
            add_log(f"[SIMPLE_QNA_EXECUTION] Domain file migration check failed (non-fatal): {migrate_err}", job_id=chat_id)

        # Pseudonymize query before execution if session has pseudonymized=true (must be in async scope)
        query_to_send = query
        try:
            from v2.modules.pseudonymization.services.pseudonymization_service import PseudonymizationService
            pseudo_svc = PseudonymizationService()
            query_to_send = await pseudo_svc.pseudonymize_query_for_execution(
                user_id=user_id,
                session_id=session_id,
                query=query,
            )
        except Exception as pseudo_err:
            add_log(f"[SIMPLE_QNA_EXECUTION] Query pseudonymization failed (using original): {pseudo_err}")

        # Wait for execution to complete (similar to analysis pattern)
        # This blocks until the Cloud Run service responds, but runs in a thread pool
        # to avoid blocking the async event loop
        def _call_execution_service():
            """Make the HTTP request to Cloud Run execution service"""
            session_req = requests.Session()
            
            # Optional: Verify Cloud Run service is accessible via health check
            base_url = container_url.rsplit('/simpleqna/analyze_job', 1)[0]
            health_url = f"{base_url}/health"
            try:
                health_response = session_req.get(health_url, timeout=10)
                add_log(f"[SIMPLE_QNA_EXECUTION] Health check: {health_url} -> {health_response.status_code}", job_id=chat_id)
                if health_response.status_code != 200:
                    add_log(f"[SIMPLE_QNA_EXECUTION] Health check returned {health_response.status_code} - service may have issues", job_id=chat_id)
            except Exception as health_err:
                add_log(f"[SIMPLE_QNA_EXECUTION] Health check failed (service may be starting): {str(health_err)}", job_id=chat_id)
            
            # Now make the actual request (query_to_send from closure)
            payload = {
                'chat_id': chat_id,
                'query_id': query_id,
                'query': query_to_send,
                'model': model,
                'session_id': session_id,
                'user_id': user_id,
                'user_email': user_id,  # Execution layer compatibility
                'user_email_sanitized': user_id,
                'user_token_info': user_token_info,
            }
            add_log(f"[SIMPLE_QNA_EXECUTION] Calling execution service: {container_url}", job_id=chat_id)
            add_log(f"[SIMPLE_QNA_EXECUTION] Payload: chat_id={chat_id}, query_id={query_id}, query={query[:50]}...", job_id=chat_id)
            
            response = session_req.post(
                container_url,
                json=payload,
                timeout=3600,  # 1 hour timeout
                headers={'Content-Type': 'application/json'}
            )
            add_log(f"[SIMPLE_QNA_EXECUTION] Response status: {response.status_code}", job_id=chat_id)
            if response.status_code != 200:
                add_log(f"[SIMPLE_QNA_EXECUTION] Response headers: {dict(response.headers)}", job_id=chat_id)
                add_log(f"[SIMPLE_QNA_EXECUTION] Response text: {response.text[:1000]}", job_id=chat_id)
            return response
        
        # Run the blocking HTTP call in a thread to avoid freezing the API server
        # (same pattern as analysis module)
        container_response = await asyncio.to_thread(_call_execution_service)
        
        # Process the response
        if container_response.status_code == 200:
            result = container_response.json()
            answer = result.get('answer', '')
            metrics = result.get('metrics', {})
            error = result.get('error')
            
            # Depseudonymize Simple QnA HTML: read html -> LLM tag -> regex replace -> return and save to bucket
            if answer and session_id and user_id:
                try:
                    from v2.modules.pseudonymization.services.pseudonymization_service import PseudonymizationService
                    pseudo_service = PseudonymizationService()
                    answer = await pseudo_service.depseudonymize_simple_qna_html_and_save(
                        user_id=user_id,
                        session_id=session_id,
                        query_id=query_id,
                        html_content=answer,
                    )
                except Exception as depseudo_err:
                    add_log(f"[SIMPLE_QNA_EXECUTION] Depseudonymization failed (using original): {depseudo_err}")
            
            if error:
                # Add tokens even on failure - LLM calls were made, tokens were consumed
                total_used = metrics.get("total_tokens", 0)
                if total_used > 0 and user_id:
                    await add_used_tokens(user_id, total_used)
                    add_log(f"[SIMPLE_QNA_EXECUTION] Added {total_used:,} used tokens (on failure) for user {user_id}", job_id=chat_id)
                # Mark query as error
                await self.query_store_service.mark_query_error(
                    query_id, chat_id, error, {"metrics": metrics}
                )
                # Create milestone: QUERY_FAILED (Progress format for report consistency)
                try:
                    milestone = MilestoneCreate(
                        job_id=chat_id,
                        name="QUERY_FAILED",
                        description="Progress: Query failed",
                        data={"query_id": query_id, "error": error}
                    )
                    await self.job_manager._save_milestone(milestone)
                except Exception as e:
                    add_log(f"SimpleQnaExecutionService: Failed to create QUERY_FAILED milestone: {str(e)}")
                raise RuntimeError(f"Query execution failed: {error}")
            else:
                # Update user tokens: add total_used to used_tokens in Firestore
                total_used = metrics.get("total_tokens", 0)
                if total_used > 0 and user_id:
                    await add_used_tokens(user_id, total_used)
                    add_log(f"[SIMPLE_QNA_EXECUTION] Added {total_used:,} used tokens for user {user_id}", job_id=chat_id)

                # Step 7: Persist answer
                await self.query_store_service.mark_query_completed(
                    query_id, chat_id, answer, {"metrics": metrics}
                )
                
                # Update chat metadata: increment total_queries
                chat_job = await self.chat_job_service.get_chat_job(chat_id)
                if chat_job:
                    new_total = chat_job.total_queries + 1
                    await self.chat_job_service.update_chat_metadata(
                        chat_id, total_queries=new_total
                    )
                
                # Create milestone: QUERY_EXECUTION_COMPLETED (Progress format for report consistency)
                try:
                    milestone = MilestoneCreate(
                        job_id=chat_id,
                        name="QUERY_EXECUTION_COMPLETED",
                        description="Progress: Query execution completed",
                        data={"query_id": query_id, "metrics": metrics}
                    )
                    await self.job_manager._save_milestone(milestone)
                except Exception as e:
                    add_log(f"SimpleQnaExecutionService: Failed to create QUERY_EXECUTION_COMPLETED milestone: {str(e)}")
                
                add_log(f"[SIMPLE_QNA_EXECUTION] Query {query_id} completed successfully", job_id=chat_id)
        else:
            error_msg = f"Execution service returned status {container_response.status_code}: {container_response.text}"
            await self.query_store_service.mark_query_error(query_id, chat_id, error_msg)
            # Add tokens even on failure if metrics in error response
            try:
                err_body = container_response.json()
                err_metrics = err_body.get("metrics", {})
                total_used = err_metrics.get("total_tokens", 0)
                if total_used > 0 and user_id:
                    await add_used_tokens(user_id, total_used)
                    add_log(f"[SIMPLE_QNA_EXECUTION] Added {total_used:,} used tokens (on failure) for user {user_id}", job_id=chat_id)
            except Exception:
                pass
            raise RuntimeError(error_msg)
        
        return query_id, rolled_over
