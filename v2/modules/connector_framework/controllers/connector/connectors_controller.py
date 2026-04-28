"""
Connectors controller for FastAPI v2

Provides REST API endpoints for connector operations.

ARCHITECTURE:
Long-running operations (fetch-data, upload-file, copy-sample-data) are 
wrapped with the Job Framework for:
- Progress tracking via milestones
- Cancellation support  
- Status polling

Quick operations (connect, fetch-schema, health, disconnect) run directly.
"""
from typing import Dict, Any, Union
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from v2.common.model.api_response import ApiResponse
from v2.modules.connector_framework.manager.connector.connector_manager import ConnectorManager
from v2.modules.connector_framework.models.connector_models import (
    ConnectorActionRequest,
    ConnectorConnectRequest,
    ConnectorOAuthCallbackRequest,
    CopySampleDataRequest,
    FetchN8nDataRequest,
)
from v2.modules.job_framework.manager.job.job_manager import JobManager
from v2.modules.job_framework.models.job_model import JobCreate
from v2.modules.job_framework.manager.job.job import JobContext
from v2.modules.session_framework.manager.session.session_manager import SessionManager
from v2.modules.connector_framework.controllers.sample_data.sample_data_controller import _get_suggested_questions_for_filename
from v2.common.logger import add_log
import traceback

router = APIRouter(prefix="/connectors")


def _convert_id_to_int(id_value: Union[str, int]) -> int:
    """
    Convert user_id or session_id to integer.
    
    Handles:
    - Integer values: returns as-is
    - Numeric strings: converts to int
    - Non-numeric strings (emails, UUIDs): uses hash to generate deterministic integer
    
    Args:
        id_value: User ID or session ID (can be string or int)
        
    Returns:
        Integer representation of the ID
    """
    if id_value is None:
        return 0
    
    # If already an integer, return as-is
    if isinstance(id_value, int):
        return id_value
    
    # Try to convert numeric string to int
    if isinstance(id_value, str):
        # Check if it's a numeric string
        if id_value.isdigit() or (id_value.startswith('-') and id_value[1:].isdigit()):
            return int(id_value)
        
        # For non-numeric strings (emails, UUIDs), use hash
        # Use abs() to ensure positive integer, and modulo to keep it reasonable
        hashed = abs(hash(id_value))
        # Keep it within reasonable range (max 32-bit signed int)
        return hashed % (2**31 - 1)
    
    # Fallback: convert to string and hash
    return abs(hash(str(id_value))) % (2**31 - 1)


def get_connector_manager() -> ConnectorManager:
    """Dependency to get connector manager instance"""
    return ConnectorManager()


def get_job_manager() -> JobManager:
    """Dependency to get job manager instance"""
    return JobManager()


def get_session_manager() -> SessionManager:
    """Dependency to get session manager instance"""
    return SessionManager()


@router.post("/connect", response_model=ApiResponse)
async def connect(
    req: ConnectorConnectRequest,
    manager: ConnectorManager = Depends(get_connector_manager)
):
    """
    Connect to a data source.
    
    Creates a connector for the specified type and establishes connection
    using credentials from credential_framework.
    """
    try:
        result = await manager._connect(req)
        
        if result.get("result") == "success":
            return ApiResponse(
                status=result.get("status"),
                statusCode=200,
                message=result.get("message", ""),
                data=result.get("data", result)
            )
        else:
            return ApiResponse(
                status="error",
                statusCode=400,
                message=result.get("message", "Connection failed"),
                data=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/callback", response_model=ApiResponse)
async def oauth_callback(
    req: ConnectorOAuthCallbackRequest,
    manager: ConnectorManager = Depends(get_connector_manager)
):
    """
    Handle OAuth callback for Salesforce and Acumatica connectors.
    
    This endpoint processes the OAuth authorization code exchange after the user
    authorizes the application. Only supports Salesforce and Acumatica connectors.
    
    Request Body:
        - session_id: Session identifier
        - user_id: User identifier
        - connector_type: Must be 'salesforce' or 'acumatica'
        - code: Authorization code from OAuth provider
        - state: OAuth state parameter for CSRF protection
    
    Returns:
        Connection result with READY status after successful code exchange
    """
    try:
        # Validate connector type
        normalized_type = (req.connector_type or "").strip().lower()
        if normalized_type not in ["salesforce", "acumatica"]:
            return ApiResponse(
                status="error",
                statusCode=400,
                message=f"OAuth callback is only supported for 'salesforce' and 'acumatica'. Received: '{req.connector_type}'",
                data=None
            )
        
        # Handle OAuth callback
        result = await manager._handle_oauth_callback(
            session_id=req.session_id,
            user_id=req.user_id,
            connector_type=normalized_type,
            authorization_code=req.code,
            state=req.state
        )
        
        if result.get("result") == "success":
            return ApiResponse(
                status=result.get("status", "success"),
                statusCode=200,
                message=result.get("message", "OAuth callback processed successfully"),
                data=result.get("data")
            )
        else:
            return ApiResponse(
                status="error",
                statusCode=400,
                message=result.get("message", "OAuth callback failed"),
                data=None
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetch-schema", response_model=ApiResponse)
async def fetch_schema(
    req: ConnectorActionRequest,
    manager: ConnectorManager = Depends(get_connector_manager)
):
    """
    Fetch schema from the connected data source.
    
    Returns available tables, views, and objects depending on the connector type.
    """
    try:
        result = await manager._fetch_schema(req)
        
        if result.get("result") == "success":
            return ApiResponse(
                status="success",
                statusCode=200,
                message=result.get("message", "Schema retrieved successfully"),
                data=result.get("data")
            )
        else:
            return ApiResponse(
                status="error",
                statusCode=400,
                message=result.get("message", "Failed to fetch schema"),
                data=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/list-tables", response_model=ApiResponse)
async def list_tables(
    req: ConnectorActionRequest,
    manager: ConnectorManager = Depends(get_connector_manager)
):
    """
    List tables and views for database connectors (MSSQL/MySQL).
    
    Similar to v1's /db/list_tables endpoint. Uses the connector's get_schema() method
    to fetch tables and views, then returns them as simple string arrays.
    
    Only supports database connectors (mssql, mysql).
    
    Request body:
        - user_id: User ID
        - session_id: Session ID
        - connector_type: Must be "mssql" or "mysql"
    """
    try:
        result = await manager._list_tables(req)
        
        if result.get("result") == "success":
            return ApiResponse(
                status="success",
                statusCode=200,
                message=result.get("message", "Tables and views retrieved successfully"),
                data=result.get("data")
            )
        else:
            return ApiResponse(
                status="error",
                statusCode=400,
                message=result.get("message", "Failed to list tables"),
                data=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetch-data", response_model=ApiResponse)
async def fetch_data(
    req: ConnectorActionRequest,
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Fetch data from the connected data source.
    
    This is wrapped with Job Framework for consistent tracking and step updates.
    
    Request body should include:
        - user_id: User ID
        - session_id: Session ID
        - connector_type: Connector type
        - subject_area: Optional subject area name (for Salesforce/Acumatica)
        - tables: Optional list of table/object names (for database connectors)
        - params: Optional dict with additional parameters
    
    Returns:
        job_id: ID to poll for status at GET /api/v2/jobs/{job_id}
    """
    try:
        # Define handler
        async def data_fetch_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for fetching data with granular per-table milestones."""
            ctx.check_cancellation()
            
            # Extract original string IDs from job_config
            job_config = ctx.job_config or {}
            original_user_id = job_config.get("_original_user_id", req.user_id)
            original_session_id = job_config.get("_original_session_id", req.session_id)
            
            # Create request with original string IDs, preserving all fields from original request
            req_with_original_ids = ConnectorActionRequest(
                user_id=original_user_id,
                session_id=original_session_id,
                connector_type=req.connector_type,
                subject_area=req.subject_area,
                tables=req.tables,
                params=req.params
            )
            
            async def milestone_cb(description: str, data: dict = None):
                await ctx.create_milestone(description, data or {})
            
            result = await connector_manager._fetch_data(req_with_original_ids, milestone_callback=milestone_cb)
            
            if result.get("result") == "success":
                saved_count = len(result.get("data", {}).get("saved_files", []))
                await ctx.create_milestone(
                    f"Data fetch: Complete ({saved_count} file(s) saved)",
                    {"saved_count": saved_count, "dependency": "sequential", "is_llm_call": False}
                )
                return {
                    "status": "success",
                    "data": result.get("data")
                }
            else:
                return {
                    "status": "failed",
                    "error": result.get("message")
                }
        
        # Create job with handler - execution starts automatically!
        # Store original string IDs in job_config for Firestore lookups
        job_data = JobCreate(
            user_id=req.user_id,
            session_id=req.session_id,
            job_type="data_fetch",
            label=f"Fetch data",
            job_config={
                "subject_area": req.subject_area,
                "tables": req.tables,
                "params": req.params,
                "_original_user_id": req.user_id,
                "_original_session_id": req.session_id
            }
        )
        
        # Update session status to "in_progress" when data loading job starts
        try:
            await session_manager._update_session_status(req.session_id, "in_progress")
        except Exception as status_error:
            add_log(f"Warning: Failed to update session status to 'in_progress': {str(status_error)}")
        
        # Create job and start execution
        job = await job_manager._create_job(job_data, execute_func=data_fetch_handler)
        
        # Wait for job completion before returning response
        completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # Return response only after job completes
        if completed_job.status.value == "completed":
            return ApiResponse(
                status="success",
                statusCode=200,
                message="Data fetched successfully",
                data={
                    "job_id": completed_job.job_id,
                    "status": completed_job.status.value.upper(),
                    "result": completed_job.result_metadata
                }
            )
        elif completed_job.status.value == "error":
            raise HTTPException(
                status_code=500,
                detail=completed_job.error_message or "Data fetch failed"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Data fetch job ended with status: {completed_job.status.value}"
            )
        # tables = req.params.get("tables", []) if req.params else []
        
        # # Define handler (the actual work)
        # async def data_fetch_handler(ctx: JobContext) -> Dict[str, Any]:
        #     """Handler for data fetching with progress tracking"""
        #     fetched_tables = []
        #     failed_tables = []
            
        #     await ctx.create_milestone(f"Starting data fetch: {len(tables)} tables")
            
        #     for i, table in enumerate(tables):
        #         # Check for cancellation before each table
        #         ctx.check_cancellation()
                
        #         await ctx.create_milestone(f"Fetching table: {table} ({i+1}/{len(tables)})")
                
        #         try:
        #             # Call connector manager to fetch this table
        #             table_req = ConnectorActionRequest(
        #                 user_id=req.user_id,
        #                 session_id=req.session_id,
        #                 params={"tables": [table]}
        #             )
        #             result = await connector_manager._fetch_data(table_req)
                    
        #             if result.get("result") == "success":
        #                 fetched_tables.append(table)
        #                 await ctx.create_milestone(f"Saved: {table}.pkl ({i+1}/{len(tables)})")
        #             else:
        #                 failed_tables.append({"table": table, "error": result.get("message")})
        #         except Exception as e:
        #             failed_tables.append({"table": table, "error": str(e)})
            
        #     await ctx.create_milestone(
        #         f"Data fetch completed: {len(fetched_tables)}/{len(tables)} tables"
        #     )
            
        #     return {
        #         "fetched_tables": fetched_tables,
        #         "failed_tables": failed_tables,
        #         "total": len(tables),
        #         "successful": len(fetched_tables)
        #     }
        
        # # Create job with handler - execution starts automatically!
        # # Store original string IDs in job_config for Firestore lookups
        # job_data = JobCreate(
        #     user_id=_convert_id_to_int(req.user_id),
        #     session_id=_convert_id_to_int(req.session_id),
        #     job_type="data_fetch",
        #     label=f"Fetch data: {len(tables)} tables",
        #     job_config={
        #         "tables": tables,
        #         "_original_user_id": req.user_id,  # Store original string for Firestore
        #         "_original_session_id": req.session_id  # Store original string for Firestore
        #     }
        # )
        
        # # Update session status to "in_progress" when data loading job starts
        # try:
        #     await session_manager._update_session_status(req.session_id, "in_progress")
        # except Exception as status_error:
        #     add_log(f"Warning: Failed to update session status to 'in_progress': {str(status_error)}")
        
        # # Create job and start execution
        # job = await job_manager._create_job(job_data, execute_func=data_fetch_handler)
        
        # # Wait for job completion before returning response
        # completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # # Return response only after job completes
        # if completed_job.status.value == "completed":
        #     return ApiResponse(
        #         status="success",
        #         statusCode=200,
        #         message="Data fetched successfully",
        #         data={
        #             "job_id": completed_job.job_id,
        #             "status": completed_job.status.value.upper(),
        #             "tables_requested": tables,
        #             "result": completed_job.result_metadata
        #         }
        #     )
        # elif completed_job.status.value == "error":
        #     raise HTTPException(
        #         status_code=500,
        #         detail=completed_job.error_message or "Data fetch failed"
        #     )
        # else:
            # raise HTTPException(
            #     status_code=500,
            #     detail=f"Data fetch job ended with status: {completed_job.status.value}"
            # )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/copy-sample-data", response_model=ApiResponse)
async def copy_sample_data(
    req: CopySampleDataRequest,
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Copy sample data to session folder (for sample_data connector type).
    
    This is wrapped with Job Framework for consistent tracking.
    
    Request body:
        - user_id: User ID
        - session_id: Session ID
        - params: Object containing "filename" (name of the sample .pkl file to copy)
    
    Returns:
        job_id: ID to poll for status at GET /api/v2/jobs/{job_id}
    """
    try:
        params = req.params or {}
        filename = params.get("filename", "").strip()
        
        if not filename:
            raise HTTPException(status_code=400, detail="Missing required 'filename' in params")
        
        # Create a ConnectorActionRequest internally for compatibility with manager
        action_req = ConnectorActionRequest(
            user_id=req.user_id,
            session_id=req.session_id,
            connector_type="sample_data",
            params={"filename": filename}
        )
        
        # Define handler
        async def copy_sample_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for copying sample data"""
            await ctx.create_milestone(
                f"Copy sample: Copying {filename}",
                {"file": filename, "dependency": "sequential", "is_llm_call": False}
            )
            
            ctx.check_cancellation()
            
            result = await connector_manager._copy_sample_data(action_req)
            
            if result.get("result") == "success":
                await ctx.create_milestone(
                    f"Copy sample: Saved {filename}",
                    {"file": filename, "dependency": "sequential", "is_llm_call": False}
                )
                
                # Fetch sample questions from constants and update session with questions and label
                try:
                    deep_questions, simple_questions = _get_suggested_questions_for_filename(filename)
                    
                    # Sanitize filename for label: remove .pkl extension and replace underscores with spaces
                    sanitized_filename = filename.replace('.pkl', '').replace('_', ' ')
                    
                    # Update session with questions and label
                    update_data = {
                        "suggested_questions": deep_questions,
                        "suggested_questions_simple": simple_questions,
                        "label": f"Sample Data - {sanitized_filename}"
                    }
                    
                    await session_manager._update_session_fields(req.session_id, update_data)
                    add_log(f"Successfully updated session {req.session_id} with label 'Sample Data - {sanitized_filename}' and {len(deep_questions)} deep questions and {len(simple_questions)} simple questions")
                except Exception as e:
                    # Log error but don't fail the job if question saving fails
                    add_log(f"Warning: Failed to save suggested questions and label to session: {str(e)} | traceback: {traceback.format_exc()}")
                
                return {
                    "filename": filename,
                    "status": "success",
                    "data": result.get("data")
                }
            else:
                return {
                    "filename": filename,
                    "status": "failed",
                    "error": result.get("message")
                }
        
        # Create job with handler - execution starts automatically!
        # Store original string IDs in job_config for Firestore lookups
        # user_id and session_id are now always strings (UUIDs)
        job_data = JobCreate(
            user_id=req.user_id,  # user_id is now always a string (UUID)
            session_id=req.session_id,  # session_id is now always a string (UUID)
            job_type="copy_sample_data",
            label=f"Copy sample data: {filename}",
            job_config={
                "filename": filename,
                "_original_user_id": req.user_id,  # Store original string for Firestore
                "_original_session_id": req.session_id  # Store original string for Firestore
            }
        )
        
        # Update session status to "in_progress" when data loading job starts
        try:
            await session_manager._update_session_status(req.session_id, "in_progress")
        except Exception as status_error:
            add_log(f"Warning: Failed to update session status to 'in_progress': {str(status_error)}")
        
        # Create job and start execution
        job = await job_manager._create_job(job_data, execute_func=copy_sample_handler)
        
        # Wait for job completion before returning response
        completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # Return response only after job completes
        if completed_job.status.value == "completed":
            return ApiResponse(
                status="success",
                statusCode=200,
                message="Sample data copied successfully",
                data={
                    "job_id": completed_job.job_id,
                    "status": completed_job.status.value.upper(),
                    "filename": filename,
                    "result": completed_job.result_metadata
                }
            )
        elif completed_job.status.value == "error":
            raise HTTPException(
                status_code=500,
                detail=completed_job.error_message or "Copy sample data failed"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Copy sample data job ended with status: {completed_job.status.value}"
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetch-n8n-data", response_model=ApiResponse)
async def fetch_n8n_data(
    req: FetchN8nDataRequest,
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Fetch CSV data from n8n URL and save to session folder (for n8n connector type).
    
    This is wrapped with Job Framework for consistent tracking.
    
    Request body:
        - user_id: User ID
        - connector_type: Connector type (should be "n8n")
        - session_id: Session ID
        - params: Object containing "url" (URL to download CSV from)
    
    Returns:
        job_id: ID to poll for status at GET /api/v2/jobs/{job_id}
    """
    try:
        params = req.params or {}
        url = params.get("url", "").strip()
        
        if not url:
            raise HTTPException(status_code=400, detail="Missing required 'url' in params")
        
        # Create a ConnectorActionRequest internally for compatibility with manager
        action_req = ConnectorActionRequest(
            user_id=req.user_id,
            session_id=req.session_id,
            connector_type=req.connector_type,
            params={"url": url}
        )
        
        # Define handler
        async def fetch_n8n_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for fetching n8n data"""
            await ctx.create_milestone(
                f"N8N fetch: Downloading from URL",
                {"url": url[:80], "dependency": "sequential", "is_llm_call": False}
            )
            
            ctx.check_cancellation()
            
            # Extract original string IDs from job_config
            job_config = ctx.job_config or {}
            original_user_id = job_config.get("_original_user_id", req.user_id)
            original_session_id = job_config.get("_original_session_id", req.session_id)
            
            # Create request with original string IDs
            req_with_original_ids = ConnectorActionRequest(
                user_id=original_user_id,
                session_id=original_session_id,
                connector_type=req.connector_type,
                params={"url": url}
            )
            
            result = await connector_manager._fetch_n8n_data(req_with_original_ids)
            
            if result.get("result") == "success":
                await ctx.create_milestone(
                    f"N8N fetch: CSV saved to session",
                    {"dependency": "sequential", "is_llm_call": False}
                )
                return {
                    "url": url,
                    "status": "success",
                    "data": result.get("data")
                }
            else:
                return {
                    "url": url,
                    "status": "failed",
                    "error": result.get("message")
                }
        
        # Create job with handler - execution starts automatically!
        # Store original string IDs in job_config for Firestore lookups
        # user_id and session_id are now always strings (UUIDs)
        job_data = JobCreate(
            user_id=req.user_id,  # user_id is now always a string (UUID)
            session_id=req.session_id,  # session_id is now always a string (UUID)
            job_type="fetch_n8n_data",
            label=f"Fetch n8n data from URL",
            job_config={
                "url": url,
                "_original_user_id": req.user_id,  # Store original string for Firestore
                "_original_session_id": req.session_id  # Store original string for Firestore
            }
        )
        
        # Update session status to "in_progress" when data loading job starts
        try:
            await session_manager._update_session_status(req.session_id, "in_progress")
        except Exception as status_error:
            add_log(f"Warning: Failed to update session status to 'in_progress': {str(status_error)}")
        
        # Create job and start execution
        job = await job_manager._create_job(job_data, execute_func=fetch_n8n_handler)
        
        # Wait for job completion before returning response
        completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # Return response only after job completes
        if completed_job.status.value == "completed":
            return ApiResponse(
                status="success",
                statusCode=200,
                message="N8N data fetched successfully",
                data={
                    "job_id": completed_job.job_id,
                    "status": completed_job.status.value.upper(),
                    "url": url,
                    "result": completed_job.result_metadata
                }
            )
        elif completed_job.status.value == "error":
            raise HTTPException(
                status_code=500,
                detail=completed_job.error_message or "N8N data fetch failed"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"N8N data fetch job ended with status: {completed_job.status.value}"
            )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        add_log(f"ConnectorController.fetch_n8n_data error: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/health", response_model=ApiResponse)
async def check_health(
    req: ConnectorActionRequest,
    manager: ConnectorManager = Depends(get_connector_manager)
):
    """
    Check if the connector for a session is healthy.
    """
    try:
        healthy = await manager.health(req.session_id)
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Health check completed",
            data={"healthy": healthy}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/disconnect", response_model=ApiResponse)
async def disconnect(
    req: ConnectorActionRequest,
    manager: ConnectorManager = Depends(get_connector_manager)
):
    """
    Disconnect the connector for a session.
    """
    try:
        success = await manager._disconnect(req.session_id)
        return ApiResponse(
            status="success" if success else "error",
            statusCode=200,
            message="Disconnected successfully" if success else "Disconnect failed",
            data={"disconnected": success}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload-file", response_model=ApiResponse)
async def upload_file(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    session_id: str = Form(...),
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Upload and validate a CSV/Excel file.
    
    This is a long-running operation wrapped with Job Framework for:
    - Progress tracking via milestones
    - Cancellation support
    - Status polling
    
    Content-Type: multipart/form-data
    
    Form fields:
        - file: The CSV/Excel file to upload
        - user_id: User identifier
        - session_id: Session identifier
    
    Returns:
        job_id: ID to poll for status at GET /api/v2/jobs/{job_id}
        milestones: Poll at GET /api/v2/jobs/{job_id}/milestones
    """
    try:
        # Validate required form fields
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        
        filename = file.filename or "uploaded_file"
        
        # Read file content (we need to do this before creating the job 
        # because UploadFile is a stream that can only be read once)
        file_content = await file.read()
        file_content_type = file.content_type
        
        # Define handler
        async def file_upload_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for file upload with progress tracking"""
            ctx.check_cancellation()
            await ctx.create_milestone(
                f"File upload: Validating format and headers",
                {"file": filename, "dependency": "sequential", "is_llm_call": False}
            )
            
            # Create a temporary file-like object for the connector
            from io import BytesIO
            
            # Create a mock UploadFile for the connector
            class MockUploadFile:
                def __init__(self, content: bytes, fname: str, ctype: str):
                    self._content = content
                    self.filename = fname
                    self.content_type = ctype
                    self._stream = BytesIO(content)
                
                async def read(self):
                    return self._content
                
                async def seek(self, pos):
                    self._stream.seek(pos)
            
            mock_file = MockUploadFile(file_content, filename, file_content_type)
            
            ctx.check_cancellation()
            await ctx.create_milestone(
                f"File upload: Converting to DataFrame",
                {"file": filename, "dependency": "sequential", "is_llm_call": False}
            )
            
            # Call connector manager
            result = await connector_manager._upload_file(
                file=mock_file,
                user_id=user_id,
                session_id=session_id
            )
            
            if result.get("result") == "success":
                await ctx.create_milestone(
                    f"File upload: Saved {filename}.pkl",
                    {"file": filename, "dependency": "sequential", "is_llm_call": False}
                )
                return {
                    "filename": filename,
                    "status": "success",
                    "data": result.get("data")
                }
            else:
                return {
                    "filename": filename,
                    "status": "failed",
                    "error": result.get("message")
                }
        
        # Create job with handler - execution starts automatically!
        # Store original string IDs in job_config for Firestore lookups
        # user_id and session_id are now always strings (UUIDs)
        job_data = JobCreate(
            user_id=user_id,  # user_id is now always a string (UUID)
            session_id=session_id,  # session_id is now always a string (UUID)
            job_type="file_upload",
            label=f"Upload file: {filename}",
            job_config={
                "filename": filename,
                "content_type": file_content_type,
                "_original_user_id": user_id,  # Store original string for Firestore
                "_original_session_id": session_id  # Store original string for Firestore
            }
        )
        
        # Update session status to "in_progress" when data loading job starts
        try:
            await session_manager._update_session_status(session_id, "in_progress")
        except Exception as status_error:
            add_log(f"Warning: Failed to update session status to 'in_progress': {str(status_error)}")
        
        # Create job and start execution
        job = await job_manager._create_job(job_data, execute_func=file_upload_handler)
        
        # Wait for job completion before returning response
        completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # Return response only after job completes
        if completed_job.status.value == "completed":
            return ApiResponse(
                status="success",
                statusCode=200,
                message="File uploaded successfully",
                data={
                    "job_id": completed_job.job_id,
                    "status": completed_job.status.value.upper(),
                    "filename": filename,
                    "result": completed_job.result_metadata
                }
            )
        elif completed_job.status.value == "error":
            raise HTTPException(
                status_code=500,
                detail=completed_job.error_message or "File upload failed"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"File upload job ended with status: {completed_job.status.value}"
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/salesforce/subject-areas", response_model=ApiResponse)
async def get_salesforce_subject_areas():
    """
    Get available Salesforce subject areas.
    
    Returns:
        List of Salesforce subject areas with their configurations
    """
    try:
        from v2.utils.env import init_env
        
        constants = init_env()
        subject_areas = constants.get('salesforce_subject_areas', [])
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Salesforce subject areas retrieved successfully",
            data={"subject_areas": subject_areas}
        )
    except Exception as e:
        add_log(f"ConnectorController: Error fetching Salesforce subject areas: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to get Salesforce subject areas: {str(e)}")


@router.get("/acumatica/subject-areas", response_model=ApiResponse)
async def get_acumatica_subject_areas():
    """
    Get available Acumatica subject areas.
    
    Returns:
        List of Acumatica subject areas with their configurations
    """
    try:
        from v2.utils.env import init_env
        
        constants = init_env()
        subject_areas = constants.get('acumatica_subject_areas', [])
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Acumatica subject areas retrieved successfully",
            data={"subject_areas": subject_areas}
        )
    except Exception as e:
        add_log(f"ConnectorController: Error fetching Acumatica subject areas: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to get Acumatica subject areas: {str(e)}")
