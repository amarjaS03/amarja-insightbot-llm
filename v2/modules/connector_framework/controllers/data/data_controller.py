"""
Data Controller for FastAPI v2

Provides REST API endpoints for data operations:
- Preview loaded data (top N rows) for any connector type
- Works with GCS and local storage
"""
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from pathlib import Path
import pandas as pd
import json
import io
import asyncio
import traceback

from v2.common.model.api_response import ApiResponse
from v2.common.logger import add_log
from v2.common.gcp import GcpManager
from v2.utils.env import init_env
from v2.modules.session_framework.manager.session.session_manager import SessionManager

router = APIRouter(prefix="/data")


class PreviewDataRequest(BaseModel):
    """Request model for preview data endpoint"""
    session_id: str = Field(..., description="Session identifier")
    filename: Optional[str] = Field(None, description="Specific PKL filename to preview (optional, previews all files if not provided)")
    num_rows: Optional[int] = Field(10, description="Number of rows to preview (default: 10)")
    include_domain: Optional[bool] = Field(False, description="Include domain dictionary if available")
    all_files: Optional[bool] = Field(False, description="Preview all files in session (returns multiple file format)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
                "filename": "entity_configs.pkl",
                "num_rows": 10,
                "include_domain": False,
                "all_files": False
            }
        }


def get_session_manager() -> SessionManager:
    """Dependency to get session manager instance"""
    return SessionManager()


async def _load_pkl_from_gcs(user_id: str, session_id: str, filename: str) -> Optional[pd.DataFrame]:
    """Load PKL file from GCS"""
    try:
        gcp_manager = GcpManager._get_instance()
        storage_service = gcp_manager._storage_service
        constants = init_env()
        bucket_name = constants.get('storage_bucket')
        
        if not bucket_name:
            return None
        
        # GCS path: {user_id}/{session_id}/input_data/{filename}
        # Ensure IDs are strings (defensive check)
        user_id_str = str(user_id) if user_id else None
        session_id_str = str(session_id) if session_id else None
        gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/{filename}"
        
        # Check if file exists
        if not storage_service._file_exists(bucket_name, gcs_blob_path):
            add_log(f"DataController: File not found in GCS: {gcs_blob_path}")
            return None
        
        # Download bytes from GCS
        pkl_bytes = await asyncio.to_thread(
            storage_service._download_bytes,
            bucket_name,
            gcs_blob_path
        )
        
        # Load DataFrame from bytes
        buffer = io.BytesIO(pkl_bytes)
        df = pd.read_pickle(buffer)
        
        add_log(f"DataController: Loaded {filename} from GCS ({len(df)} rows)")
        return df
    except Exception as e:
        add_log(f"DataController: Error loading from GCS: {str(e)} | traceback: {traceback.format_exc()}")
        return None


async def _load_pkl_from_local(session_id: str, filename: str) -> Optional[pd.DataFrame]:
    """Load PKL file from local storage (fallback)"""
    try:
        project_root = Path(__file__).resolve().parents[6]  # Go up to project root
        input_data_dir = project_root / 'execution_layer' / 'input_data' / session_id
        
        if not input_data_dir.exists():
            return None
        
        pkl_path = input_data_dir / filename
        if not pkl_path.exists():
            return None
        
        df = await asyncio.to_thread(pd.read_pickle, str(pkl_path))
        add_log(f"DataController: Loaded {filename} from local storage ({len(df)} rows)")
        return df
    except Exception as e:
        add_log(f"DataController: Error loading from local: {str(e)} | traceback: {traceback.format_exc()}")
        return None


async def _load_domain_from_gcs(user_id: str, session_id: str) -> Optional[Dict[str, Any]]:
    """Load domain dictionary from GCS"""
    try:
        gcp_manager = GcpManager._get_instance()
        storage_service = gcp_manager._storage_service
        constants = init_env()
        bucket_name = constants.get('storage_bucket')
        
        if not bucket_name:
            return None
        
        # Ensure IDs are strings (defensive check)
        user_id_str = str(user_id) if user_id else None
        session_id_str = str(session_id) if session_id else None
        gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/domain_directory.json"
        
        if storage_service._file_exists(bucket_name, gcs_blob_path):
            domain_bytes = await asyncio.to_thread(
                storage_service._download_bytes,
                bucket_name,
                gcs_blob_path
            )
            domain_dict = json.loads(domain_bytes.decode('utf-8'))
            add_log(f"DataController: Loaded domain directory from GCS: domain_directory.json")
            return domain_dict
        
        return None
    except Exception as e:
        add_log(f"DataController: Error loading domain from GCS: {str(e)}")
        return None


async def _load_domain_from_local(session_id: str) -> Optional[Dict[str, Any]]:
    """Load domain dictionary from local storage (fallback)"""
    try:
        project_root = Path(__file__).resolve().parents[6]
        input_data_dir = project_root / 'execution_layer' / 'input_data' / session_id
        
        if not input_data_dir.exists():
            return None
        
        domain_path = input_data_dir / "domain_directory.json"
        if domain_path.exists():
            with open(domain_path, 'r', encoding='utf-8') as f:
                domain_dict = json.load(f)
            add_log(f"DataController: Loaded domain directory from local: domain_directory.json")
            return domain_dict
        
        return None
    except Exception as e:
        add_log(f"DataController: Error loading domain from local: {str(e)}")
        return None


def _prepare_preview_rows(df: pd.DataFrame, num_rows: int) -> List[Dict[str, Any]]:
    """
    Prepare preview rows from DataFrame in V1 format.
    Converts values to strings and handles null/nan values.
    """
    try:
        top_records = df.head(num_rows).to_dict('records')
        preview_rows = []
        for record in top_records:
            string_record = {}
            for key, value in record.items():
                if value is None or pd.isna(value):
                    string_record[str(key)] = "nan"
                elif isinstance(value, (dict, list)):
                    string_record[str(key)] = str(value)
                else:
                    string_record[str(key)] = str(value)
            preview_rows.append(string_record)
        return preview_rows
    except Exception as e:
        add_log(f"DataController: Error preparing preview rows: {str(e)}")
        return []


@router.post("/preview", response_model=ApiResponse)
async def preview_data(
    request: PreviewDataRequest,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Preview loaded data for a session.
    
    This endpoint reads PKL files from the session's input_data folder (GCS or local)
    and returns the top N rows for preview purposes.
    
    Works for all connector types:
    - File Upload (CSV/Excel)
    - Sample Data
    - Database connectors (MSSQL, MySQL, etc.)
    - Salesforce, Acumatica, etc.
    
    Args:
        request: Request body with session_id, optional filename, num_rows, and include_domain
        
    Returns:
        Preview rows from the dataset (and optionally domain dictionary)
    """
    try:
        session_id = request.session_id.strip()
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        
        # Get session to retrieve user_id
        session = await session_manager._get_session_by_id(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        # UserModel uses 'uid' attribute, not 'userId'
        if not session.user or not session.user.uid:
            raise HTTPException(status_code=400, detail="Session user information not available")
        
        user_id = session.user.uid
        
        # Determine filename and preview mode
        filename = request.filename.strip() if request.filename else None
        num_rows = request.num_rows if request.num_rows and request.num_rows > 0 else 10
        preview_all = request.all_files or (filename is None)
        
        # List all available PKL files
        pkl_files = []
        try:
            # Try to list files from GCS first
            gcp_manager = GcpManager._get_instance()
            storage_service = gcp_manager._storage_service
            constants = init_env()
            bucket_name = constants.get('storage_bucket')
            
            if bucket_name:
                prefix = f"{user_id}/{session_id}/input_data/"
                files = await asyncio.to_thread(
                    storage_service._list_files,
                    bucket_name,
                    prefix
                )
                pkl_files = [f.split('/')[-1] for f in files if f.endswith('.pkl')]
        except Exception as e:
            add_log(f"DataController: Error listing files from GCS: {str(e)}")
        
        # Fallback to local if GCS didn't work or no files found
        if not pkl_files:
            try:
                project_root = Path(__file__).resolve().parents[6]
                input_data_dir = project_root / 'execution_layer' / 'input_data' / session_id
                
                if input_data_dir.exists():
                    pkl_files = [f.name for f in input_data_dir.glob('*.pkl')]
            except Exception as e:
                add_log(f"DataController: Error listing files from local: {str(e)}")
        
        if not pkl_files:
            raise HTTPException(
                status_code=404,
                detail="No PKL files found in session input_data folder"
            )
        
        # If preview_all is True or filename not specified, preview all files
        if preview_all or filename is None:
            # Multiple files format
            preview_map = {}
            saved_files = []
            
            for pkl_file in pkl_files:
                # Ensure filename ends with .pkl
                if not pkl_file.endswith('.pkl'):
                    pkl_file = f"{pkl_file}.pkl"
                
                # Load DataFrame from GCS or local
                df = await _load_pkl_from_gcs(user_id, session_id, pkl_file)
                if df is None:
                    df = await _load_pkl_from_local(session_id, pkl_file)
                
                if df is not None:
                    preview_rows = _prepare_preview_rows(df, num_rows)
                    preview_map[pkl_file] = {"top_records": preview_rows}
                    saved_files.append(pkl_file)
            
            if not preview_map:
                raise HTTPException(
                    status_code=404,
                    detail="No valid PKL files found to preview"
                )
            
            # Build response in V1 multiple files format
            response_data = {
                "preview_data": preview_map,
                "saved_files": saved_files
            }
            previewed_info = f"{len(saved_files)} files"
        else:
            # Single file format
            # Ensure filename ends with .pkl
            if not filename.endswith('.pkl'):
                filename = f"{filename}.pkl"
            
            # Load DataFrame from GCS or local
            df = await _load_pkl_from_gcs(user_id, session_id, filename)
            if df is None:
                df = await _load_pkl_from_local(session_id, filename)
            
            if df is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"File '{filename}' not found in session {session_id} input_data folder"
                )
            
            # Prepare preview rows in V1 format (top_records)
            preview_rows = _prepare_preview_rows(df, num_rows)
            
            # Build response in V1 single file format
            response_data = {
                "preview_data": {
                    "top_records": preview_rows
                },
                "saved_file": filename,
                "valid": True
            }
            previewed_info = filename
        
        # Optionally include domain dictionary
        if request.include_domain:
            domain_dict = await _load_domain_from_gcs(user_id, session_id)
            if domain_dict is None:
                domain_dict = await _load_domain_from_local(session_id)
            
            if domain_dict:
                response_data["domain"] = domain_dict
        
        # Log preview completion
        add_log(f"DataController: Previewed {previewed_info} for session {session_id}")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Preview data retrieved successfully",
            data=response_data
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"DataController: Error previewing data: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview", response_model=ApiResponse)
async def preview_data_get(
    session_id: str = Query(..., description="Session identifier"),
    filename: Optional[str] = Query(None, description="Specific PKL filename to preview"),
    num_rows: Optional[int] = Query(10, description="Number of rows to preview (default: 10)"),
    include_domain: Optional[bool] = Query(False, description="Include domain dictionary if available"),
    all_files: Optional[bool] = Query(False, description="Preview all files in session"),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Preview loaded data for a session (GET version).
    
    Same functionality as POST /preview but using query parameters.
    """
    request = PreviewDataRequest(
        session_id=session_id,
        filename=filename,
        num_rows=num_rows,
        include_domain=include_domain,
        all_files=all_files
    )
    return await preview_data(request, session_manager)

