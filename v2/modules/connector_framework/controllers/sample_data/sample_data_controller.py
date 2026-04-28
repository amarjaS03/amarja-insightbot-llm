"""
Sample Data Controller for FastAPI v2

Provides REST API endpoints for sample data operations:
- Get available sample datasets info
- Preview sample data and domain dictionary
- Get QnA suggestions for sample data
"""
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import json

from v2.common.model.api_response import ApiResponse
from v2.common.logger import add_log
from v2.modules.connector_framework.manager.connector.connector_manager import ConnectorManager
from v2.modules.connector_framework.connectors.sample_data_connector import SampleDataConnector
from v2.utils.env import init_env
import traceback

router = APIRouter(prefix="/sample-data")


class PreviewRequest(BaseModel):
    """Request model for preview endpoint"""
    filename: str


def get_connector_manager() -> ConnectorManager:
    """Dependency to get connector manager instance"""
    return ConnectorManager()


def _get_suggested_questions_for_filename(filename: str) -> tuple[List[str], List[str]]:
    """
    Returns a pair (deep_questions, simple_questions) for the given sample filename.
    
    - deep_questions come from 'suggested_questions'
    - simple_questions come from 'suggested_questions_simple', falling back to deep or generic defaults
    """
    fallback = [
        "Show the first 10 rows.",
        "List all column names.",
        "Show summary statistics for numeric columns.",
        "Count rows and columns in the dataset.",
        "Find top 10 frequent values for a key categorical column."
    ]
    
    try:
        constants = init_env()
        sample_info = constants.get('sample_preload_data_info') or []
        for domain in sample_info:
            datasets = domain.get('datasets') or []
            for ds in datasets:
                if str(ds.get('filename', '')).strip() == str(filename).strip():
                    deep_raw = ds.get('suggested_questions') or []
                    simple_raw = ds.get('suggested_questions_simple') or []
                    
                    deep = [str(q).strip() for q in deep_raw if str(q).strip()]
                    simple = [str(q).strip() for q in simple_raw if str(q).strip()]
                    
                    if not deep:
                        deep = fallback.copy()
                    if not simple:
                        simple = deep.copy()
                    return deep, simple
    except Exception as e:
        add_log(f"Error getting suggested questions: {str(e)}")
    
    return fallback.copy(), fallback.copy()


@router.get("/info", response_model=ApiResponse)
async def get_sample_data_info():
    """
    Get information about all available sample datasets.
    
    Returns:
        List of domains with their datasets, filtered to only show datasets
        that actually exist in the sample_data directory.
    """
    try:
        constants = init_env()
        sample_info: List[Dict[str, Any]] = constants.get('sample_preload_data_info') or []
        
        if not isinstance(sample_info, list):
            raise HTTPException(status_code=404, detail="sample_preload_data_info not found in constants")
        
        # Resolve project root and sample_data path
        project_root = Path(__file__).resolve().parents[5]  # Go up to project root
        sample_data_dir = project_root / 'sample_data'
        
        if not sample_data_dir.exists() or not sample_data_dir.is_dir():
            raise HTTPException(status_code=404, detail="sample_data directory not found")
        
        # Collect all .pkl filenames present under sample_data
        existing_pkl_filenames = {p.name for p in sample_data_dir.rglob('*.pkl')}
        
        # Filter datasets by presence in existing_pkl_filenames
        filtered_domains: List[Dict[str, Any]] = []
        for domain in sample_info:
            datasets = domain.get('datasets') or []
            filtered_datasets = [
                d for d in datasets 
                if str(d.get('filename', '')).strip() in existing_pkl_filenames
            ]
            if filtered_datasets:
                filtered_domain = {**domain, 'datasets': filtered_datasets}
                filtered_domains.append(filtered_domain)
        
        add_log(f"SampleData: Retrieved info for {len(filtered_domains)} domains")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Sample data info retrieved successfully",
            data=filtered_domains
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"SampleData: Error getting sample data info: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/preview", response_model=ApiResponse)
async def preview_sample_data(request: PreviewRequest):
    """
    Preview sample data and domain dictionary for a given filename.
    
    Args:
        request: Request body containing filename
        
    Returns:
        Preview rows from the dataset and the domain dictionary
    """
    try:
        filename = request.filename.strip()
        if not filename:
            raise HTTPException(status_code=400, detail="Missing field: filename")
        
        # Resolve project root and sample_data path
        project_root = Path(__file__).resolve().parents[5]  # Go up to project root
        sample_data_dir = project_root / 'sample_data'
        
        if not sample_data_dir.exists() or not sample_data_dir.is_dir():
            raise HTTPException(status_code=404, detail="sample_data directory not found")
        
        # Find the requested file under sample_data
        candidates = list(sample_data_dir.rglob(filename))
        if not candidates:
            raise HTTPException(status_code=404, detail=f"File not found in sample_data: {filename}")
        
        src_file = candidates[0]
        if not src_file.is_file():
            raise HTTPException(status_code=404, detail=f"Not a file: {filename}")
        
        # Determine domain directory file alongside the data file
        src_dir = src_file.parent
        domain_file = src_dir / 'domain_directory.json'
        if not domain_file.exists() or not domain_file.is_file():
            raise HTTPException(status_code=404, detail="Domain directory (domain_directory.json) not found beside the file")
        
        # Number of rows to preview from constants
        constants = init_env()
        num_rows = constants.get('sample_data_rows')
        if not isinstance(num_rows, int) or num_rows <= 0:
            num_rows = 5
        
        # Read data and prepare preview with JSON-safe types
        df = pd.read_pickle(str(src_file))
        df_preview = df.head(num_rows)
        df_preview = df_preview.where(pd.notnull(df_preview), None)
        preview_rows = json.loads(df_preview.to_json(orient='records', date_format='iso'))
        
        # Read domain dictionary JSON
        with open(domain_file, 'r', encoding='utf-8') as f:
            domain_dict = json.load(f)
        
        add_log(f"SampleData: Previewed {src_file.name} ({len(preview_rows)} rows)")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Sample data preview retrieved successfully",
            data={
                "filename": src_file.name,
                "data": preview_rows,
                "domain": domain_dict
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"SampleData: Error previewing sample data: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/qna-suggestions", response_model=ApiResponse)
async def get_sample_qna_suggestions(
    filename: Optional[str] = Query(None, description="Name of the .pkl file"),
    session_id: Optional[str] = Query(None, description="Session ID to auto-detect filename")
):
    """
    Get QnA suggestions (both deep analysis and simple QnA) for a sample dataset.
    
    Args:
        filename: Name of the .pkl file (optional if session_id provided)
        session_id: Session ID to auto-detect filename from (optional if filename provided)
        
    Returns:
        Suggested questions for deep analysis and simple QnA
    """
    try:
        project_root = Path(__file__).resolve().parents[5]  # Go up to project root
        
        # Auto-detect filename from session_id if provided
        if not filename and session_id:
            input_dir = project_root / 'execution_layer' / 'input_data' / session_id
            if not input_dir.exists() or not input_dir.is_dir():
                raise HTTPException(
                    status_code=404,
                    detail="input_data directory not found for session"
                )
            
            pkl_files = [p.name for p in input_dir.glob('*.pkl')]
            if not pkl_files:
                raise HTTPException(
                    status_code=404,
                    detail="No .pkl file found in session input_data"
                )
            filename = pkl_files[0]
        
        if not filename:
            raise HTTPException(
                status_code=400,
                detail="Provide either session_id or filename"
            )
        
        # Get suggested questions
        deep_suggestions, simple_suggestions = _get_suggested_questions_for_filename(filename)
        
        add_log(f"SampleData: Retrieved QnA suggestions for {filename}")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="QnA suggestions retrieved successfully",
            data={
                "filename": filename,
                "suggested_questions": deep_suggestions,
                "suggested_questions_simple": simple_suggestions
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"SampleData: Error getting QnA suggestions: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

