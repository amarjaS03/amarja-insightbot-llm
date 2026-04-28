"""
Domain controller for FastAPI v2

Provides REST API endpoints for domain dictionary operations:
- create_domain: Creates an empty domain structure
- generate_domain: Generates domain dictionary using LLM (long-running job)
- save_domain: Saves domain dictionary to GCS/local storage (long-running job)

ARCHITECTURE:
Long-running operations (generate_domain, save_domain) are wrapped with 
the Job Framework for progress tracking, cancellation, and status polling.
"""
from typing import Dict, Any, Union, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import json
import os
import re
import math
import numbers
import asyncio
import io
from pathlib import Path
import pandas as pd

from execution_layer.utils.llm_core import initialize_llm
from v2.common.model.api_response import ApiResponse
from v2.modules.job_framework.manager.job.job_manager import JobManager
from v2.modules.job_framework.models.job_model import JobCreate
from v2.modules.job_framework.manager.job.job import JobContext
from v2.modules.session_framework.manager.session.session_manager import SessionManager
from v2.common.logger import add_log
from v2.utils.env import init_env
from v2.common.gcp import GcpManager

router = APIRouter(prefix="/domain")


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


def _get_project_root() -> Path:
    """Get project root directory"""
    return Path(__file__).resolve().parents[5]


def _get_input_data_dir(session_id: str) -> Path:
    """Get input data directory for a session"""
    project_root = _get_project_root()
    return project_root / "execution_layer" / "input_data" / session_id


def _get_domain_file_paths(session_id: str) -> List[Path]:
    """Get domain directory file path (standardized on domain_directory.json)"""
    input_data_dir = _get_input_data_dir(session_id)
    return [input_data_dir / "domain_directory.json"]


# Character mapping for pseudonymization (same behavior as v1).
_MAPPER: Dict[str, str] = {
    'A': 'Q', 'B': 'W', 'C': 'E', 'D': 'R', 'E': 'T',
    'F': 'Y', 'G': 'U', 'H': 'I', 'I': 'O', 'J': 'P',
    'K': 'A', 'L': 'S', 'M': 'D', 'N': 'F', 'O': 'G',
    'P': 'H', 'Q': 'J', 'R': 'K', 'S': 'L', 'T': 'Z',
    'U': 'X', 'V': 'C', 'W': 'V', 'X': 'B', 'Y': 'N', 'Z': 'M',
    '0': '5', '1': '6', '2': '7', '3': '8', '4': '9',
    '5': '0', '6': '1', '7': '2', '8': '3', '9': '4',
    ' ': '_', '"': '!', "'": '@',
}
_MAPPER_EXTENDED: Dict[str, str] = {
    **_MAPPER,
    **{k.lower(): v for k, v in _MAPPER.items() if isinstance(k, str) and len(k) == 1 and k.isalpha()},
}
_TRANSLATION_TABLE = str.maketrans(_MAPPER_EXTENDED)


def _pseudonymize_value(value: Any) -> Any:
    """Type-safe pseudonymization for str/int/float/bool/null and generic objects."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return value
    except Exception:
        pass
    if isinstance(value, bool):
        return value

    def _translate_text(s: str) -> str:
        s2 = s.upper()
        if s2 == "":
            return s2
        return s2.translate(_TRANSLATION_TABLE)

    if isinstance(value, str):
        return _translate_text(value)

    if isinstance(value, numbers.Integral):
        mapped = _translate_text(str(value))
        try:
            return int(mapped)
        except Exception:
            return mapped

    if isinstance(value, numbers.Real):
        try:
            if not math.isfinite(float(value)):
                return value
        except Exception:
            pass
        mapped = _translate_text(repr(value))
        try:
            return float(mapped)
        except Exception:
            return mapped

    try:
        return _translate_text(str(value))
    except Exception:
        return value


def _load_existing_domain(session_id: str) -> Optional[Dict[str, Any]]:
    """Load existing domain dictionary from disk"""
    for path in _get_domain_file_paths(session_id):
        try:
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            continue
    return None


# Request Models
class CreateDomainRequest(BaseModel):
    """Simple request to create empty domain structure"""
    session_id: str = Field(..., description="Session ID")
    domain: Optional[str] = Field(None, description="Domain description (optional)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
                "domain": "NCR ride booking data"
            }
        }


class GenerateDomainRequest(BaseModel):
    """
    Simplified request model matching V1 structure.
    
    The data_set_files dict uses filename keys (with .pkl extension) and contains:
    - file_info: Description of the file
    - underlying_conditions_about_dataset: List of business rules
    - anom_cols: Optional list of anomaly columns (for future use)
    """
    user_id: str = Field(..., description="User ID")
    session_id: str = Field(..., description="Session ID")
    domain: str = Field(..., description="Domain description")
    data_set_files: Dict[str, Dict[str, Any]] = Field(..., description="File mapping with metadata. Keys are filenames (e.g., 'entity_configs.pkl')")
    existing_domain_dictionary: Optional[Dict[str, Any]] = Field(None, description="Existing domain dictionary to merge")
    
    class Config:
        json_schema_extra = {
            "example": {
                "domain": "NCR ride booking data",
                "data_set_files": {
                    "entity_configs.pkl": {
                        "file_info": "NCR ride booking data",
                        "underlying_conditions_about_dataset": [
                            "NCR ride booking data"
                        ],
                        "anom_cols": []
                    }
                },
                "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
                "user_id": "samarth.mali@zingworks.co"
            }
        }


class SaveDomainRequest(BaseModel):
    """Simple request to save domain dictionary - matches V1 structure"""
    session_id: str = Field(..., description="Session ID")
    domain_dictionary: Dict[str, Any] = Field(..., description="Domain dictionary to save")
    
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
                "domain_dictionary": {
                    "domain": "NCR ride booking data",
                    "data_set_files": {
                        "entity_configs.pkl": {
                            "description": "NCR ride booking data",
                            "columns": [
                                {
                                    "name": "id",
                                    "description": "Unique identifier",
                                    "dtype": "int64"
                                }
                            ],
                            "underlying_conditions_about_dataset": [
                                "NCR ride booking data"
                            ]
                        }
                    }
                }
            }
        }


@router.post("/create", response_model=ApiResponse)
async def create_domain(
    req: CreateDomainRequest,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Create an empty domain structure for a session.
    
    If domain dictionary already exists, returns it.
    Otherwise, creates an empty structure.
    
    **Example Request Body:**
    ```json
    {
        "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
        "domain": "NCR ride booking data"
    }
    ```
    """
    try:
        # Check if domain already exists
        existing = _load_existing_domain(req.session_id)
        if existing:
            return ApiResponse(
                status="success",
                statusCode=200,
                message="Domain dictionary already exists",
                data={"domain_dictionary": existing, "created": False}
            )
        
        # Create empty domain structure
        input_data_dir = _get_input_data_dir(req.session_id)
        input_data_dir.mkdir(parents=True, exist_ok=True)
        
        domain_dict = {
            "domain": req.domain or "",
            "data_set_files": {}
        }
        
        domain_file_path = input_data_dir / "domain_directory.json"
        with open(domain_file_path, 'w', encoding='utf-8') as f:
            json.dump(domain_dict, f, indent=2, ensure_ascii=False)
        
        add_log(f"DomainController: Created empty domain structure for session {req.session_id}")
        
        return ApiResponse(
            status="success",
            statusCode=200,
            message="Domain structure created successfully",
            data={"domain_dictionary": domain_dict, "created": True, "file_path": str(domain_file_path)}
        )
    except Exception as e:
        add_log(f"DomainController: Error creating domain: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create domain: {str(e)}")


@router.post("/generate", response_model=ApiResponse)
async def generate_domain(
    req: GenerateDomainRequest,
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Generate domain dictionary using LLM.
    
    This is a long-running operation that:
    1. Reads PKL files from session input_data
    2. Analyzes column structure and data
    3. Uses OpenAI to generate domain dictionary
    4. Returns generated dictionary
    
    Execution happens as a background job.
    
    **Example Request Body:**
    ```json
    {
        "domain": "NCR ride booking data",
        "data_set_files": {
            "entity_configs.pkl": {
                "file_info": "NCR ride booking data",
                "underlying_conditions_about_dataset": [
                    "NCR ride booking data"
                ],
                "anom_cols": []
            }
        },
        "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
        "user_id": "samarth.mali@zingworks.co"
    }
    ```
    """
    try:
        # Validate inputs
        if not req.domain and not req.existing_domain_dictionary:
            raise HTTPException(status_code=400, detail="Missing field: domain")
        
        if not req.data_set_files:
            raise HTTPException(status_code=400, detail="Missing field: data_set_files")
        
        # Extract files to process from data_set_files (simplified V1-style)
        filenames: List[str] = []
        underlying_map: Dict[str, List[str]] = {}
        file_info_map: Dict[str, str] = {}
        pseudonymized_cols_map: Dict[str, List[str]] = {}
        
        for key, val in req.data_set_files.items():
            if not isinstance(val, dict):
                continue
            # Key is the filename (may include .pkl extension)
            filenames.append(key)
            # Extract file_info
            finfo = (val.get('file_info') or '').strip()
            if finfo:
                file_info_map[key] = finfo
            # Extract underlying_conditions_about_dataset
            ulist = val.get('underlying_conditions_about_dataset')
            if isinstance(ulist, list):
                underlying_map[key] = [str(x).strip() for x in ulist if str(x).strip()]
            # Extract pseudonymized columns selected from frontend
            anom_cols = val.get('anom_cols')
            if isinstance(anom_cols, list):
                cleaned = []
                for c in anom_cols:
                    cs = str(c).strip()
                    if cs:
                        cleaned.append(cs)
                seen = set()
                pseudonymized_cols_map[key] = [x for x in cleaned if not (x in seen or seen.add(x))]
        
        if not filenames:
            raise HTTPException(status_code=400, detail="No valid files provided in data_set_files")

        # Token check: ensure user has sufficient tokens before starting
        from v2.modules.utility.services.token_service import check_can_proceed, add_used_tokens
        user_id_str = req.user_id or ""
        can_proceed, token_msg = await check_can_proceed(user_id_str, "domain_generation")
        if not can_proceed:
            raise HTTPException(status_code=402, detail=token_msg)
        add_log(f"[DomainController] Token check passed: {token_msg}")
        
        # Define inline handler (this is the actual work)
        async def generate_domain_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for domain generation"""
            # Use original string IDs from job_config for GCS paths
            job_config = ctx.job_config or {}
            session_id_str = job_config.get("_original_session_id") or str(ctx.session_id)
            user_id_str = job_config.get("_original_user_id") or str(ctx.user_id)
            input_data_dir = _get_input_data_dir(session_id_str)
            
            await ctx.create_milestone(
                f"Domain: Starting generation for {len(filenames)} file(s)",
                {"file_count": len(filenames), "dependency": "sequential", "is_llm_call": False}
            )
            
            # Validate that required data files exist (dependency check)
            # Check both GCS and local storage (files may be in either location)
            missing_files = []
            
            for filename_key in filenames:
                # Handle filename: remove .pkl extension if present (for file lookup)
                if filename_key.endswith('.pkl'):
                    base_filename = filename_key[:-4]
                else:
                    base_filename = filename_key
                
                pkl_filename = f"{base_filename}.pkl"
                file_found = False
                
                # Check GCS first (primary storage location)
                try:
                    gcp_manager = GcpManager._get_instance()
                    storage_service = gcp_manager._storage_service
                    constants = init_env()
                    bucket_name = constants.get('storage_bucket')
                    
                    if bucket_name:
                        gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/{pkl_filename}"
                        add_log(f"DomainController: Checking if file {pkl_filename} exists in GCS: {gcs_blob_path}")
                        # Check if file exists in GCS first (more efficient than downloading)
                        file_exists = await asyncio.to_thread(
                            storage_service._file_exists,
                            bucket_name,
                            gcs_blob_path
                        )
                        if file_exists:
                            file_found = True
                            add_log(f"DomainController: Found {pkl_filename} in GCS")
                        else:
                            add_log(f"DomainController: File {pkl_filename} not found in GCS, checking local...")
                except Exception as gcs_check_err:
                    add_log(f"DomainController: Warning - GCS check failed: {str(gcs_check_err)}")
                
                # Check local file system (fallback)
                if not file_found:
                    pkl_path = input_data_dir / pkl_filename
                    if pkl_path.exists():
                        file_found = True
                        add_log(f"DomainController: Found {pkl_filename} locally")
                
                if not file_found:
                    missing_files.append(pkl_filename)
            
            if missing_files:
                error_msg = f"Required data files not found: {', '.join(missing_files)}. Please ensure data loading jobs (upload/copy/fetch) have completed successfully before generating domain dictionary."
                await ctx.create_milestone(f"Domain: Error: {error_msg}", {"dependency": "sequential", "is_llm_call": False})
                raise ValueError(error_msg)
            
            # Load existing domain dictionary if exists
            existing_dict = req.existing_domain_dictionary
            if existing_dict is None:
                existing_dict = _load_existing_domain(session_id_str)
            
            # Initialize result structure
            if existing_dict and isinstance(existing_dict, dict):
                result = existing_dict.copy()
                if 'data_set_files' not in result or not isinstance(result['data_set_files'], dict):
                    result['data_set_files'] = {}
                if 'domain' not in result and req.domain:
                    result['domain'] = req.domain
            else:
                result = {
                    'domain': req.domain,
                    'data_set_files': {}
                }
            
            # Utility to parse JSON robustly
            def parse_json_strict_or_fallback(text: str) -> Dict[str, Any]:
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    fenced_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
                    candidate = None
                    if fenced_match:
                        candidate = fenced_match.group(1).strip()
                    else:
                        start = text.find('{')
                        end = text.rfind('}')
                        if start != -1 and end != -1 and end > start:
                            candidate = text[start:end+1]
                    if candidate is None:
                        raise json.JSONDecodeError("No JSON object found in response content", text, 0)
                    return json.loads(candidate)
            
            llm = initialize_llm()
            total_tokens_used = 0

            # Process each file
            for idx, filename_key in enumerate(filenames):
                ctx.check_cancellation()
                
                # Handle filename: remove .pkl extension if present (for file lookup)
                # But keep original key for data_set_files structure
                if filename_key.endswith('.pkl'):
                    base_filename = filename_key[:-4]  # Remove .pkl extension
                else:
                    base_filename = filename_key
                
                pkl_filename = f"{base_filename}.pkl"
                df = None
                storage_service = None
                bucket_name = ""
                
                # Try to load from GCS first (primary storage)
                try:
                    gcp_manager = GcpManager._get_instance()
                    storage_service = gcp_manager._storage_service
                    constants = init_env()
                    bucket_name = constants.get('storage_bucket')
                    
                    if bucket_name:
                        gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/{pkl_filename}"
                        try:
                            # Download from GCS
                            file_content = await asyncio.to_thread(
                                storage_service._download_bytes,
                                bucket_name,
                                gcs_blob_path
                            )
                            if file_content:
                                # Load DataFrame from bytes
                                pkl_buffer = io.BytesIO(file_content)
                                df = await asyncio.to_thread(pd.read_pickle, pkl_buffer)
                                add_log(f"DomainController: Loaded {pkl_filename} from GCS")
                        except Exception as gcs_err:
                            add_log(f"DomainController: GCS load failed for {pkl_filename}, trying local: {str(gcs_err)}")
                except Exception as gcs_check_err:
                    add_log(f"DomainController: Warning - GCS check failed: {str(gcs_check_err)}")
                
                # Fallback to local file system
                if df is None:
                    pkl_path = input_data_dir / pkl_filename
                    if not pkl_path.exists():
                        raise ValueError(f'Saved file not found: {pkl_filename}. Please ensure data loading jobs have completed successfully.')
                    
                    # Load DataFrame from local file
                    try:
                        df = await asyncio.to_thread(pd.read_pickle, str(pkl_path))
                        add_log(f"DomainController: Loaded {pkl_filename} from local storage")
                    except Exception as e:
                        raise ValueError(f'Failed to load saved file {pkl_filename}: {str(e)}')

                await ctx.create_milestone(
                    f"Domain: Loaded {pkl_filename}",
                    {"file": pkl_filename, "dependency": "sequential", "is_llm_call": False}
                )

                # Apply pseudonymization to selected columns and persist updated PKL.
                requested_cols = pseudonymized_cols_map.get(filename_key, [])
                cols_to_apply = [c for c in requested_cols if c in df.columns]
                if cols_to_apply:
                    df = df.copy()
                    for col_name in cols_to_apply:
                        df[col_name] = df[col_name].apply(_pseudonymize_value)
                    add_log(f"DomainController: Applied pseudonymization to {pkl_filename} columns: {cols_to_apply}")
                    await ctx.create_milestone(
                        f"Domain: Pseudonymized {pkl_filename}",
                        {"file": pkl_filename, "dependency": "sequential", "is_llm_call": False}
                    )

                    # Persist pseudonymized PKL back to storage.
                    pkl_buffer = io.BytesIO()
                    await asyncio.to_thread(df.to_pickle, pkl_buffer)
                    pkl_bytes = pkl_buffer.getvalue()

                    try:
                        if bucket_name:
                            gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/{pkl_filename}"
                            await asyncio.to_thread(
                                storage_service._upload_bytes,
                                bucket_name,
                                pkl_bytes,
                                gcs_blob_path,
                                "application/octet-stream",
                            )
                            add_log(f"DomainController: Saved pseudonymized {pkl_filename} to GCS")
                        else:
                            local_pkl_path = input_data_dir / pkl_filename
                            await asyncio.to_thread(local_pkl_path.write_bytes, pkl_bytes)
                            add_log(f"DomainController: Saved pseudonymized {pkl_filename} locally")
                    except Exception as persist_err:
                        add_log(f"DomainController: Warning - Failed to persist pseudonymized {pkl_filename}: {str(persist_err)}")
                
                rows, cols = df.shape
                
                # Analyze columns
                columns_info = []
                for col in df.columns:
                    dtype_str = str(df[col].dtype)
                    unique_values = df[col].dropna().unique()
                    if len(unique_values) > 10:
                        unique_sample = unique_values[:10].tolist()
                        unique_count = len(unique_values)
                    else:
                        unique_sample = unique_values.tolist()
                        unique_count = len(unique_values)
                    sample_values = df[col].dropna().head(3).tolist()
                    columns_info.append({
                        'name': str(col),
                        'dtype': dtype_str,
                        'sample_values': [str(v) for v in sample_values],
                        'unique_values': [str(v) for v in unique_sample],
                        'unique_count': unique_count,
                        'null_count': int(df[col].isnull().sum())
                    })
                
                # Get file-specific metadata from data_set_files
                this_file_info = file_info_map.get(filename_key, '').strip()
                # Get underlying conditions for this specific file
                underlying_list = underlying_map.get(filename_key, [])
                
                is_first = (len(result.get('data_set_files', {})) == 0)
                
                # Build prompt
                if is_first:
                    prompt = f"""You are a senior data analyst. Produce a high-quality, business-friendly domain dictionary from the dataset below.
                    Domain: {req.domain}
                    File: {filename_key}
                    File Info: {this_file_info}
                    Shape: {rows} rows, {cols} columns

                    Detailed Column Analysis (JSON):
                    {json.dumps(columns_info, indent=2)}

                    Business Rules (list): {underlying_list}

                    Authoring guidelines (critical):
                    - Write specific, business-meaningful descriptions. Do NOT start with 'Column' or repeat the column name verbatim.
                    - Use evidence from dtype, unique_values, sample_values and null_count to infer meaning.
                    - IDs: state the entity the ID refers to and whether it is unique per row.
                    - Categorical fields: mention representative categories (3–8) from unique_values and what they indicate.
                    - Dates/times: state which event they mark (e.g., creation, pickup, drop), observed format, and timezone if inferable.
                    - Numeric measures: include unit/currency/scale (e.g., km, minutes, INR/USD, 1–5).
                    - If uncertain, use cautious wording like 'appears to' based on data; avoid speculation beyond provided context.
                    - Keep each column description 1–2 sentences.

                    Return ONLY a JSON object with EXACTLY this structure:
                    {{
                    "domain": "1–2 sentence dataset-level description using the domain context",
                    "data_set_files": {{
                        "{filename_key}": {{
                        "description": "1–2 sentence file-level description summarizing the contents and purpose",
                        "columns": [
                            {{"name": "column_name", "description": "precise, context-aware description (no 'Column …')", "dtype": "data_type"}}
                        ],
                        "underlying_conditions_about_dataset": ["rule1", "rule2"]
                        }}
                    }}
                    }}"""
                else:
                    prompt = f"""You are a senior data analyst. Generate ONLY the JSON for this file's dictionary entry.
                    File: {filename_key}
                    File Info: {this_file_info}
                    Shape: {rows} rows, {cols} columns

                    Detailed Column Analysis (JSON):
                    {json.dumps(columns_info, indent=2)}

                    Business Rules (list): {underlying_list}

                    Authoring guidelines (critical):
                    - Write specific, business-meaningful descriptions. Do NOT start with 'Column' or repeat the column name verbatim.
                    - Use evidence from dtype, unique_values, sample_values and null_count to infer meaning.
                    - IDs: state the entity the ID refers to and whether it is unique per row.
                    - Categorical fields: mention representative categories (3–8) from unique_values and what they indicate.
                    - Dates/times: state which event they mark, observed format, and timezone if inferable.
                    - Numeric measures: include unit/currency/scale; ratings must include scale bounds.
                    - If uncertain, use cautious wording; avoid speculation beyond provided context.
                    - Keep each column description 1–2 sentences.

                    Return ONLY a JSON object with EXACTLY this structure:
                    {{
                    "file": "{filename_key}",
                    "description": "1–2 sentence file-level description summarizing contents and purpose",
                    "columns": [
                        {{"name": "column_name", "description": "precise, context-aware description (no 'Column …')", "dtype": "data_type"}}
                    ],
                    "underlying_conditions_about_dataset": ["rule1", "rule2"]
                    }}"""
                
                # Call OpenAI (sequential: one file at a time)
                payload = {
                    "messages": [
                        {"role": "system", "content": "You are a senior data analyst. Create concise, accurate domain dictionaries."},
                        {"role": "user", "content": prompt},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.2,
                    "max_completion_tokens": 10000,
                }
                response = await asyncio.to_thread(llm, **payload)
                
                content = response.choices[0].message.content or ""
                usage = getattr(response, "usage", None)
                if usage:
                    total_tokens_used += getattr(usage, "total_tokens", 0) or 0
                await ctx.create_milestone(
                    f"Domain: LLM done for {filename_key}",
                    {"file": filename_key, "dependency": "sequential", "is_llm_call": True}
                )
                ai_json = parse_json_strict_or_fallback(content)
                
                # Merge result (subprocess - no LLM)
                if is_first:
                    new_domain = ai_json.get('domain') or req.domain
                    files_map = ai_json.get('data_set_files') or {}
                    if isinstance(files_map, dict):
                        result['domain'] = new_domain
                        for fname, entry in files_map.items():
                            if isinstance(entry, str):
                                entry_obj = {
                                    'description': entry,
                                    'columns': [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info],
                                    'underlying_conditions_about_dataset': underlying_list
                                }
                            else:
                                entry_obj = entry
                            if fname in pseudonymized_cols_map:
                                entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(fname, [])
                            result['data_set_files'][fname] = entry_obj
                    else:
                        entry_obj = ai_json if 'description' in ai_json else {
                            'description': this_file_info,
                            'columns': [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info],
                            'underlying_conditions_about_dataset': underlying_list
                        }
                        if filename_key in pseudonymized_cols_map:
                            entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(filename_key, [])
                        result['data_set_files'][filename_key] = entry_obj
                else:
                    if 'file' in ai_json and isinstance(ai_json.get('file'), str):
                        fname_key = ai_json['file']
                        entry_obj = {
                            'description': ai_json.get('description', this_file_info),
                            'columns': ai_json.get('columns', [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info]),
                            'underlying_conditions_about_dataset': ai_json.get('underlying_conditions_about_dataset', underlying_list)
                        }
                        if fname_key in pseudonymized_cols_map:
                            entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(fname_key, [])
                        result['data_set_files'][fname_key] = entry_obj
                    else:
                        entry_obj = {
                            'description': ai_json.get('description', this_file_info),
                            'columns': ai_json.get('columns', [{'name': c['name'], 'description': f"Column {c['name']}", 'dtype': c['dtype']} for c in columns_info]),
                            'underlying_conditions_about_dataset': ai_json.get('underlying_conditions_about_dataset', underlying_list)
                        }
                        if filename_key in pseudonymized_cols_map:
                            entry_obj['pseudonymized_columns'] = pseudonymized_cols_map.get(filename_key, [])
                        result['data_set_files'][filename_key] = entry_obj
                
                await ctx.create_milestone(
                    f"Domain: Merged result for {filename_key}",
                    {"file": filename_key, "dependency": "sequential", "is_llm_call": False}
                )
            
            await ctx.create_milestone(
                "Domain: Generation completed",
                {"dependency": "sequential", "is_llm_call": False}
            )

            # Update user tokens: add total_used to used_tokens in Firestore
            if total_tokens_used > 0 and user_id_str:
                await add_used_tokens(user_id_str, total_tokens_used)
                add_log(f"[DomainController] Added {total_tokens_used:,} used tokens for user {user_id_str}")
            
            # Save generated dictionary using canonical filename only.
            try:
                # Use original string IDs from job_config for GCS paths
                job_config = ctx.job_config or {}
                session_id_str = job_config.get("_original_session_id") or str(ctx.session_id)
                user_id_str = job_config.get("_original_user_id") or str(ctx.user_id)
                
                # Convert domain dictionary to JSON string
                domain_json_str = json.dumps(result, indent=2, ensure_ascii=False)
                domain_bytes = domain_json_str.encode('utf-8')
                
                gcs_path = None
                local_path = None
                
                # Try GCS first
                try:
                    gcp_manager = GcpManager._get_instance()
                    storage_service = gcp_manager._storage_service
                    constants = init_env()
                    bucket_name = constants.get('storage_bucket')
                    
                    if bucket_name:
                        gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/domain_directory.json"
                        
                        await asyncio.to_thread(
                            storage_service._upload_bytes,
                            bucket_name,
                            domain_bytes,
                            gcs_blob_path,
                            "application/json"
                        )
                        
                        gcs_path = f"gs://{bucket_name}/{gcs_blob_path}"
                        add_log(f"DomainController: Auto-saved generated domain to GCS: {gcs_path}")
                except Exception as gcs_err:
                    add_log(f"DomainController: GCS auto-save failed, using local fallback: {str(gcs_err)}")
                
                # Fallback to local storage
                if not gcs_path:
                    input_data_dir = _get_input_data_dir(session_id_str)
                    input_data_dir.mkdir(parents=True, exist_ok=True)
                    generated_domain_file_path = input_data_dir / "domain_directory.json"
                    
                    await asyncio.to_thread(
                        lambda: generated_domain_file_path.write_text(domain_json_str, encoding='utf-8')
                    )
                    
                    local_path = str(generated_domain_file_path)
                    add_log(f"DomainController: Auto-saved generated domain locally: {local_path}")
                
                await ctx.create_milestone(
                    "Domain: Auto-saved to storage",
                    {"dependency": "sequential", "is_llm_call": False}
                )
            except Exception as auto_save_err:
                # Log but don't fail - generation succeeded, auto-save is fallback
                add_log(f"DomainController: Warning - Auto-save failed (non-critical): {str(auto_save_err)}")
            
            return {
                "status": "success",
                "message": "generated",
                "domain_dictionary": result,
                "pseudonymized_applied": any(len(v) > 0 for v in pseudonymized_cols_map.values()),
            }
        
        # Create job with handler - execution starts automatically!
        # user_id and session_id are now always strings (UUIDs)
        job_data = JobCreate(
            user_id=req.user_id,  # user_id is now always a string (UUID)
            session_id=req.session_id,  # session_id is now always a string (UUID)
            job_type="domain_dictionary",
            label=f"Generate domain dictionary: {len(filenames)} file(s)",
            job_config={
                "data_set_files": req.data_set_files,  # Store original data_set_files structure
                "domain": req.domain,
                "_original_user_id": req.user_id,
                "_original_session_id": req.session_id
            }
        )
        
        # Create job and start execution
        job = await job_manager._create_job(job_data, execute_func=generate_domain_handler)
        
        # Wait for job completion before returning response
        completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # Return response only after job completes
        if completed_job.status.value == "completed":
            # Store generated domain dictionary in session for later retrieval
            # This allows users to retrieve it even if they don't save immediately
            try:
                result_metadata = completed_job.result_metadata or {}
                generated_domain = result_metadata.get("domain_dictionary")
                if generated_domain:
                    await session_manager._update_session_fields(
                        req.session_id,
                        {"generated_domain_dictionary": generated_domain}
                    )
                    add_log(f"DomainController: Stored generated domain dictionary in session {req.session_id}")
                result_metadata = completed_job.result_metadata or {}
                # If anom_cols (pseudonymized columns) were found and applied, set pseudonymized=true; otherwise keep false.
                if result_metadata.get("pseudonymized_applied"):
                    await session_manager._update_session_fields(
                        req.session_id,
                        {"pseudonymized": True}
                    )
                    add_log(f"DomainController: Marked session {req.session_id} as pseudonymized=true")
                else:
                    await session_manager._update_session_fields(
                        req.session_id,
                        {"pseudonymized": False}
                    )
                    add_log(f"DomainController: Marked session {req.session_id} as pseudonymized=false (no anom_cols)")
            except Exception as store_err:
                # Log but don't fail - domain generation succeeded, storage is optional
                add_log(f"DomainController: Warning - Failed to store generated domain in session: {str(store_err)}")
            
            return ApiResponse(
                status="success",
                statusCode=200,
                message="Domain dictionary generated successfully",
                data={
                    "job_id": completed_job.job_id,
                    "status": completed_job.status.value.upper(),
                    "files_requested": filenames,
                }
            )
        elif completed_job.status.value == "error":
            raise HTTPException(
                status_code=500,
                detail=completed_job.error_message or "Domain generation failed"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Domain generation job ended with status: {completed_job.status.value}"
            )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"DomainController: Error generating domain: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate domain: {str(e)}")


@router.get("/get-generated/{session_id}", response_model=ApiResponse)
async def get_generated_domain(
    session_id: str,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Get the last generated domain dictionary for a session.
    
    This retrieves the domain dictionary that was generated but not yet saved.
    Checks multiple locations:
    1. Session document field (generated_domain_dictionary) - primary source
    2. Saved file (domain_directory.json) in bucket/local storage - fallback
    
    Useful when user returns to a session after generating domain dictionary.
    
    **Returns:**
    - If generated domain exists: Returns the domain dictionary with source info
    - If not found: Returns null in data field
    """
    try:
        # Verify session exists
        session = await session_manager._get_session_by_id(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        generated_domain = None
        source = None
        
        # Method 1: Check session document field (primary source)
        try:
            from v2.common.gcp import GcpManager
            gcp_manager = GcpManager._get_instance()
            firestore_service = gcp_manager._firestore_service
            session_doc = await firestore_service._get_document("sessions", session_id)
            
            if session_doc:
                generated_domain = session_doc.get("generated_domain_dictionary")
                if generated_domain:
                    source = "session_field"
                    add_log(f"DomainController: Found generated domain in session field for {session_id}")
        except Exception as session_err:
            add_log(f"DomainController: Warning - Failed to check session field: {str(session_err)}")
        
        # Method 2: Check auto-saved file (fallback)
        if not generated_domain:
            try:
                # Try GCS first
                try:
                    user_id = session_doc.get("user_id") if session_doc else None
                    if user_id:
                        storage_service = gcp_manager._storage_service
                        constants = init_env()
                        bucket_name = constants.get('storage_bucket')
                        
                        if bucket_name:
                            # Ensure IDs are strings (defensive check)
                            user_id_str = str(user_id) if user_id else None
                            session_id_str = str(session_id) if session_id else None
                            gcs_blob_path = f"{user_id_str}/{session_id_str}/input_data/domain_directory.json"
                            
                            # Download from GCS
                            file_content = await asyncio.to_thread(
                                storage_service._download_bytes,
                                bucket_name,
                                gcs_blob_path
                            )
                            
                            if file_content:
                                generated_domain = json.loads(file_content.decode('utf-8'))
                                source = "gcs_auto_save"
                                add_log(f"DomainController: Found generated domain in GCS auto-save for {session_id}")
                except Exception as gcs_err:
                    # GCS not available or file not found, try local
                    pass
                
                # Try local file system (fallback)
                if not generated_domain:
                    generated_domain_file_path = _get_input_data_dir(session_id) / "domain_directory.json"
                    if generated_domain_file_path.exists():
                        file_content = await asyncio.to_thread(
                            lambda: generated_domain_file_path.read_text(encoding='utf-8')
                        )
                        if file_content:
                            generated_domain = json.loads(file_content)
                            source = "local_auto_save"
                            add_log(f"DomainController: Found generated domain in local auto-save for {session_id}")
            except Exception as file_err:
                add_log(f"DomainController: Warning - Failed to check auto-saved file: {str(file_err)}")
        
        if generated_domain:
            return ApiResponse(
                status="success",
                statusCode=200,
                message=f"Generated domain dictionary retrieved successfully (from {source})",
                data={"domain_dictionary": generated_domain, "source": source}
            )
        else:
            return ApiResponse(
                status="success",
                statusCode=200,
                message="No generated domain dictionary found for this session",
                data=None
            )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"DomainController: Error retrieving generated domain: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve generated domain: {str(e)}")


@router.post("/save", response_model=ApiResponse)
async def save_domain(
    req: SaveDomainRequest,
    job_manager: JobManager = Depends(get_job_manager),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Save domain dictionary to GCS/local storage.
    
    This is a long-running operation that:
    1. Validates domain dictionary structure
    2. Saves to GCS bucket (with local fallback)
    3. Returns saved file path
    
    Execution happens as a background job.
    
    **Example Request Body:**
    ```json
    {
        "session_id": "2eb23bcb-b0c2-48ca-910f-81f9c7bc6572",
        "domain_dictionary": {
            "domain": "NCR ride booking data",
            "data_set_files": {
                "entity_configs.pkl": {
                    "description": "NCR ride booking data",
                    "columns": [
                        {
                            "name": "id",
                            "description": "Unique identifier",
                            "dtype": "int64"
                        }
                    ],
                    "underlying_conditions_about_dataset": [
                        "NCR ride booking data"
                    ]
                }
            }
        }
    }
    ```
    """
    try:
        if not req.domain_dictionary:
            raise HTTPException(status_code=400, detail="Missing domain_dictionary in request")
        
        # Get user_id from session (matching V1 behavior - user_id not in request)
        session = await session_manager._get_session_by_id(req.session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Session {req.session_id} not found")
        
        user_id_str =  session.user.uid
        
        # Capture variables for closure
        domain_dict = req.domain_dictionary
        
        # Define inline handler (this is the actual work)
        async def save_domain_handler(ctx: JobContext) -> Dict[str, Any]:
            """Handler for domain dictionary saving"""
            # Use original string IDs from job_config for GCS paths
            job_config = ctx.job_config or {}
            session_id_str = job_config.get("_original_session_id") or str(ctx.session_id)
            user_id_local = job_config.get("_original_user_id") or user_id_str  # Use outer-scope user_id if absent
            
            await ctx.create_milestone("Progress: Validating domain dictionary structure")
            
            # Validate structure
            if not isinstance(domain_dict, dict):
                raise ValueError("domain_dictionary must be a dictionary")
            
            # Prepare file content
            domain_json_str = json.dumps(domain_dict, indent=2, ensure_ascii=False)
            domain_bytes = domain_json_str.encode('utf-8')
            
            await ctx.create_milestone("Progress: Preparing to save domain dictionary")
            
            # Try GCS upload first
            gcs_saved = False
            gcs_path = None
            
            try:
                gcp_manager = GcpManager._get_instance()
                storage_service = gcp_manager._storage_service
                constants = init_env()
                bucket_name = constants.get('storage_bucket')
                
                if bucket_name:
                    add_log(f"DomainController: Attempting GCS upload for user_id={user_id_local}, session_id={session_id_str}")
                    
                    gcs_blob_path = f"{user_id_local}/{session_id_str}/input_data/domain_directory.json"
                    
                    await asyncio.to_thread(
                        storage_service._upload_bytes,
                        bucket_name,
                        domain_bytes,
                        gcs_blob_path,
                        "application/json"
                    )
                    
                    gcs_path = f"gs://{bucket_name}/{gcs_blob_path}"
                    gcs_saved = True
                    add_log(f"DomainController: ✅ Successfully uploaded domain_directory.json to GCS: {gcs_path}")
                else:
                    add_log(f"DomainController: ⚠️ Bucket name not configured, skipping GCS upload")
            except Exception as gcs_err:
                add_log(f"DomainController: ❌ GCS upload failed, falling back to local: {str(gcs_err)}")
            
            # Fallback to local storage
            local_path = None
            if not gcs_saved:
                input_data_dir = _get_input_data_dir(session_id_str)
                input_data_dir.mkdir(parents=True, exist_ok=True)
                domain_file_path = input_data_dir / "domain_directory.json"
                
                await asyncio.to_thread(
                    lambda: domain_file_path.write_text(domain_json_str, encoding='utf-8')
                )
                
                local_path = str(domain_file_path)
                add_log(f"DomainController: Saved domain_directory.json locally: {local_path}")
            
            await ctx.create_milestone("Progress: Domain dictionary saved successfully")
            
            return {
                "status": "success",
                "message": "Domain dictionary saved successfully",
                "file_path": gcs_path or local_path,
                "gcs_path": gcs_path,
                "local_path": local_path
            }
        
        # Create job with handler - execution starts automatically!
        # user_id and session_id are now always strings (UUIDs)
        job_data = JobCreate(
            user_id=user_id_str,  # user_id is now always a string (UUID)
            session_id=req.session_id,  # session_id is now always a string (UUID)
            job_type="domain_save",  # Changed from "domain_dictionary"
            label="Save domain dictionary",
            job_config={
                "domain_dictionary": domain_dict,
                "_original_user_id": user_id_str,
                "_original_session_id": req.session_id
            }
        )   
        
        # Create job and start execution
        job = await job_manager._create_job(job_data, execute_func=save_domain_handler)
        
        # Wait for job completion before returning response
        completed_job = await job_manager._wait_for_job_completion(job.job_id)
        
        # Return response only after job completes
        if completed_job.status.value == "completed":
            # Clear generated_domain_dictionary from session after successful save
            # (it's now saved permanently, no need to keep the temporary copy)
            try:
                await session_manager._update_session_fields(
                    req.session_id,
                    {"generated_domain_dictionary": None}
                )
                add_log(f"DomainController: Cleared generated domain dictionary from session {req.session_id} after save")
            except Exception as clear_err:
                # Log but don't fail - save succeeded, cleanup is optional
                add_log(f"DomainController: Warning - Failed to clear generated domain from session: {str(clear_err)}")
            
            return ApiResponse(
                status="success",
                statusCode=200,
                message="Domain dictionary saved successfully",
                data={
                    "job_id": completed_job.job_id,
                    "status": completed_job.status.value.upper(),
                    "result": completed_job.result_metadata
                }
            )
        elif completed_job.status.value == "error":
            raise HTTPException(
                status_code=500,
                detail=completed_job.error_message or "Domain save failed"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Domain save job ended with status: {completed_job.status.value}"
            )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"DomainController: Error saving domain: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to save domain: {str(e)}")

