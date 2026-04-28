"""
File Upload Connector - Handles CSV/Excel file uploads
"""
import asyncio
import io
import os
import re
import json
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd

from v2.modules.connector_framework.core.base_connector import (
    BaseConnector, ConnectionError, SchemaError, DataFetchError
)
from v2.common.logger import add_log
import traceback


class FileUploadConnector(BaseConnector):
    """
    File Upload Connector for CSV/Excel files.
    
    Handles user-uploaded files, validates them, and converts to PKL format.
    
    Required credentials:
        - None required (file is uploaded directly)
    
    Optional:
        - session_id: Session to save the file to
    """
    
    ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize File Upload connector.
        
        Args:
            credentials: Dict (can be empty, or contain session_id)
        """
        super().__init__(credentials)
        self._session_id: Optional[str] = credentials.get('session_id')
        self._uploaded_df: Optional[pd.DataFrame] = None
        self._uploaded_filename: Optional[str] = None
        self._validation_errors: List[str] = []
        self._validation_warnings: List[str] = []
    
    def _get_project_root(self) -> Path:
        """Get the project root directory"""
        current_file = Path(__file__).resolve()
        # v2/modules/connector_framework/connectors/file_upload_connector.py
        return current_file.parents[4]
    
    async def connect(self) -> bool:
        """
        Initialize file upload connector.
        No actual connection needed - just marks as ready to receive files.
        
        Returns:
            bool: True (always succeeds)
        """
        await asyncio.sleep(0)
        self._connected = True
        add_log("FileUploadConnector: Initialized and ready to receive files")
        return {"status": "CONNECTED"}
    
    async def test_connection(self) -> bool:
        """
        Test if connector is ready.
        
        Returns:
            bool: True if connector is initialized
        """
        await asyncio.sleep(0)
        return self._connected
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get schema from uploaded file (column info).
        
        Returns:
            Dict containing column information
            
        Raises:
            SchemaError: If no file has been uploaded
        """
        await asyncio.sleep(0)
        try:
            if self._uploaded_df is None:
                raise SchemaError("No file uploaded. Call process_file() first.")
            
            columns_info = []
            for col in self._uploaded_df.columns:
                dtype_str = str(self._uploaded_df[col].dtype)
                unique_count = self._uploaded_df[col].nunique()
                null_count = int(self._uploaded_df[col].isnull().sum())
                sample_values = self._uploaded_df[col].dropna().head(3).tolist()
                
                columns_info.append({
                    'name': str(col),
                    'dtype': dtype_str,
                    'unique_count': unique_count,
                    'null_count': null_count,
                    'sample_values': [str(v) for v in sample_values]
                })
            
            rows, cols = self._uploaded_df.shape
            
            return {
                "filename": self._uploaded_filename,
                "rows": rows,
                "columns": cols,
                "column_info": columns_info,
                "tables": [self._uploaded_filename] if self._uploaded_filename else [],
                "views": [],
                "objects": []
            }
        except Exception as e:
            add_log(f"FileUploadConnector: Schema retrieval failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Failed to get schema: {str(e)}")
    
    async def get_data(
        self, 
        table_name: str,  # Not used for file upload - returns the uploaded file
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Get the uploaded data as DataFrame.
        
        Args:
            table_name: Ignored for file upload (only one file at a time)
            limit: Optional row limit
            
        Returns:
            pd.DataFrame: The uploaded and validated data
            
        Raises:
            DataFetchError: If no file has been uploaded
        """
        await asyncio.sleep(0)
        try:
            if self._uploaded_df is None:
                raise DataFetchError("No file uploaded. Call process_file() first.")
            
            df = self._uploaded_df
            if limit:
                df = df.head(limit)
            
            return df
        except Exception as e:
            add_log(f"FileUploadConnector: Data fetch failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to get data: {str(e)}")
    
    async def process_file(
        self,
        file_content: bytes,
        filename: str,
        session_id: str
    ) -> Dict[str, Any]:
        """
        Process an uploaded file: validate and convert to DataFrame.
        
        Args:
            file_content: Raw file bytes
            filename: Original filename
            session_id: Session ID to associate with
            
        Returns:
            Dict with validation result and preview
            
        Raises:
            DataFetchError: If file processing fails
        """
        await asyncio.sleep(0)
        self._validation_errors = []
        self._validation_warnings = []
        self._session_id = session_id
        
        try:
            # Check file extension
            ext = '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
            if ext not in self.ALLOWED_EXTENSIONS:
                raise DataFetchError(f"Invalid file type '{ext}'. Allowed: {', '.join(self.ALLOWED_EXTENSIONS)}")
            
            # Save to temp file for pandas to read
            with tempfile.NamedTemporaryFile(prefix='upload_', suffix=ext, delete=False) as tmp:
                tmp_path = tmp.name
                tmp.write(file_content)
            
            try:
                # Read file based on extension
                if ext == '.csv':
                    df = pd.read_csv(tmp_path, header=0, encoding_errors='replace')
                else:
                    try:
                        df = pd.read_excel(tmp_path, header=0, engine='openpyxl')
                    except Exception:
                        df = pd.read_excel(tmp_path, header=0)
            finally:
                # Clean up temp file
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
            
            # Validate the data
            validation_result = self._validate_dataframe(df)
            
            if not validation_result['valid']:
                raise DataFetchError(validation_result['errors'][0] if validation_result['errors'] else 'Validation failed')
            
            # Store the validated DataFrame
            self._uploaded_df = df
            self._uploaded_filename = filename
            
            add_log(f"FileUploadConnector: Processed {filename} - {df.shape[0]} rows, {df.shape[1]} columns")
            
            return {
                'valid': True,
                'filename': filename,
                'rows': df.shape[0],
                'columns': df.shape[1],
                'column_names': [str(c) for c in df.columns.tolist()],
                'errors': self._validation_errors,
                'warnings': self._validation_warnings
            }
            
        except DataFetchError:
            raise
        except Exception as e:
            add_log(f"FileUploadConnector: File processing failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to process file: {str(e)}")
    
    def _validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate the DataFrame for common issues.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dict with validation result
        """
        errors: List[str] = []
        warnings: List[str] = []
        
        col_names = [str(c) for c in df.columns.tolist()]
        
        # Check for empty/unnamed columns
        if any((c.strip() == '' or c.lower().startswith('unnamed')) for c in col_names):
            errors.append('Header row has empty or Unnamed columns. Ensure the first row contains proper column names.')
        
        # Check for duplicate column names (case-insensitive)
        lowered = [c.lower() for c in col_names]
        if len(set(lowered)) != len(lowered):
            errors.append('Duplicate column names detected (case-insensitive). Column names must be unique.')
        
        # Check if first row looks like data instead of headers
        def is_numeric_like(s: str) -> bool:
            try:
                float(s.replace(',', ''))
                return True
            except Exception:
                return False
        
        numeric_like_count = sum(1 for c in col_names if is_numeric_like(c))
        if df.shape[1] > 0 and numeric_like_count >= max(1, int(0.6 * df.shape[1])):
            errors.append('First row appears to be data, not headers. Please include a header row as the first line.')
        
        # Check for JSON/blob columns
        base64_regex = re.compile(r'^[A-Za-z0-9+/=\r\n]+$')
        
        def looks_like_json(text: str) -> bool:
            text = text.strip()
            if not text or (text[0] not in '{['):
                return False
            if len(text) > 20000:
                return True
            try:
                json.loads(text)
                return True
            except Exception:
                return False
        
        def looks_like_base64_blob(text: str) -> bool:
            if len(text) < 1000:
                return False
            compact = ''.join(ch for ch in text if not ch.isspace())
            if not base64_regex.match(compact):
                return False
            return len(compact) > 5000
        
        try:
            object_cols = [c for c in df.columns if str(df[c].dtype) == 'object']
            sample_size = min(int(df.shape[0]), 1000)
            for col in object_cols:
                series = df[col].dropna().astype(str).head(sample_size)
                if series.empty:
                    continue
                very_long = (series.str.len() > 50000).any()
                json_like_ratio = series.apply(looks_like_json).mean() if len(series) > 0 else 0
                b64_like_ratio = series.apply(looks_like_base64_blob).mean() if len(series) > 0 else 0
                if very_long or json_like_ratio >= 0.2 or b64_like_ratio >= 0.2:
                    errors.append(f"Column '{col}' appears to contain non-text payloads (JSON/blob/image/base64). These are not allowed.")
        except Exception:
            pass
        
        self._validation_errors = errors
        self._validation_warnings = warnings
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    async def save_to_session(
        self,
        session_id: str,
        user_id: Optional[str] = None,
        clear_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Save the uploaded file as PKL to GCS (or local fallback).
        
        Args:
            session_id: Session ID
            user_id: User ID (required for GCS path)
            clear_existing: Whether to clear existing files first (only for local)
            
        Returns:
            Dict with saved file info and preview
        """
        await asyncio.sleep(0)
        try:
            if self._uploaded_df is None or not self._uploaded_filename:
                raise DataFetchError("No file uploaded. Call process_file() first.")
            
            # Generate PKL filename
            base_filename = self._uploaded_filename.rsplit('.', 1)[0] if '.' in self._uploaded_filename else self._uploaded_filename
            pkl_filename = f"{base_filename}.pkl"
            
            # Try GCS first
            gcs_saved = False
            gcs_path = None
            
            if user_id:
                try:
                    from v2.common.gcp import GcpManager
                    
                    gcp_manager = GcpManager._get_instance()
                    storage_service = gcp_manager._storage_service
                    
                    # Use the utility method
                    gcs_path = await storage_service.upload_pkl_to_session(
                        data=self._uploaded_df,
                        user_id=user_id,
                        session_id=session_id,
                        filename=pkl_filename
                    )
                    
                    if gcs_path:
                        gcs_saved = True
                        add_log(f"FileUploadConnector: Saved {pkl_filename} to GCS: {gcs_path}")
                except Exception as gcs_err:
                    add_log(f"FileUploadConnector: GCS save failed, falling back to local: {str(gcs_err)}")
            
            # Fallback to local storage if GCS not available
            local_path = None
            if not gcs_saved:
                project_root = self._get_project_root()
                input_data_dir = project_root / "execution_layer" / "input_data" / session_id
                
                # Clear existing data if requested
                if clear_existing and input_data_dir.exists():
                    for f in input_data_dir.iterdir():
                        if f.is_file():
                            f.unlink()
                
                os.makedirs(input_data_dir, exist_ok=True)
                
                pkl_path = input_data_dir / pkl_filename
                self._uploaded_df.to_pickle(str(pkl_path))
                local_path = str(pkl_path)
                
                add_log(f"FileUploadConnector: Saved {pkl_filename} to local: {local_path}")
            
            # Generate preview
            preview_data = self.get_preview(self._uploaded_df)
            
            result = {
                'saved_file': pkl_filename,
                'rows': self._uploaded_df.shape[0],
                'columns': self._uploaded_df.shape[1],
                'preview_data': preview_data
            }
            
            if gcs_saved:
                result['gcs_path'] = gcs_path
            else:
                result['local_path'] = local_path
            
            return result
            
        except Exception as e:
            add_log(f"FileUploadConnector: Save failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to save file: {str(e)}")
    
    async def disconnect(self) -> None:
        """Clean up file upload connector"""
        await asyncio.sleep(0)
        self._uploaded_df = None
        self._uploaded_filename = None
        self._validation_errors = []
        self._validation_warnings = []
        self._connected = False
        add_log("FileUploadConnector: Disconnected")

