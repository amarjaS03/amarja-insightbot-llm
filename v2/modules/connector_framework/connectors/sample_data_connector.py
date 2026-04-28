"""
Sample Data Connector - Handles pre-loaded sample datasets
"""
import asyncio
import os
import json
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd

from v2.modules.connector_framework.core.base_connector import (
    BaseConnector, ConnectionError, SchemaError, DataFetchError
)
from v2.common.logger import add_log
import traceback

from v2.modules.session_framework.manager.session.session_manager import SessionManager


class SampleDataConnector(BaseConnector):
    """
    Sample Data Connector for pre-loaded datasets.
    
    Handles copying sample .pkl files and domain dictionaries
    from the sample_data directory to session input folders.
    
    Required credentials:
        - None required (sample data is pre-loaded)
    
    Optional:
        - filename: Specific sample file to use
    """
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize Sample Data connector.
        
        Args:
            credentials: Dict (can be empty for sample data)
        """
        super().__init__(credentials)
        self._sample_data_dir: Optional[Path] = None
        self._available_datasets: List[Dict[str, Any]] = []
        self._constants: Dict[str, Any] = {}
        self._session_manager: SessionManager = SessionManager()
    
    def _get_project_root(self) -> Path:
        """Get the project root directory"""
        # Navigate from connector file to project root
        current_file = Path(__file__).resolve()
        # v2/modules/connector_framework/connectors/sample_data_connector.py
        # Go up 4 levels to get to project root
        return current_file.parents[4]
    
    def _load_constants(self) -> None:
        """Load constants from v2/utils/constants.json"""
        try:
            constants_path = self._get_project_root() / "v2" / "utils" / "constants.json"
            if constants_path.exists():
                with open(constants_path, 'r', encoding='utf-8') as f:
                    self._constants = json.load(f)
        except Exception as e:
            add_log(f"SampleDataConnector: Failed to load constants: {str(e)}")
            self._constants = {}
    
    async def connect(self) -> bool:
        """
        Initialize sample data connector.
        
        Returns:
            bool: True if initialization successful
            
        Raises:
            ConnectionError: If initialization fails
        """
        await asyncio.sleep(0)
        try:
            project_root = self._get_project_root()
            self._sample_data_dir = project_root / "sample_data"
            
            if not self._sample_data_dir.exists():
                raise ConnectionError(f"Sample data directory not found: {self._sample_data_dir}")
            
            # Load constants for dataset info
            self._load_constants()
            
            # Discover available datasets
            await self._discover_datasets()
            
            self._connected = True
            add_log(f"SampleDataConnector: Initialized with {len(self._available_datasets)} datasets")
            return {"status": "CONNECTED"}
        except Exception as e:
            self._connected = False
            add_log(f"SampleDataConnector: Initialization failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to initialize sample data connector: {str(e)}")
    
    async def _discover_datasets(self) -> None:
        """Discover available sample datasets"""
        await asyncio.sleep(0)
        try:
            self._available_datasets = []
            
            # Get dataset info from constants
            sample_info = self._constants.get('sample_preload_data_info', [])
            
            # Find all .pkl files in sample_data directory
            existing_pkl_files = set()
            if self._sample_data_dir:
                for pkl_file in self._sample_data_dir.rglob('*.pkl'):
                    existing_pkl_files.add(pkl_file.name)
            
            # Match datasets from constants with existing files
            for domain in sample_info:
                domain_name = domain.get('domain', 'Unknown')
                datasets = domain.get('datasets', [])
                
                for dataset in datasets:
                    filename = dataset.get('filename', '')
                    if filename in existing_pkl_files:
                        self._available_datasets.append({
                            'filename': filename,
                            'domain': domain_name,
                            'display_name': dataset.get('display_name', filename),
                            'description': dataset.get('description', ''),
                            'suggested_questions': dataset.get('suggested_questions', []),
                            'suggested_questions_simple': dataset.get('suggested_questions_simple', [])
                        })
            
            # Also add any .pkl files not in constants
            for pkl_name in existing_pkl_files:
                if not any(d['filename'] == pkl_name for d in self._available_datasets):
                    self._available_datasets.append({
                        'filename': pkl_name,
                        'domain': 'Other',
                        'display_name': pkl_name,
                        'description': '',
                        'suggested_questions': [],
                        'suggested_questions_simple': []
                    })
        except Exception as e:
            add_log(f"SampleDataConnector: Error discovering datasets: {str(e)}")
    
    async def test_connection(self) -> bool:
        """
        Test if the connector is working.
        
        Returns:
            bool: True if connector is healthy
        """
        await asyncio.sleep(0)
        return self._sample_data_dir is not None and self._sample_data_dir.exists()
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get available sample datasets.
        
        Returns:
            Dict containing 'datasets' list
            
        Raises:
            SchemaError: If schema retrieval fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected:
                raise SchemaError("Sample data connector not initialized")
            
            # Refresh dataset discovery
            await self._discover_datasets()
            
            add_log(f"SampleDataConnector: Found {len(self._available_datasets)} available datasets")
            return {
                "datasets": self._available_datasets,
                "tables": [d['filename'] for d in self._available_datasets],
                "views": [],
                "objects": []
            }
        except Exception as e:
            add_log(f"SampleDataConnector: Schema retrieval failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Failed to get available datasets: {str(e)}")
    
    async def get_data(
        self, 
        table_name: str,  # This is the filename for sample data
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load sample data from a .pkl file.
        
        Args:
            table_name: Filename of the .pkl file
            limit: Optional row limit
            
        Returns:
            pd.DataFrame: The loaded data
            
        Raises:
            DataFetchError: If data loading fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected or not self._sample_data_dir:
                raise DataFetchError("Sample data connector not initialized")
            
            # Find the file
            candidates = list(self._sample_data_dir.rglob(table_name))
            if not candidates:
                raise DataFetchError(f"Sample file not found: {table_name}")
            
            src_file = candidates[0]
            
            # Load the pickle file
            df = pd.read_pickle(str(src_file))
            
            if limit:
                df = df.head(limit)
            
            add_log(f"SampleDataConnector: Loaded {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            add_log(f"SampleDataConnector: Data load failed for {table_name}: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to load sample data from {table_name}: {str(e)}")
    
    async def copy_to_session(
        self,
        filename: str,
        session_id: str,
        user_id: Optional[str] = None,
        clear_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Copy sample data and domain dictionary to a session's input folder (GCS or local).
        
        Args:
            filename: Name of the .pkl file to copy
            session_id: Target session ID
            user_id: User ID (required for GCS upload)
            clear_existing: Whether to clear existing files first
            
        Returns:
            Dict with copied file info and suggested questions
        """
        await asyncio.sleep(0)
        import io
        
        try:
            if not self._sample_data_dir:
                raise DataFetchError("Sample data connector not initialized")
            
            # Find the source file
            candidates = list(self._sample_data_dir.rglob(filename))
            if not candidates:
                raise DataFetchError(f"Sample file not found: {filename}")
            
            src_file = candidates[0]
            src_dir = src_file.parent
            
            # Find domain directory
            domain_file = src_dir / "domain_directory.json"
            if not domain_file.exists():
                raise DataFetchError("Domain directory (domain_directory.json) not found for sample data")
            
            # Try GCS upload first if user_id provided
            gcs_saved = False
            gcs_paths = {}
            
            if user_id:
                try:
                    from v2.common.gcp import GcpManager
                    from v2.utils.env import init_env
                    
                    add_log(f"SampleDataConnector: Attempting GCS upload for user_id={user_id}, session_id={session_id}")
                    
                    gcp_manager = GcpManager._get_instance()
                    storage_service = gcp_manager._storage_service
                    constants = init_env()
                    bucket_name = constants.get('storage_bucket')
                    
                    add_log(f"SampleDataConnector: Bucket name from constants: '{bucket_name}'")
                    
                    if bucket_name:
                        add_log(f"SampleDataConnector: Starting GCS upload to bucket '{bucket_name}'")
                        
                        # Read PKL file bytes
                        with open(src_file, 'rb') as f:
                            pkl_bytes = f.read()
                        
                        pkl_filename = src_file.name
                        # Ensure IDs are strings (defensive check)
                        user_id_str = str(user_id) if user_id else None
                        session_id_str = str(session_id) if session_id else None
                        
                        # Use utility for PKL upload
                        gcs_path_pkl = await storage_service.upload_pkl_to_session(
                            data=pkl_bytes,
                            user_id=user_id,
                            session_id=session_id,
                            filename=pkl_filename
                        )
                        
                        if gcs_path_pkl:
                            gcs_paths['data_file'] = gcs_path_pkl
                            add_log(f"SampleDataConnector: ✅ Successfully uploaded {pkl_filename} to GCS: {gcs_path_pkl}")
                        
                        # Upload domain dictionary to GCS
                        with open(domain_file, 'rb') as f:
                            domain_bytes = f.read()
                        
                        domain_filename = "domain_directory.json"
                        gcs_blob_path_domain = f"{user_id_str}/{session_id_str}/input_data/{domain_filename}"
                        
                        add_log(f"SampleDataConnector: Uploading domain file ({len(domain_bytes)} bytes) to gs://{bucket_name}/{gcs_blob_path_domain}")
                        
                        await asyncio.to_thread(
                            storage_service._upload_bytes,
                            bucket_name,
                            domain_bytes,
                            gcs_blob_path_domain,
                            "application/json"
                        )
                        
                        gcs_paths['domain_file'] = f"gs://{bucket_name}/{gcs_blob_path_domain}"
                        add_log(f"SampleDataConnector: ✅ Successfully uploaded {domain_filename} to GCS: {gcs_paths['domain_file']}")
                        
                        gcs_saved = True
                        add_log(f"SampleDataConnector: ✅ Successfully uploaded both files to GCS for session {session_id}")
                    else:
                        add_log(f"SampleDataConnector: ⚠️ Bucket name not configured in constants (storage_bucket), skipping GCS upload")
                except Exception as gcs_err:
                    add_log(f"SampleDataConnector: ❌ GCS upload failed, falling back to local: {str(gcs_err)} | traceback: {traceback.format_exc()}")
            else:
                add_log(f"SampleDataConnector: ⚠️ user_id not provided, skipping GCS upload")
            
            # Fallback to local storage if GCS not available or failed
            local_paths = {}
            if not gcs_saved:
                project_root = self._get_project_root()
                input_data_dir = project_root / "execution_layer" / "input_data" / session_id
                
                # Clear existing data if requested
                if clear_existing and input_data_dir.exists():
                    for f in input_data_dir.iterdir():
                        if f.is_file():
                            f.unlink()
                
                os.makedirs(input_data_dir, exist_ok=True)
                
                # Copy files locally
                dest_pkl = input_data_dir / src_file.name
                shutil.copyfile(str(src_file), str(dest_pkl))
                local_paths['data_file'] = str(dest_pkl)
                
                dest_domain = input_data_dir / "domain_directory.json"
                shutil.copyfile(str(domain_file), str(dest_domain))
                local_paths['domain_file'] = str(dest_domain)
                
                add_log(f"SampleDataConnector: Copied {filename} to local session folder: {input_data_dir}")
            
            # Get suggested questions
            dataset_info = next(
                (d for d in self._available_datasets if d['filename'] == filename),
                {}
            )
            
            result = {
                "data_file": src_file.name,
                "domain_file": "domain_directory.json",
                "suggested_questions": dataset_info.get('suggested_questions', []),
                "suggested_questions_simple": dataset_info.get('suggested_questions_simple', [])
            }
            
            if gcs_saved:
                result['gcs_paths'] = gcs_paths
                add_log(f"SampleDataConnector: Successfully uploaded to GCS for session {session_id}")
                await self._session_manager._update_session_status(session_id, "active")
            else:
                result['local_paths'] = local_paths
                add_log(f"SampleDataConnector: Saved locally for session {session_id} (GCS not available)")
                await self._session_manager._update_session_status(session_id, "active")
            
            return result
        except Exception as e:
            add_log(f"SampleDataConnector: Copy failed: {str(e)} | traceback: {traceback.format_exc()}")
            await self._session_manager._update_session_status(session_id, "failed")
            raise DataFetchError(f"Failed to copy sample data: {str(e)}")
    
    async def disconnect(self) -> None:
        """Clean up sample data connector"""
        await asyncio.sleep(0)
        self._sample_data_dir = None
        self._available_datasets = []
        self._connected = False
        add_log("SampleDataConnector: Disconnected")

