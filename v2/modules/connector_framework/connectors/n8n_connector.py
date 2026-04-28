"""
N8N Connector - Handles fetching CSV data from n8n URLs
"""
import asyncio
import os
import io
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs, unquote
import pandas as pd
import requests

from v2.modules.connector_framework.core.base_connector import (
    BaseConnector, ConnectionError, SchemaError, DataFetchError
)
from v2.common.logger import add_log
import traceback


class N8nConnector(BaseConnector):
    """
    N8N Connector for fetching CSV data from URLs.
    
    Handles downloading CSV files from n8n URLs, converting to PKL,
    and saving to session input folders (GCS or local).
    
    Required credentials:
        - None required (n8n connector doesn't need credentials)
    
    Usage:
        - Connect to initialize the connector
        - Call fetch_from_url() with a URL to download and save CSV data
    """
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize N8N connector.
        
        Args:
            credentials: Dict (can be empty for n8n connector)
        """
        super().__init__(credentials)
    
    def _get_project_root(self) -> Path:
        """Get the project root directory"""
        current_file = Path(__file__).resolve()
        # v2/modules/connector_framework/connectors/n8n_connector.py
        # Go up 4 levels to get to project root
        return current_file.parents[4]
    
    async def connect(self) -> bool:
        """
        Initialize n8n connector.
        
        Returns:
            bool: True if initialization successful
            
        Raises:
            ConnectionError: If initialization fails
        """
        await asyncio.sleep(0)
        try:
            self._connected = True
            add_log("N8nConnector: Initialized successfully")
            return {"status": "CONNECTED"}
        except Exception as e:
            self._connected = False
            add_log(f"N8nConnector: Initialization failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to initialize n8n connector: {str(e)}")
    
    async def test_connection(self) -> bool:
        """
        Test if the connector is working.
        
        Returns:
            bool: True (n8n connector doesn't maintain a persistent connection)
        """
        await asyncio.sleep(0)
        try:
            return {"status": "CONNECTED"}
        except Exception as e:
            add_log(f"N8nConnector: Connection test failed: {str(e)} | traceback: {traceback.format_exc()}")
            return {"status": "DISCONNECTED"}
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get schema for n8n connector.
        
        N8N connector doesn't have predefined datasets, so returns empty schema.
        
        Returns:
            Dict containing empty schema
            
        Raises:
            SchemaError: If schema retrieval fails
        """
        await asyncio.sleep(0)
        try:
            if not self._connected:
                raise SchemaError("N8N connector not initialized")
            
            add_log("N8nConnector: Schema retrieved (empty - no predefined datasets)")
            return {
                "datasets": [],
                "tables": [],
                "views": [],
                "objects": []
            }
        except Exception as e:
            add_log(f"N8nConnector: Schema retrieval failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise SchemaError(f"Failed to get schema: {str(e)}")
    
    async def get_data(
        self, 
        table_name: str, 
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Get data is not applicable for n8n connector.
        Data comes from URLs via fetch_from_url() method.
        
        Args:
            table_name: Not used
            limit: Not used
            
        Raises:
            DataFetchError: Always raises - use fetch_from_url() instead
        """
        raise DataFetchError("N8N connector doesn't support get_data(). Use fetch_from_url() instead.")
    
    async def fetch_from_url(
        self,
        url: str,
        session_id: str,
        user_id: Optional[str] = None,
        clear_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch CSV data from URL and save as PKL to session folder (GCS or local).
        
        Args:
            url: URL to download CSV from
            session_id: Target session ID
            user_id: User ID (required for GCS upload)
            clear_existing: Whether to clear existing files first
            
        Returns:
            Dict with saved file info, paths, and preview data
            
        Raises:
            DataFetchError: If fetch/save fails
        """
        await asyncio.sleep(0)
        
        try:
            if not self._connected:
                raise DataFetchError("N8N connector not initialized")
            
            url = url.strip()
            if not url:
                raise DataFetchError("URL is required")
            
            # Fetch CSV bytes
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                csv_bytes = resp.content
                add_log(f"N8nConnector: Downloaded CSV from {url} ({len(csv_bytes)} bytes)")
            except requests.RequestException as re:
                add_log(f"N8nConnector: Failed to download CSV from {url}: {str(re)} | traceback: {traceback.format_exc()}")
                raise DataFetchError(f"Failed to download CSV: {str(re)}")
            
            # Parse CSV via pandas
            try:
                df = pd.read_csv(io.BytesIO(csv_bytes), low_memory=False)
                add_log(f"N8nConnector: Parsed CSV: {len(df)} rows, {len(df.columns)} columns")
            except Exception as pe:
                add_log(f"N8nConnector: Failed reading CSV: {str(pe)} | traceback: {traceback.format_exc()}")
                raise DataFetchError(f"Failed to read CSV: {str(pe)}")
            
            # Determine filename from URL
            parsed = urlparse(url)
            candidate = os.path.basename(parsed.path)
            candidate = unquote(candidate)
            
            # Try to extract filename from URL path or query params
            if not candidate.lower().endswith('.csv') or not candidate:
                qs = parse_qs(parsed.query)
                found = None
                for key in ('filename', 'file', 'name', 'download', 'attachment', 'export'):
                    for value in qs.get(key, []):
                        if '.csv' in value.lower():
                            found = value
                            break
                    if found:
                        break
                if not found:
                    m = re.search(r'([^/?#]+\.csv)(?:[?#]|$)', unquote(url), flags=re.IGNORECASE)
                    candidate = m.group(1) if m else 'download.csv'
                else:
                    candidate = found
            
            # Sanitize for filesystem (Windows-safe)
            unsafe_chars = '<>:"/\\|?*'
            safe_base, _ = os.path.splitext(candidate)
            for ch in unsafe_chars:
                safe_base = safe_base.replace(ch, '_')
            safe_base = safe_base.strip(' .') or 'download'
            
            pkl_filename = f"{safe_base}.pkl"
            
            # Try GCS upload first if user_id provided
            gcs_saved = False
            gcs_path = None
            
            if user_id:
                try:
                    from v2.common.gcp import GcpManager
                    
                    gcp_manager = GcpManager._get_instance()
                    storage_service = gcp_manager._storage_service
                    
                    # Use the new utility method
                    gcs_path = await storage_service.upload_pkl_to_session(
                        data=df,
                        user_id=user_id,
                        session_id=session_id,
                        filename=pkl_filename
                    )
                    
                    if gcs_path:
                        gcs_saved = True
                        add_log(f"N8nConnector: Saved {pkl_filename} to GCS: {gcs_path}")
                except Exception as gcs_err:
                    add_log(f"N8nConnector: GCS save failed, falling back to local: {str(gcs_err)} | traceback: {traceback.format_exc()}")
            
            # Fallback to local storage if GCS not available or user_id not provided
            local_path = None
            if not gcs_saved:
                project_root = self._get_project_root()
                input_data_dir = project_root / "execution_layer" / "input_data" / session_id
                
                # Clear existing data if requested
                if clear_existing and input_data_dir.exists():
                    for f in input_data_dir.iterdir():
                        if f.is_file():
                            f.unlink()
                    add_log(f"N8nConnector: Cleared existing input data for session {session_id}")
                
                os.makedirs(input_data_dir, exist_ok=True)
                pkl_path = input_data_dir / pkl_filename
                
                # Save PKL file locally
                try:
                    df.to_pickle(str(pkl_path))
                    local_path = str(pkl_path)
                    add_log(f"N8nConnector: Saved PKL {pkl_filename} to local: {local_path}")
                except Exception as se:
                    add_log(f"N8nConnector: Failed saving PKL: {str(se)} | traceback: {traceback.format_exc()}")
                    raise DataFetchError(f"Failed to save PKL: {str(se)}")
            
            # Generate preview data (top 10 records)
            top_records = []
            try:
                preview = df.head(10).to_dict('records')
                for rec in preview:
                    str_rec = {str(k): ("null" if v is None else str(v)) for k, v in rec.items()}
                    top_records.append(str_rec)
            except Exception:
                pass
            
            result = {
                "data_file": pkl_filename,
                "saved_file": pkl_filename,
                "preview_data": {
                    "top_records": top_records
                }
            }
            
            if gcs_saved:
                result['gcs_path'] = gcs_path
                add_log(f"N8nConnector: Successfully uploaded to GCS for session {session_id}")
            else:
                result['local_path'] = local_path
                add_log(f"N8nConnector: Saved locally for session {session_id} (GCS not available)")
            
            return result
            
        except DataFetchError:
            raise
        except Exception as e:
            add_log(f"N8nConnector: Fetch failed: {str(e)} | traceback: {traceback.format_exc()}")
            raise DataFetchError(f"Failed to fetch data from URL: {str(e)}")
    
    async def disconnect(self) -> None:
        """Clean up n8n connector"""
        await asyncio.sleep(0)
        self._connected = False
        add_log("N8nConnector: Disconnected")
