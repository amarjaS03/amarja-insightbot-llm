"""
GCP Cloud Storage Service - Utilities for Cloud Storage operations.
"""

import os
import asyncio
from typing import Optional, Dict, Any, List
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions

from v2.common.logger import add_log
from v2.utils.env import init_env


class GcpStorageService:
    """Service for managing Cloud Storage operations."""
    
    def __init__(self, project_id: str, credentials=None):
        """Initialize Cloud Storage service."""        
        if credentials:
            self.__client = storage.Client(project=project_id, credentials=credentials)
        else:
            self.__client = storage.Client(project=project_id)
    
    @property
    def _client(self) -> storage.Client:
        """Get the Cloud Storage client."""
        return self.__client
    
    def _upload_file(
        self,
        bucket_name: str,
        source_file_path: str,
        destination_blob_name: str,
        content_type: Optional[str] = None
    ) -> None:
        """Upload a file to Cloud Storage."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            if content_type:
                blob.content_type = content_type
            
            blob.upload_from_filename(source_file_path)
        except FileNotFoundError:
            raise RuntimeError(f"Source file not found: {source_file_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to upload file: {str(e)}")
    
    def _upload_bytes(
        self,
        bucket_name: str,
        data: bytes,
        destination_blob_name: str,
        content_type: Optional[str] = None
    ) -> None:
        """Upload bytes data to Cloud Storage."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            # Pass content_type directly to upload_from_string to avoid Content-Type mismatch
            if content_type:
                blob.upload_from_string(data, content_type=content_type)
            else:
                blob.upload_from_string(data)
        except Exception as e:
            raise RuntimeError(f"Failed to upload bytes: {str(e)}")
    
    async def upload_pkl_to_session(
        self,
        data,
        user_id: str,
        session_id: str,
        filename: str,
        subfolder: str = "input_data"
    ) -> Optional[str]:
        """
        Upload PKL data to GCS session folder.
        
        Handles DataFrame serialization and GCS path construction automatically.
        
        Args:
            data: DataFrame (pandas) to serialize or bytes data
            user_id: User ID (will be converted to string)
            session_id: Session ID (will be converted to string)
            filename: Filename (should include .pkl extension)
            subfolder: Subfolder within session (default: "Input_data")
            
        Returns:
            GCS path (gs://bucket/path) if successful, None if failed
        """
        try:
            import io
            import pandas as pd
            from v2.common.logger import add_log
            
            constants = init_env()
            bucket_name = constants.get('storage_bucket')
            
            if not bucket_name:
                add_log("GcpStorageService: No bucket name configured, skipping GCS upload")
                return None
            
            # Convert DataFrame to bytes if needed
            if hasattr(data, 'to_pickle'):  # Check if it's a DataFrame
                pkl_buffer = io.BytesIO()
                data.to_pickle(pkl_buffer)
                pkl_bytes = pkl_buffer.getvalue()
            else:
                pkl_bytes = data
            
            # Construct GCS path: {user_id}/{session_id}/{subfolder}/{filename}
            user_id_str = str(user_id) if user_id else None
            session_id_str = str(session_id) if session_id else None
            
            if not user_id_str or not session_id_str:
                add_log("GcpStorageService: Missing user_id or session_id")
                return None
                
            gcs_blob_path = f"{user_id_str}/{session_id_str}/{subfolder}/{filename}"
            
            # Delete existing items in the folder if it exists
            # try:
            #     await self._clear_folder_items(bucket_name, f"{user_id_str}/{session_id_str}/{subfolder}")
            # except Exception as delete_err:
            #     add_log(f'GcpStorageService: Failed to clear folder items: {user_id_str}/{session_id_str}/{subfolder} in bucket: {bucket_name}: {str(delete_err)}')
        
            # Upload to GCS using existing _upload_bytes method
            await asyncio.to_thread(
                self._upload_bytes,
                bucket_name,
                pkl_bytes,
                gcs_blob_path,
                "application/octet-stream"
            )
            
            gcs_path = f"gs://{bucket_name}/{gcs_blob_path}"
            add_log(f"GcpStorageService: Successfully uploaded {filename} to {gcs_path}")
            return gcs_path
            
        except Exception as e:
            from v2.common.logger import add_log
            import traceback
            add_log(f"GcpStorageService: Failed to upload PKL to GCS: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    def _download_file(
        self,
        bucket_name: str,
        source_blob_name: str,
        destination_file_path: str
    ) -> None:
        """Download a file from Cloud Storage."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
            blob.download_to_filename(destination_file_path)
        except gcp_exceptions.NotFound:
            raise RuntimeError(f"Blob '{source_blob_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Failed to download file: {str(e)}")
    
    def _download_bytes(self, bucket_name: str, source_blob_name: str) -> bytes:
        """Download a blob as bytes from Cloud Storage."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            return blob.download_as_bytes()
        except gcp_exceptions.NotFound:
            raise RuntimeError(f"Blob '{source_blob_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Failed to download bytes: {str(e)}")
    
    def _delete_file(self, bucket_name: str, blob_name: str) -> None:
        """Delete a file from Cloud Storage."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except gcp_exceptions.NotFound:
            pass
        except Exception as e:
            raise RuntimeError(f"Failed to delete file: {str(e)}")
    
    async def _clear_folder_items(self, bucket_name: str, folder_name: str) -> None:
        """Clear all items in a folder in a bucket."""
        try:
            blobs = self._list_files(bucket_name, folder_name)
            if not blobs:
                add_log(f'GcpStorageService: No items to clear in folder: {folder_name} in bucket: {bucket_name}')
                return
            
            # Delete all blobs in parallel for better performance
            delete_tasks = [
                asyncio.to_thread(
                    self._delete_file,
                    bucket_name,
                    blob_name
                )
                for blob_name in blobs
            ]
            
            await asyncio.gather(*delete_tasks)
            add_log(f"GcpStorageService: Cleared {len(blobs)} items from folder: {folder_name} in bucket: {bucket_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to clear folder items: {str(e)}")
                
    def _list_files(
        self,
        bucket_name: str,
        prefix: Optional[str] = None
    ) -> List[str]:
        """List files in a bucket."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            raise RuntimeError(f"Failed to list files: {str(e)}")
    
    def _file_exists(self, bucket_name: str, blob_name: str) -> bool:
        """Check if a file exists in Cloud Storage."""
        try:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.exists()
        except Exception:
            return False

    def _create_bucket_folder(self, folder_name: str) -> None:
        """Create a folder in a bucket."""
        try:
            constants = init_env()
            bucket = self.__client.bucket(constants.get('storage_bucket'))
            bucket.blob(folder_name).upload_from_string('')
        except Exception as e:
            raise RuntimeError(f"Failed to create folder: {str(e)}")
    
    async def _create_bucket_folders(self, folder_names: List[str]) -> None:
        """
        Create multiple bucket folders in parallel for better performance.
        Only creates folders if they don't already exist (idempotent).
        
        In GCS, a folder exists if:
        1. A blob with the folder name (trailing slash) exists, OR
        2. Any blob with that prefix exists
        
        Args:
            folder_names: List of folder paths to create (should end with '/')
            
        Raises:
            RuntimeError: If folder creation fails
        """
        try:
            constants = init_env()
            bucket_name = constants.get('storage_bucket')
            
            async def folder_exists(folder_name: str) -> bool:
                """Check if a folder exists in GCS"""
                bucket = self.__client.bucket(bucket_name)
                
                # Check if folder marker blob exists (e.g., "input_data/")
                blob = bucket.blob(folder_name)
                if await asyncio.to_thread(blob.exists):
                    return True
                
                # Check if any blob with this prefix exists (folder has content)
                # List blobs with this prefix and limit to 1 for efficiency
                blobs = bucket.list_blobs(prefix=folder_name, max_results=1)
                try:
                    # Try to get first blob (if any exists, folder exists)
                    first_blob = await asyncio.to_thread(next, iter(blobs), None)
                    return first_blob is not None
                except StopIteration:
                    return False
            
            async def create_single_folder(folder_name: str):
                """Create a single folder asynchronously if it doesn't exist"""
                # Check if folder already exists
                exists = await folder_exists(folder_name)
                if not exists:
                    bucket = self.__client.bucket(bucket_name)
                    blob = bucket.blob(folder_name)
                    await asyncio.to_thread(blob.upload_from_string, '')
            
            # Create all folders in parallel (only if they don't exist)
            await asyncio.gather(*[create_single_folder(folder_name) for folder_name in folder_names])
        except Exception as e:
            raise RuntimeError(f"Failed to create bucket folders: {str(e)}")