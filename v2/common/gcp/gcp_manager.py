"""
GCP Manager - Common utilities for Google Cloud Platform services.
"""

import os
from typing import Optional
from google.auth import default as get_default_credentials
from google.auth.exceptions import DefaultCredentialsError

from v2.common.gcp.services.gcp_secret_service import GcpSecretService
from v2.common.gcp.services.gcp_firestore_service import GcpFirestoreService
from v2.common.gcp.services.gcp_storage_service import GcpStorageService


class GcpManager:
    """Centralized manager for GCP services using Application Default Credentials."""
    
    __instance: Optional['GcpManager'] = None
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize GCP Manager with Application Default Credentials."""
        if GcpManager.__instance is not None:
            return
        
        self.__project_id = project_id or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')
        self.__credentials = None
        
        self._initialize_credentials()
        self._initialize_services()
    
    def _initialize_credentials(self) -> None:
        """Initialize Application Default Credentials."""
        try:
            credentials, project = get_default_credentials()
            self.__credentials = credentials
            
            if not self.__project_id:
                self.__project_id = project
            
            if not self.__project_id:
                raise RuntimeError("GCP project ID not found. Set GCP_PROJECT environment variable.")
            
        except DefaultCredentialsError:
            raise RuntimeError(
                "Failed to initialize GCP credentials. Run: gcloud auth application-default login"
            )
    
    def _initialize_services(self) -> None:
        """Initialize all GCP service clients."""
        self.__secret_service = GcpSecretService(self.__project_id, self.__credentials)
        self.__firestore_service = GcpFirestoreService(self.__project_id, self.__credentials)
        self.__storage_service = GcpStorageService(self.__project_id, self.__credentials)
    
    @property
    def _project_id(self) -> str:
        """Get the GCP project ID."""
        return self.__project_id
    
    @property
    def _secret_service(self) -> GcpSecretService:
        """Get the Secret Manager service."""
        return self.__secret_service
    
    @property
    def _firestore_service(self) -> GcpFirestoreService:
        """Get the Firestore service."""
        return self.__firestore_service
    
    @property
    def _storage_service(self) -> GcpStorageService:
        """Get the Cloud Storage service."""
        return self.__storage_service
    
    @classmethod
    def _get_instance(cls, project_id: Optional[str] = None) -> 'GcpManager':
        """Get singleton instance of GCP Manager."""
        if cls.__instance is None:
            cls.__instance = cls(project_id)
        return cls.__instance

