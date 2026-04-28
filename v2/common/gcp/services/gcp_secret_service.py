"""
GCP Secret Manager Service - Utilities for managing secrets.
"""

import json
from typing import Optional, Dict, Any
from google.cloud import secretmanager
from google.api_core import exceptions as gcp_exceptions


class GcpSecretService:
    """Service for managing secrets in Google Cloud Secret Manager."""
    
    def __init__(self, project_id: str, credentials=None):
        """Initialize Secret Manager service."""
        self.__project_id = project_id
        
        if credentials:
            self.__client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        else:
            self.__client = secretmanager.SecretManagerServiceClient()
    
    def _get_secret(self, secret_id: str, version: str = "latest") -> str:
        """Retrieve a secret value from Secret Manager."""
        try:
            secret_name = f"projects/{self.__project_id}/secrets/{secret_id}/versions/{version}"
            response = self.__client.access_secret_version(request={"name": secret_name})
            return response.payload.data.decode("UTF-8").strip()
        except gcp_exceptions.NotFound:
            raise RuntimeError(f"Secret '{secret_id}' not found")
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve secret '{secret_id}': {str(e)}")
    
    def _get_secret_json(self, secret_id: str, version: str = "latest") -> Dict[str, Any]:
        """Retrieve a secret value and parse it as JSON."""
        try:
            secret_value = self._get_secret(secret_id, version)
            return json.loads(secret_value)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse secret '{secret_id}' as JSON: {str(e)}")
    
    def _create_secret(self, secret_id: str) -> None:
        """Create a new secret in Secret Manager."""
        try:
            parent = f"projects/{self.__project_id}"
            secret = {"replication": {"automatic": {}}}
            self.__client.create_secret(parent=parent, secret_id=secret_id, secret=secret)
        except gcp_exceptions.AlreadyExists:
            pass
        except Exception as e:
            raise RuntimeError(f"Failed to create secret '{secret_id}': {str(e)}")
    
    def _add_secret_version(self, secret_id: str, secret_value: str) -> None:
        """Add a new version to an existing secret."""
        try:
            secret_name = f"projects/{self.__project_id}/secrets/{secret_id}"
            payload = {"data": secret_value.encode('utf-8')}
            self.__client.add_secret_version(parent=secret_name, payload=payload)
        except gcp_exceptions.NotFound:
            raise RuntimeError(f"Secret '{secret_id}' not found. Create it first.")
        except Exception as e:
            raise RuntimeError(f"Failed to add version to secret '{secret_id}': {str(e)}")
    
    def _store_secret(self, secret_id: str, secret_value: str, create_if_not_exists: bool = True) -> None:
        """Store a secret value (create secret if needed and add version)."""
        try:
            if create_if_not_exists:
                self._create_secret(secret_id)
            self._add_secret_version(secret_id, secret_value)
        except Exception as e:
            raise RuntimeError(f"Failed to store secret '{secret_id}': {str(e)}")
    
    def _store_secret_json(self, secret_id: str, secret_data: Dict[str, Any], create_if_not_exists: bool = True) -> None:
        """Store a dictionary as JSON in Secret Manager."""
        secret_value = json.dumps(secret_data)
        self._store_secret(secret_id, secret_value, create_if_not_exists)
    
    def _delete_secret(self, secret_id: str) -> None:
        """Delete a secret from Secret Manager."""
        try:
            secret_name = f"projects/{self.__project_id}/secrets/{secret_id}"
            self.__client.delete_secret(request={"name": secret_name})
        except gcp_exceptions.NotFound:
            pass
        except Exception as e:
            raise RuntimeError(f"Failed to delete secret '{secret_id}': {str(e)}")
    
    def _list_secrets(self) -> list:
        """List all secrets in the project."""
        try:
            parent = f"projects/{self.__project_id}"
            secrets = []
            for secret in self.__client.list_secrets(request={"parent": parent}):
                secrets.append(secret.name.split('/')[-1])
            return secrets
        except Exception as e:
            raise RuntimeError(f"Failed to list secrets: {str(e)}")

