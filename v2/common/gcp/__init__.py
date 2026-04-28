"""
GCP Common Utilities Package.

Usage:
    from v2.common.gcp import GcpManager
    
    # Initialize (uses gcloud ADC automatically)
    manager = GcpManager._get_instance()
    
    # Secret Manager
    secret = manager._secret_service._get_secret("my-secret-id")
    manager._secret_service._store_secret("my-secret-id", "value")
    
    # Firestore
    doc = manager._firestore_service._get_document("users", "user123")
    manager._firestore_service._set_document("users", "user123", {"name": "John"})
    
    # Cloud Storage
    manager._storage_service._upload_file("bucket", "local.txt", "remote.txt")
    manager._storage_service._download_file("bucket", "remote.txt", "local.txt")
"""

from v2.common.gcp.gcp_manager import GcpManager

__all__ = ['GcpManager']

