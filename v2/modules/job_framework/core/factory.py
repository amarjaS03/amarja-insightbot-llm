"""
Factory for creating JobManager with adapters.

Usage:
    # In-memory (default)
    manager = create_job_manager()
    
    # Firestore (for production)
    manager = create_job_manager("firestore", firestore_service=firestore_service)
    
    # Custom store
    manager = create_job_manager(storage_type=my_custom_store)
"""
from typing import Optional
from v2.modules.job_framework.core.manager import JobManager
from v2.modules.job_framework.core.interfaces import JobStore, MilestoneStore
from v2.common.logger import add_log


def create_job_manager(
    storage_type: str = "memory",
    milestone_store: Optional[MilestoneStore] = None,
    **kwargs
) -> JobManager:
    """
    Factory function to create JobManager.
    
    Args:
        storage_type: "memory" (default), "firestore", or custom JobStore instance
        milestone_store: Optional milestone store
        **kwargs: Additional configuration (e.g., firestore_service for firestore)
        
    Returns:
        JobManager: Configured JobManager instance
        
    Examples:
        # In-memory (default)
        manager = create_job_manager()
        
        # Firestore
        manager = create_job_manager("firestore", firestore_service=firestore_service)
        
        # Custom store
        manager = create_job_manager(storage_type=my_custom_store)
    """
    if storage_type == "memory":
        from v2.modules.job_framework.adapters.storage.memory.job_store import InMemoryJobStore
        job_store = InMemoryJobStore()
        if milestone_store is None:
            from v2.modules.job_framework.adapters.storage.memory.milestone_store import InMemoryMilestoneStore
            milestone_store = InMemoryMilestoneStore()
    elif storage_type == "firestore":
        from v2.modules.job_framework.adapters.storage.firestore.job_store import FirestoreJobStore
        firestore_service = kwargs.get('firestore_service')
        job_store = FirestoreJobStore(
            firestore_service=firestore_service,
            collection_name=kwargs.get('collection_name', 'JOBS'),
            session_collection_name=kwargs.get('session_collection_name', 'sessions'),
            user_collection_name=kwargs.get('user_collection_name', 'userCollection'),
            counter_collection_name=kwargs.get('counter_collection_name', 'counters')
        )
        if milestone_store is None:
            from v2.modules.job_framework.adapters.storage.firestore.milestone_store import FirestoreMilestoneStore
            milestone_store = FirestoreMilestoneStore(
                firestore_service=firestore_service,
                collection_name=kwargs.get('collection_name', 'JOBS')
            )
    elif isinstance(storage_type, JobStore):
        job_store = storage_type
    else:
        raise ValueError(f"Unknown storage_type: {storage_type}. Use 'memory', 'firestore', or provide JobStore instance.")
    
    add_log(f"JobManagerFactory: Creating JobManager with {type(job_store).__name__}")
    
    return JobManager(
        job_store=job_store,
        milestone_store=milestone_store
    )

