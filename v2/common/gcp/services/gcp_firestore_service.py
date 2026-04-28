"""
GCP Firestore Service - Utilities for Firestore database operations.
"""

import asyncio
from typing import Optional, Dict, Any, List
from google.cloud import firestore
from google.api_core import exceptions as gcp_exceptions
from google.api_core.exceptions import FailedPrecondition


class GcpFirestoreService:
    """Service for managing Firestore database operations."""
    
    def __init__(self, project_id: str, credentials=None):
        """Initialize Firestore service."""
                
        if credentials:
            self.__client = firestore.Client(project=project_id, credentials=credentials)
        else:
            self.__client = firestore.Client(project=project_id)
    
    @property
    def _client(self) -> firestore.Client:
        """Get the Firestore client."""
        return self.__client
    
    async def _get_document(self, collection: str, document_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a document from Firestore."""
        try:
            doc_ref = self.__client.collection(collection).document(document_id)
            doc = await asyncio.to_thread(doc_ref.get)
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve document '{collection}/{document_id}': {str(e)}")
    
    async def _create_document(self, collection: str, data: Dict[str, Any], document_id: Optional[str] = None) -> str:
        """Create a document with auto-generated ID in Firestore."""
        try:
            doc_ref = self.__client.collection(collection).document(document_id) if document_id else self.__client.collection(collection).document()
            await asyncio.to_thread(doc_ref.set, data)
            return doc_ref.id
        except Exception as e:
            raise RuntimeError(f"Failed to create document in '{collection}': {str(e)}")
    
    async def _set_document(self, collection: str, document_id: str, data: Dict[str, Any], merge: bool = False) -> None:
        """Create or update a document in Firestore."""
        try:
            doc_ref = self.__client.collection(collection).document(document_id)
            await asyncio.to_thread(doc_ref.set, data, merge=merge)
        except Exception as e:
            raise RuntimeError(f"Failed to save document '{collection}/{document_id}': {str(e)}")
    
    async def _update_document(self, collection: str, document_id: str, updates: Dict[str, Any]) -> None:
        """Update specific fields in a document."""
        try:
            doc_ref = self.__client.collection(collection).document(document_id)
            await asyncio.to_thread(doc_ref.update, updates)
        except gcp_exceptions.NotFound:
            raise RuntimeError(f"Document '{collection}/{document_id}' not found")
        except Exception as e:
            raise RuntimeError(f"Failed to update document '{collection}/{document_id}': {str(e)}")
    
    async def _delete_document(self, collection: str, document_id: str) -> None:
        """Delete a document from Firestore."""
        try:
            doc_ref = self.__client.collection(collection).document(document_id)
            await asyncio.to_thread(doc_ref.delete)
        except Exception as e:
            raise RuntimeError(f"Failed to delete document '{collection}/{document_id}': {str(e)}")
    
    async def _query_collection(
        self,
        collection: str,
        filters: Optional[List[tuple]] = None,
        order_by: Optional[str] = None,
        order_direction: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query documents from a collection.
        
        Args:
            collection: Collection name
            filters: List of (field, operator, value) tuples for filtering
            order_by: Field name to order by
            order_direction: 'desc' for descending (latest first), 'asc' for ascending (oldest first)
            limit: Maximum number of documents to return
        """
        try:
            query = self.__client.collection(collection)
            
            if filters:
                for field, operator, value in filters:
                    query = query.where(field, operator, value)
            
            if order_by:
                # Firestore QueryDirection: ASCENDING or DESCENDING
                if order_direction and order_direction.lower() == 'desc':
                    query = query.order_by(order_by, direction=firestore.Query.DESCENDING)
                elif order_direction and order_direction.lower() == 'asc':
                    query = query.order_by(order_by, direction=firestore.Query.ASCENDING)
                else:
                    # Default to ascending if order_direction not specified
                    query = query.order_by(order_by)
            
            if limit:
                query = query.limit(limit)
            
            def _execute_query():
                results = []
                for doc in query.stream():
                    data = doc.to_dict()
                    data['id'] = doc.id
                    results.append(data)
                return results
            
            return await asyncio.to_thread(_execute_query)
        except FailedPrecondition as e:
            # Firestore index error - preserve the error message with index creation link
            error_msg = str(e)
            if "requires an index" in error_msg or "create_composite" in error_msg:
                # Extract the index creation link if present
                if "create_composite" in error_msg:
                    # The error message contains a link to create the index
                    raise RuntimeError(
                        f"Firestore query requires a composite index for collection '{collection}'. "
                        f"Please create the index using the link provided in the error details.\n"
                        f"Original error: {error_msg}\n"
                        f"Note: You may need separate indexes for ASCENDING and DESCENDING order directions."
                    ) from e
                else:
                    raise RuntimeError(
                        f"Firestore query requires an index for collection '{collection}'. "
                        f"Please check the Firestore console to create the required index.\n"
                        f"Original error: {error_msg}"
                    ) from e
            else:
                raise RuntimeError(f"Failed to query collection '{collection}': {error_msg}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to query collection '{collection}': {str(e)}") from e
    
    async def _get_all_documents(self, collection: str) -> List[Dict[str, Any]]:
        """Get all documents from a collection."""
        return await self._query_collection(collection)
    
    def _create_reference(self, collection: str, document_id: str):
        """Create a Firestore document reference."""
        return self.__client.collection(collection).document(document_id)
    
    async def _resolve_reference(self, reference) -> Optional[Dict[str, Any]]:
        """Resolve a Firestore document reference to document data."""
        try:
            doc = await asyncio.to_thread(reference.get)
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            raise RuntimeError(f"Failed to resolve reference: {str(e)}")
    
    async def _get_document_with_references(self, collection: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a document while preserving DocumentReference objects.
        Returns document data with references intact, or None if document doesn't exist.
        """
        try:
            doc_ref = self.__client.collection(collection).document(document_id)
            doc = await asyncio.to_thread(doc_ref.get)
            if not doc.exists:
                return None
            return doc.to_dict()
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve document '{collection}/{document_id}': {str(e)}")
    
    async def _resolve_references(
        self, 
        references: List[Any], 
        target_collection: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Resolve a list of Firestore references to document data.
        
        Args:
            references: List of DocumentReference objects or string paths
            target_collection: Optional collection name for string path references
            
        Returns:
            List of resolved document dictionaries (skips non-existent documents)
        """
        resolved_docs = []
        
        for ref in references:
            try:
                if isinstance(ref, firestore.DocumentReference):
                    # Resolve DocumentReference directly
                    doc = await asyncio.to_thread(ref.get)
                    if doc.exists:
                        doc_data = doc.to_dict()
                        doc_data['id'] = ref.id
                        resolved_docs.append(doc_data)
                elif isinstance(ref, str) and target_collection:
                    # Handle string paths - extract document ID and fetch
                    doc_id = ref.split('/')[-1]
                    doc_data = await self._get_document(target_collection, doc_id)
                    if doc_data:
                        doc_data['id'] = doc_id
                        resolved_docs.append(doc_data)
            except Exception:
                # Skip invalid references silently
                continue
        
        return resolved_docs
    
    async def get_documents_by_field(self, collection: str, field: str, value: Any) -> List[Dict[str, Any]]:
        try:
            query = self.__client.collection(collection).where(field, '==', value)
            docs = await asyncio.to_thread(query.get)

            result = []
            for doc in docs:
                d = doc.to_dict() or {}
                if 'session_id' not in d and collection == 'sessions':
                    d['session_id'] = doc.id
                result.append(d)
            return result

        except Exception as e:
            raise RuntimeError(f"Failed to get documents: {str(e)}")
