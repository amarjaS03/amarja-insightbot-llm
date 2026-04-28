"""
Query Store Service for Simple QnA v2

Manages queries subcollection in Firestore:
JOBS/{chat_id}/Queries/{query_id}

Provides atomic query_number generation and query lifecycle management.
"""
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from v2.modules.simple_qna.models.simple_qna_models import QueryCreate, QueryResponse
from v2.common.gcp import GcpManager
from v2.common.logger import add_log
import traceback


class QueryStoreService:
    """
    Service for managing queries subcollection in Firestore.
    Each chat (job) has a Queries subcollection.
    """
    
    def __init__(self, firestore_service=None, jobs_collection_name: str = "JOBS"):
        """
        Initialize query store service.
        
        Args:
            firestore_service: Firestore service instance (from GcpManager if None)
            jobs_collection_name: Jobs collection name (default: "JOBS")
        """
        if firestore_service is None:
            try:
                gcp_manager = GcpManager._get_instance()
                self._firestore_service = gcp_manager._firestore_service
            except Exception:
                raise ValueError(
                    "QueryStoreService requires firestore_service parameter or GcpManager must be configured."
                )
        else:
            self._firestore_service = firestore_service
        
        self._jobs_collection_name = jobs_collection_name
        add_log("QueryStoreService: Initialized with Firestore storage")
    
    def _get_queries_collection_path(self, chat_id: str) -> str:
        """Get the queries subcollection path for a chat"""
        return f"{self._jobs_collection_name}/{str(chat_id)}/Queries"
    
    async def get_next_query_number(self, chat_id: str) -> int:
        """
        Get the next sequential query number for a chat.
        This is atomic and thread-safe.
        
        Args:
            chat_id: Chat ID (job_id)
            
        Returns:
            int: Next query number (1, 2, 3...)
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            
            # Get all existing queries to determine next number
            existing_queries = await self._firestore_service._query_collection(
                queries_collection,
                order_by='query_number',
                order_direction='desc',
                limit=1
            )
            
            if existing_queries and len(existing_queries) > 0:
                max_number = existing_queries[0].get('query_number', 0)
                return max_number + 1
            
            return 1  # First query
        except Exception as e:
            add_log(f"QueryStoreService: Error getting next query number for chat {chat_id}: {str(e)} | traceback: {traceback.format_exc()}")
            # Fallback: try to count all queries
            try:
                all_queries = await self._firestore_service._get_all_documents(queries_collection)
                return len(all_queries) + 1
            except Exception:
                return 1
    
    async def create_query(self, chat_id: str, query_data: QueryCreate, model: str = None) -> QueryResponse:
        """
        Create a new query document in Firestore subcollection.
        
        Args:
            chat_id: Chat ID (job_id)
            query_data: Query creation data (contains query text)
            model: LLM model to use (defaults to MODEL_NAME from env, or "gpt-5.4")
            
        Returns:
            QueryResponse: Created query response
        """
        try:
            # Get default model from environment variable
            import os
            default_model = os.getenv("MODEL_NAME", "gpt-5.4")
            model = model or default_model
            
            queries_collection = self._get_queries_collection_path(chat_id)
            now = datetime.now(timezone.utc)
            
            # Get next query number atomically
            query_number = await self.get_next_query_number(chat_id)
            
            query_dict = {
                'chat_id': chat_id,
                'query': query_data.query,
                'answer': None,
                'query_number': query_number,
                'status': 'pending',
                'created_at': now,
                'completed_at': None,
                'metadata': {
                    'model': model,
                    'metrics': {},
                    'error': None
                }
            }
            
            # Create query document (Firestore generates UUID)
            query_id = await self._firestore_service._create_document(queries_collection, query_dict)
            
            add_log(f"QueryStoreService: Created query {query_id} (number {query_number}) for chat {chat_id}")
            
            return QueryResponse(
                query_id=query_id,
                chat_id=chat_id,
                query=query_data.query,
                answer=None,
                query_number=query_number,
                status='pending',
                created_at=now,
                completed_at=None,
                metadata=query_dict['metadata']
            )
        except Exception as e:
            add_log(f"QueryStoreService: Error creating query for chat {chat_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_query(self, query_id: str, chat_id: str) -> Optional[QueryResponse]:
        """
        Get a query by ID.
        
        Args:
            query_id: Query ID
            chat_id: Chat ID
            
        Returns:
            Optional[QueryResponse]: Query response if found, None otherwise
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            query_doc = await self._firestore_service._get_document(queries_collection, query_id)
            
            if not query_doc:
                return None
            
            return QueryResponse(
                query_id=query_id,
                chat_id=query_doc.get('chat_id', chat_id),
                query=query_doc['query'],
                answer=query_doc.get('answer'),
                query_number=query_doc.get('query_number', 0),
                status=query_doc.get('status', 'pending'),
                created_at=query_doc.get('created_at'),
                completed_at=query_doc.get('completed_at'),
                metadata=query_doc.get('metadata', {})
            )
        except Exception as e:
            add_log(f"QueryStoreService: Error getting query {query_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return None
    
    async def mark_query_running(self, query_id: str, chat_id: str) -> bool:
        """
        Mark a query as running.
        
        Args:
            query_id: Query ID
            chat_id: Chat ID
            
        Returns:
            bool: True if updated successfully
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            update_data = {
                'status': 'running'
            }
            
            await self._firestore_service._update_document(queries_collection, query_id, update_data)
            add_log(f"QueryStoreService: Marked query {query_id} as running")
            return True
        except Exception as e:
            add_log(f"QueryStoreService: Error marking query {query_id} as running: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def mark_query_completed(
        self,
        query_id: str,
        chat_id: str,
        answer: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark a query as completed and save the answer.
        
        Args:
            query_id: Query ID
            chat_id: Chat ID
            answer: HTML answer
            metadata: Optional metadata (metrics, etc.)
            
        Returns:
            bool: True if updated successfully
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            now = datetime.now(timezone.utc)
            
            update_data = {
                'status': 'completed',
                'answer': answer,
                'completed_at': now
            }
            
            if metadata:
                # Merge metadata (preserve existing, update with new)
                existing_doc = await self._firestore_service._get_document(queries_collection, query_id)
                existing_metadata = existing_doc.get('metadata', {}) if existing_doc else {}
                existing_metadata.update(metadata)
                update_data['metadata'] = existing_metadata
            
            await self._firestore_service._update_document(queries_collection, query_id, update_data)
            add_log(f"QueryStoreService: Marked query {query_id} as completed")
            return True
        except Exception as e:
            add_log(f"QueryStoreService: Error marking query {query_id} as completed: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def mark_query_error(
        self,
        query_id: str,
        chat_id: str,
        error_message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark a query as error and save error information.
        
        Args:
            query_id: Query ID
            chat_id: Chat ID
            error_message: Error message
            metadata: Optional metadata
            
        Returns:
            bool: True if updated successfully
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            now = datetime.now(timezone.utc)
            
            update_data = {
                'status': 'error',
                'completed_at': now
            }
            
            # Update metadata with error
            existing_doc = await self._firestore_service._get_document(queries_collection, query_id)
            existing_metadata = existing_doc.get('metadata', {}) if existing_doc else {}
            existing_metadata['error'] = error_message
            if metadata:
                existing_metadata.update(metadata)
            update_data['metadata'] = existing_metadata
            
            await self._firestore_service._update_document(queries_collection, query_id, update_data)
            add_log(f"QueryStoreService: Marked query {query_id} as error: {error_message}")
            return True
        except Exception as e:
            add_log(f"QueryStoreService: Error marking query {query_id} as error: {str(e)} | traceback: {traceback.format_exc()}")
            return False
    
    async def list_chat_queries(
        self,
        chat_id: str,
        order_direction: str = 'asc'
    ) -> List[QueryResponse]:
        """
        List all queries for a chat, ordered by query_number.
        
        Args:
            chat_id: Chat ID
            order_direction: 'asc' for oldest first, 'desc' for newest first
            
        Returns:
            List[QueryResponse]: List of queries
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            
            queries_data = await self._firestore_service._query_collection(
                queries_collection,
                order_by='query_number',
                order_direction=order_direction
            )
            
            queries = []
            for doc in queries_data:
                doc_id = doc.get('id', '')
                queries.append(QueryResponse(
                    query_id=doc_id,
                    chat_id=doc.get('chat_id', chat_id),
                    query=doc['query'],
                    answer=doc.get('answer'),
                    query_number=doc.get('query_number', 0),
                    status=doc.get('status', 'pending'),
                    created_at=doc.get('created_at'),
                    completed_at=doc.get('completed_at'),
                    metadata=doc.get('metadata', {})
                ))
            
            add_log(f"QueryStoreService: Found {len(queries)} queries for chat {chat_id}")
            return queries
        except Exception as e:
            add_log(f"QueryStoreService: Error listing queries for chat {chat_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return []
    
    async def delete_all_chat_queries(self, chat_id: str) -> int:
        """
        Delete all queries for a chat (subcollection JOBS/{chat_id}/Queries).
        Firestore does not auto-delete subcollections when parent is deleted.
        
        Args:
            chat_id: Chat ID (job_id)
            
        Returns:
            int: Number of query documents deleted
        """
        try:
            col_ref = self._firestore_service._client.collection(
                self._jobs_collection_name
            ).document(str(chat_id)).collection("Queries")
            
            def _delete_all():
                docs = list(col_ref.stream())
                for doc in docs:
                    doc.reference.delete()
                return len(docs)
            
            count = await asyncio.to_thread(_delete_all)
            add_log(f"QueryStoreService: Deleted {count} queries for chat {chat_id}")
            return count
        except Exception as e:
            add_log(f"QueryStoreService: Error deleting queries for chat {chat_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def get_completed_query_count(self, chat_id: str) -> int:
        """
        Get count of completed queries for a chat.
        
        Used for policy decisions (rollover, limits, etc.).
        Only counts queries with status='completed' (excludes pending, running, error).
        
        Args:
            chat_id: Chat ID
            
        Returns:
            int: Number of completed queries (for policy decisions)
        """
        try:
            queries_collection = self._get_queries_collection_path(chat_id)
            
            # Query for completed queries only
            # Filter format: (field, operator, value)
            completed_queries = await self._firestore_service._query_collection(
                queries_collection,
                filters=[('status', '==', 'completed')]
            )
            
            completed_query_count = len(completed_queries)
            add_log(f"QueryStoreService: Found {completed_query_count} completed queries for chat {chat_id}")
            return completed_query_count
        except Exception as e:
            add_log(f"QueryStoreService: Error getting completed query count for chat {chat_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return 0
