import os
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore, storage
from utils.env import init_env, ENV as CURRENT_ENV

constants = init_env()
# print(f"Constants in firebase_config.py: {constants}")

class FirebaseConfig:
    """
    Simple Firebase Configuration for CRUD Operations
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """Singleton pattern to ensure single Firebase connection"""
        if cls._instance is None:
            cls._instance = super(FirebaseConfig, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize Firebase configuration"""
        if not FirebaseConfig._initialized:
            self.logger = logging.getLogger(__name__)
            self.db = None
            self.bucket = None
            self._initialize_firebase()
            FirebaseConfig._initialized = True
    
    def _initialize_firebase(self) -> None:
        """Initialize Firebase Admin SDK"""
        try:
            # Check if Firebase app is already initialized
            try:
                app = firebase_admin.get_app()
                self.logger.info("🔥 Firebase app already initialized")
            except ValueError:
                # Initialize with service account file resolved per ENV
                configured_keyfile = (os.getenv('KEY_FILE_PATH') or '').strip()
                if configured_keyfile:
                    service_account_path = configured_keyfile
                else:
                    env_dir = os.path.dirname(__file__)
                    if CURRENT_ENV == 'prod':
                        service_account_path = os.path.join(env_dir, 'config', 'key-prod.json')
                    elif CURRENT_ENV == 'dev':
                        service_account_path = os.path.join(env_dir, 'config', 'key-dev.json')
                    else:
                        service_account_path = ''

                if not service_account_path or not os.path.exists(service_account_path):
                    if CURRENT_ENV == 'local':
                        self.logger.info("Skipping Firebase initialization for local environment (no KEY_FILE_PATH provided)")
                        return
                    raise Exception(f"Firebase service account file not found: {service_account_path}")

                cred = credentials.Certificate(service_account_path)
                firebase_admin.initialize_app(cred, {
                    'storageBucket': constants.get('storage_bucket')
                })
                self.logger.info("🔥 Firebase initialized successfully")
            
            # Initialize Firestore client
            self.db = firestore.client()
            self.logger.info("📊 Firestore client ready")
            
            # Initialize Storage bucket
            self.bucket = storage.bucket(constants.get('storage_bucket'))
            self.logger.info("🪣 Firebase Storage bucket ready")
            
        except Exception as e:
            self.logger.error(f"❌ Firebase initialization error: {str(e)}")
            raise
    
    def get_db(self):
        """Get Firestore database client"""
        return self.db
    
    def get_bucket(self):
        """Get Firebase Storage bucket"""
        return self.bucket


class FirebaseCRUD:
    """
    Simple CRUD operations for Firebase Firestore
    """
    
    def __init__(self):
        self.firebase_config = FirebaseConfig()
        self.db = self.firebase_config.get_db()
        self.bucket = self.firebase_config.get_bucket()
        self.logger = logging.getLogger(__name__)
    
    def create(self, collection_name: str, document_id: str, data: Dict[str, Any]) -> bool:
        """
        Create a new document
        
        Args:
            collection_name: Name of the Firestore collection
            document_id: Document ID (use None for auto-generated ID)
            data: Data to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            data['created_at'] = datetime.utcnow()
            data['updated_at'] = datetime.utcnow()
            
            if document_id:
                doc_ref = self.db.collection(collection_name).document(document_id)
                doc_ref.set(data)
                self.logger.info(f"✅ Created document {document_id} in {collection_name}")
            else:
                doc_ref = self.db.collection(collection_name).add(data)
                document_id = doc_ref[1].id
                self.logger.info(f"✅ Created document {document_id} in {collection_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create document in {collection_name}: {str(e)}")
            return False
    
    def read(self, collection_name: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Read a document by ID
        
        Args:
            collection_name: Name of the Firestore collection
            document_id: Document ID to read
            
        Returns:
            Dict containing document data or None if not found
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc = doc_ref.get()
            
            if doc.exists:
                data = doc.to_dict()
                self.logger.info(f"📖 Read document {document_id} from {collection_name}")
                return data
            else:
                self.logger.warning(f"⚠️ Document {document_id} not found in {collection_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Failed to read document {document_id} from {collection_name}: {str(e)}")
            return None
    
    def update(self, collection_name: str, document_id: str, data: Dict[str, Any]) -> bool:
        """
        Update an existing document
        
        Args:
            collection_name: Name of the Firestore collection
            document_id: Document ID to update
            data: Data to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            data['updated_at'] = datetime.utcnow()
            
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc_ref.update(data)
            
            self.logger.info(f"📝 Updated document {document_id} in {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update document {document_id} in {collection_name}: {str(e)}")
            return False
    
    def delete(self, collection_name: str, document_id: str) -> bool:
        """
        Delete a document
        
        Args:
            collection_name: Name of the Firestore collection
            document_id: Document ID to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc_ref.delete()
            
            self.logger.info(f"🗑️ Deleted document {document_id} from {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to delete document {document_id} from {collection_name}: {str(e)}")
            return False
    
    def read_all(self, collection_name: str, limit: int = None) -> List[Dict[str, Any]]:
        """
        Read all documents from a collection
        
        Args:
            collection_name: Name of the Firestore collection
            limit: Maximum number of documents to return
            
        Returns:
            List of document dictionaries
        """
        try:
            query = self.db.collection(collection_name)
            
            if limit:
                query = query.limit(limit)
            
            docs = query.stream()
            documents = []
            
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id  # Add document ID to the data
                documents.append(doc_data)
            
            self.logger.info(f"📚 Read {len(documents)} documents from {collection_name}")
            return documents
            
        except Exception as e:
            self.logger.error(f"❌ Failed to read documents from {collection_name}: {str(e)}")
            return []
    
    def query(self, collection_name: str, field: str, operator: str, value: Any, limit: int = None) -> List[Dict[str, Any]]:
        """
        Query documents with conditions
        
        Args:
            collection_name: Name of the Firestore collection
            field: Field to filter by
            operator: Operator ('==', '>', '<', '>=', '<=', '!=', 'in', 'array-contains')
            value: Value to compare against
            limit: Maximum number of documents to return
            
        Returns:
            List of matching document dictionaries
        """
        try:
            query = self.db.collection(collection_name).where(field, operator, value)
            
            if limit:
                query = query.limit(limit)
            
            docs = query.stream()
            documents = []
            
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id
                documents.append(doc_data)
            
            self.logger.info(f"🔍 Found {len(documents)} documents in {collection_name} where {field} {operator} {value}")
            return documents
            
        except Exception as e:
            self.logger.error(f"❌ Failed to query {collection_name}: {str(e)}")
            return []
    
    def upload_file_to_storage(self, local_file_path: str, storage_path: str) -> Optional[str]:
        """
        Upload file to Firebase Storage and return public URL
        
        Args:
            local_file_path: Path to the local file to upload
            storage_path: Path in Firebase Storage (e.g., 'sessionId/jobId/analysis_report.html')
            
        Returns:
            str: Public accessible URL or None if failed
        """
        try:
            if not os.path.exists(local_file_path):
                self.logger.error(f"❌ Local file not found: {local_file_path}")
                return None
            
            # Get the blob reference
            blob = self.bucket.blob(storage_path)
            
            # Upload the file
            with open(local_file_path, 'rb') as file_data:
                blob.upload_from_file(file_data, content_type='text/html')
            
            # Make the blob public
            blob.make_public()
            
            # Get the public URL
            public_url = blob.public_url
            
            self.logger.info(f"📤 File uploaded to Storage: {storage_path}")
            self.logger.info(f"🔗 Public URL: {public_url}")
            
            return public_url
            
        except Exception as e:
            self.logger.error(f"❌ Failed to upload file to Storage: {str(e)}")
            return None


# Global instance for easy access
firebase_crud = FirebaseCRUD()


def get_firebase_crud() -> FirebaseCRUD:
    """Get Firebase CRUD instance"""
    return firebase_crud


def test_connection() -> bool:
    """
    Test Firebase connection
    
    Returns:
        bool: True if connection is successful
    """
    try:
        crud = get_firebase_crud()
        
        # Try to read from a test collection
        test_data = crud.read_all('_connection_test', limit=1)
        print("✅ Firebase connection successful")
        return True
        
    except Exception as e:
        print(f"❌ Firebase connection failed: {str(e)}")
        return False


# if __name__ == "__main__":
#     # Test the Firebase configuration
#     print("🔥 Testing Firebase CRUD Operations...")
    
#     try:
#         # Test connection
#         test_connection()
        
#         # Get CRUD instance
#         crud = get_firebase_crud()
        
#         # Test CRUD operations
#         print("\n📝 Testing CRUD Operations:")
        
#         # CREATE
#         test_data = {
#             'name': 'Test Document',
#             'description': 'This is a test document',
#             'value': 42
#         }
        
#         success = crud.create('test_collection', 'test_doc_001', test_data)
#         print(f"Create: {'✅ Success' if success else '❌ Failed'}")
        
#         # READ
#         doc = crud.read('test_collection', 'test_doc_001')
#         print(f"Read: {'✅ Success' if doc else '❌ Failed'}")
#         if doc:
#             print(f"  Data: {doc}")
        
#         # UPDATE
#         update_data = {'value': 100, 'updated': True}
#         success = crud.update('test_collection', 'test_doc_001', update_data)
#         print(f"Update: {'✅ Success' if success else '❌ Failed'}")
        
#         # READ ALL
#         docs = crud.read_all('test_collection', limit=5)
#         print(f"Read All: ✅ Found {len(docs)} documents")
        
#         # QUERY
#         results = crud.query('test_collection', 'value', '>', 50)
#         print(f"Query: ✅ Found {len(results)} documents with value > 50")
        
#         # DELETE (uncomment to test deletion)
#         # success = crud.delete('test_collection', 'test_doc_001')
#         # print(f"Delete: {'✅ Success' if success else '❌ Failed'}")
        
#         print("\n🎉 All CRUD operations completed!")
        
#     except Exception as e:
#         print(f"❌ Test failed: {str(e)}")
