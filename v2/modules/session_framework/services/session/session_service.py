"""
Session service for FastAPI v2
"""
import asyncio
import os
import time
import yaml
from typing import List, Optional, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from v2.modules.session_framework.controllers.session.dtos import SessionRequestDTO
from v2.common.gcp import GcpManager
from v2.common.model.sessionModel import SessionModel
from v2.common.model.userModel import UserModel
from v2.common.logger import add_log
import traceback
from datetime import datetime

# Cloud Run / GCS imports
try:
    from google.cloud import storage
    import google.auth
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    CLOUD_RUN_AVAILABLE = True
except Exception:
    CLOUD_RUN_AVAILABLE = False
    storage = None

class SessionService:
    """Service layer for session operations"""
    def __init__(self):
        """Initialize session service"""
        self.__gcp_manager = GcpManager._get_instance()
        self.__collection_name = "sessions"
        self.__user_collection_name = "userCollection"
        self.__firestore_service = self.__gcp_manager._firestore_service
        
        # Cloud Run configuration (hardcoded values matching v1)
        self.project_id = "insightbot-dev-474509"
        self.region = "us-central1"
        self.gcs_bucket = "insightbot-dev-474509.firebasestorage.app"
        self.cloud_run_image = "us-central1-docker.pkg.dev/insightbot-dev-474509/code-execution/code-execution-env:latest"
        self.service_account = "dev-insightbot@insightbot-dev-474509.iam.gserviceaccount.com"
        
        # Path to cloud_run_service.yaml (relative to project root)
        # Match v1's pattern: os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloud_run_service.yaml")
        # v1: api_layer/session_manager.py -> goes up 2 levels to project root
        # v2: v2/modules/session_framework/services/session/session_service.py -> need to go up 6 levels
        current_file = Path(__file__).resolve()
        
        # Try to find cloud_run_service.yaml by going up directory levels
        # Start from current file and go up until we find it
        search_path = current_file.parent
        self.cloud_run_yaml_path = None
        
        # Go up maximum 10 levels to find the file
        for _ in range(10):
            potential_path = search_path / "cloud_run_service.yaml"
            if potential_path.exists():
                self.cloud_run_yaml_path = potential_path
                break
            if search_path == search_path.parent:  # Reached filesystem root
                break
            search_path = search_path.parent
        
        # If not found, use calculated path (6 levels up from session_service.py)
        if not self.cloud_run_yaml_path:
            project_root = current_file.parent.parent.parent.parent.parent.parent
            self.cloud_run_yaml_path = project_root / "cloud_run_service.yaml"
        
        # API layer URL for progress streaming (from env or empty)
        from v2.utils.env import init_env
        constants = init_env()
        self.api_layer_url = (os.getenv("API_LAYER_URL") or os.getenv("api_layer_url") or constants.get("api_layer_url") or "").strip().rstrip("/")
        
        self.cloud_run_enabled = bool(CLOUD_RUN_AVAILABLE)

    async def _validate_user_exists(self, user_id: str) -> bool:
        """Validate if user exists in userCollection"""
        try:
            user_doc = await self.__firestore_service._get_document(
                self.__user_collection_name, 
                user_id
            )
            return user_doc is not None
        except Exception as e:
            add_log(f"Error validating user {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return False

    def _build_user_model_from_dict(self, user_doc: dict, user_id: str) -> Optional[UserModel]:
        """Build UserModel from dictionary data (avoids redundant Firestore fetch)"""
        try:
            if not user_doc:
                return None
            
            name = user_doc.get('name', '')
            name_parts = name.split(' ', 1) if name else []
            email = user_doc.get('email') or user_doc.get('emailId', '')
            
            return UserModel(
                uid=user_id,
                email=email,
                name=name,
                created_at=user_doc.get('created_at'),
                updated_at=user_doc.get('updated_at') or user_doc.get('update_at'),
                role=user_doc.get('role', ''),
                status=user_doc.get('status', ''),
                report_count=user_doc.get('report_count', 0),
                issued_token=user_doc.get('issued_token', 0),
                used_token=user_doc.get('used_token', 0),
                emailId=email,
                first_name=name_parts[0] if name_parts else user_doc.get('first_name'),
                last_name=name_parts[1] if len(name_parts) > 1 else user_doc.get('last_name'),
                auth_provider=user_doc.get('auth_provider'),
                created_by=user_doc.get('created_by'),
                registration_type=user_doc.get('registration_type'),
                created_via=user_doc.get('created_via'),
                token_history=user_doc.get('token_history', []),
                session_id=user_doc.get('session_id', [])
            )
        except Exception as e:
            add_log(f"Error building user model from dict {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return None

    async def _get_user_model(self, user_id: str) -> Optional[UserModel]:
        """Get user model from Firestore - matches actual Firestore structure"""
        try:
            user_doc = await self.__firestore_service._get_document(
                self.__user_collection_name,
                user_id
            )
            if not user_doc:
                return None
            
            return self._build_user_model_from_dict(user_doc, user_id)
        except Exception as e:
            add_log(f"Error getting user model {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            return None

    async def _get_all_sessions(self) -> List[SessionModel]:
        """Get all sessions from Firestore"""
        try:
            sessions_data = await self.__firestore_service._get_all_documents(self.__collection_name)
            sessions = []
            for doc in sessions_data:
                doc_id = doc.get('id', '')
                sessions.append(
                    SessionModel(
                        sessionId=doc_id,
                        dataSource=doc.get('data_source', ''),
                        label=doc.get('label', ''),
                        status=doc.get('status', 'active'),
                        createdOn=doc.get('created_on'),
                        currentStep=doc.get('current_step'),
                        nextStep=doc.get('next_step'),
                        credentialId=doc.get('credential_id'),
                        jobIds=doc.get('job_ids', []),
                        suggestedQuestions=doc.get('suggested_questions', []),
                        suggestedQuestionsSimple=doc.get('suggested_questions_simple', [])
                    )
                )
            return sessions
        except Exception as e:
            add_log(f"Error getting all sessions: {str(e)} | traceback: {traceback.format_exc()}")
            raise

    async def _validate_and_fetch_user(self, user_id: str):
        """Validate user exists and fetch user document."""
        user_doc_ref = self.__firestore_service._client.collection(
            self.__user_collection_name
        ).document(user_id)
        user_doc = await asyncio.to_thread(user_doc_ref.get)
        
        if not user_doc.exists:
            raise ValueError(f"User with ID '{user_id}' does not exist")
        
        return user_doc_ref, user_doc.to_dict() or {}

    def _prepare_session_data(self, session_data: SessionRequestDTO, session_id: str, created_on: datetime) -> dict:
        """Prepare session document data."""
        # Determine initial next_step based on connector type workflow
        # Default to "2" (most connectors start at step 1, next is step 2)
        initial_next_step = "2"
        try:
            from v2.modules.connector_framework.config.workflow_steps import CONNECTOR_WORKFLOW_STEPS
            # Normalize data_source to connector type
            connector_type = session_data.dataSource.lower().strip().replace(" ", "_").replace("/", "_")
            workflow = CONNECTOR_WORKFLOW_STEPS.get(connector_type, {})
            if workflow and "1" in workflow:
                initial_next_step = workflow["1"].get("next", "2")
        except Exception:
            # If workflow config not available, use default
            pass
        
        return {
            'session_id': session_id,
            'user_id': session_data.userId,
            'data_source': session_data.dataSource,
            'label': session_data.label or session_data.dataSource,
            'status': 'initiated',
            'current_step': '1',
            'next_step': initial_next_step,  # Set based on workflow, default to "2"
            'credential_id': session_data.credentialId,
            'job_ids': session_data.jobIds or [],
            'created_on': created_on
        }

    def _get_existing_session_ids(self, existing_sessions: list) -> list:
        """Extract session IDs from existing session references."""
        from google.cloud import firestore
        return [
            ref.id if isinstance(ref, firestore.DocumentReference) 
            else (ref.split('/')[-1] if isinstance(ref, str) else str(ref))
            for ref in existing_sessions
        ]

    async def _update_user_sessions(self, user_doc_ref, existing_sessions: list, session_ref, session_id: str):
        """Update user document with new session reference if not duplicate."""
        existing_session_ids = self._get_existing_session_ids(existing_sessions)
        
        if session_id not in existing_session_ids:
            existing_sessions.append(session_ref)
            await asyncio.to_thread(user_doc_ref.update, {'sessions': existing_sessions})

    async def _create_buckets_async(self, user_id: str, session_id: str):
        """
        Create bucket folders asynchronously with error handling.
        
        This method is idempotent - it checks if folders already exist before creating them,
        so it's safe to call multiple times for the same session.
        
        Creates:
        - {user_id}/{session_id}/input_data/
        - {user_id}/{session_id}/output_data/
        """
        try:
            folder_prefix = f"{user_id}/{session_id}/"
            folder_names = [
                f"{folder_prefix}input_data/",
                f"{folder_prefix}output_data/"
            ]
            await asyncio.wait_for(
                self.__gcp_manager._storage_service._create_bucket_folders(folder_names),
                timeout=2.0
            )
        except asyncio.TimeoutError:
            add_log(f"Warning: Bucket folder creation for session {session_id} is taking longer than expected")
        except Exception as e:
            add_log(f"Warning: Failed to create bucket folders for session {session_id}: {str(e)}")

    def _build_session_response(self, user_data: dict, user_id: str, session_id: str, 
                                doc_data: dict, created_on: datetime) -> SessionModel:
        """Build SessionModel response from prepared data."""
        user_model = self._build_user_model_from_dict(user_data, user_id)
        print(f"User model from build_session_response: {user_model}")
        return SessionModel(
            user=user_model,
            sessionId=session_id,
            dataSource=doc_data['data_source'],
            label=doc_data['label'],
            status=doc_data['status'],
            createdOn=created_on,
            currentStep=doc_data['current_step'],
            nextStep=doc_data['next_step'],
            credentialId=doc_data['credential_id'],
            jobIds=doc_data['job_ids'],
            pseudonymized=doc_data.get('pseudonymized', False),
            suggestedQuestions=doc_data.get('suggested_questions', []),
            suggestedQuestionsSimple=doc_data.get('suggested_questions_simple', [])
        )

    async def _create_session(self, session_data: SessionRequestDTO):
        """
        Create a new session in Firestore with auto-generated ID.
        Validates user exists before creating session and adds session reference to user document.
        Optimized for performance with parallel operations and reduced redundant fetches.
        """
        try:
            # Step 1: Validate and fetch user document
            _user_doc_ref, user_data = await self._validate_and_fetch_user(session_data.userId)
            # Step 2: Generate session ID and prepare document data
            doc_ref = self.__firestore_service._client.collection(self.__collection_name).document()
            session_id = doc_ref.id
            created_on = datetime.now()
            doc_data = self._prepare_session_data(session_data, session_id, created_on)
            
            # Step 3: Create Firestore reference for session
            self.__firestore_service._create_reference(self.__collection_name, session_id)
            
            # Step 4: Create session document (critical path)
            await self.__firestore_service._create_document(
                self.__collection_name, doc_data, session_id
            )

            # Start bucket creation as background task (non-blocking)
            _bucket_task = asyncio.create_task(self._create_buckets_async(session_data.userId, session_id))            
            
            # Step 6: Build and return response
            return self._build_session_response(
                user_data, session_data.userId, session_id, doc_data, created_on
            )
        except ValueError:
            raise
        except Exception as e:
            add_log(f"Error creating session: {str(e)} | traceback: {traceback.format_exc()}")
            raise

    async def _get_session_by_id(self, session_id: str) -> Optional[SessionModel]:
        """Get a session by ID from Firestore"""
        try:
            doc = await self.__firestore_service._get_document(
                self.__collection_name,
                session_id
            )
            
            if doc:
                user_id = doc.get('user_id') or None
                user_model = await self._get_user_model(user_id) if user_id else None
                return SessionModel(
                    user=user_model,
                    sessionId=session_id,
                    dataSource=doc.get('data_source', ''),
                    label=doc.get('label', ''),
                    status=doc.get('status', 'active'),
                    createdOn=doc.get('created_on'),
                    currentStep=doc.get('current_step'),
                    nextStep=doc.get('next_step'),
                    credentialId=doc.get('credential_id'),
                    jobIds=doc.get('job_ids', []),
                    pseudonymized=doc.get('pseudonymized', False),
                    suggestedQuestions=doc.get('suggested_questions', []),
                    suggestedQuestionsSimple=doc.get('suggested_questions_simple', [])
                )
            return None
        except Exception as e:
            add_log(f"Error getting session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise

    async def _update_session(self, session_id: str, session_data: SessionRequestDTO) -> Optional[SessionModel]:
        """Update a session in Firestore"""
        try:
            # Get existing document
            existing_doc = await self.__firestore_service._get_document(
                self.__collection_name,
                session_id
            )
            if not existing_doc:
                return None
            
            # Fields that can be updated (DTO field name -> Firestore field name)
            updatable_fields = [
                # ('dataSource', 'data_source'),
                ('label', 'label'),
                # ('status', 'status'),
                ('currentStep', 'current_step'),
                ('nextStep', 'next_step'),
                ('credentialId', 'credential_id'),
                ('jobIds', 'job_ids')
            ]
            
            # Build update data only for fields that are provided
            update_data = {
                firestore_field: getattr(session_data, dto_field)
                for dto_field, firestore_field in updatable_fields
                if getattr(session_data, dto_field) is not None
            }
            
            # Update document if there are fields to update
            if update_data:
                await self.__firestore_service._update_document(
                    self.__collection_name,
                    session_id,
                    update_data
                )
                # Merge update_data into existing_doc for response
                existing_doc.update(update_data)
            
            # Build response from updated document
            user_id = existing_doc.get('user_id')
            user_model = await self._get_user_model(user_id) if user_id else None
            return SessionModel(
                user=user_model,
                sessionId=session_id,
                dataSource=existing_doc.get('data_source', ''),
                label=existing_doc.get('label', ''),
                status=existing_doc.get('status', 'active'),
                createdOn=existing_doc.get('created_on'),
                currentStep=existing_doc.get('current_step'),
                nextStep=existing_doc.get('next_step'),
                credentialId=existing_doc.get('credential_id'),
                jobIds=existing_doc.get('job_ids', []),
                pseudonymized=existing_doc.get('pseudonymized', False),
                suggestedQuestions=existing_doc.get('suggested_questions', []),
                suggestedQuestionsSimple=existing_doc.get('suggested_questions_simple', [])
            )
        except Exception as e:
            add_log(f"Error updating session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise

    async def _get_sessions_by_user_id(self, user_id: str) -> List[SessionModel]:
        """
        Get all sessions for a specific user by querying sessions with user_id field.
        Returns empty list if user has no sessions.
        Note: User document validation is optional - sessions can exist even if user document doesn't.
        """
        try:
            # Try to validate user exists (but don't fail if it doesn't)
            # Sessions can exist even if user document doesn't exist in userCollection
            try:
                await self._validate_and_fetch_user(user_id)
            except ValueError:
                # User document doesn't exist, but sessions might still exist
                # Log and continue to fetch sessions anyway
                add_log(f"SessionService: User document not found for '{user_id}', but checking for sessions anyway")

            # Get all sessions for the user (query by user_id field)
            session_doc = await self.__firestore_service.get_documents_by_field(
                self.__collection_name,
                'user_id',
                user_id
            )        
            if not session_doc:
                return []
            return [
                SessionModel(
                    sessionId=session.get('session_id', ''),
                    dataSource=session.get('data_source', ''),
                    label=session.get('label', ''),
                    status=session.get('status', 'active'),
                    createdOn=session.get('created_on'),
                    currentStep=session.get('current_step'),
                    nextStep=session.get('next_step'),
                    credentialId=session.get('credential_id'),
                    jobIds=session.get('job_ids', []),
                    pseudonymized=session.get('pseudonymized', False),
                    suggestedQuestions=session.get('suggested_questions', []),
                    suggestedQuestionsSimple=session.get('suggested_questions_simple', [])
                )
                for session in session_doc
            ]
        except Exception as e:
            add_log(f"Error getting sessions for user {user_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise

    def _filter_out_session_reference(self, session_refs: list, session_id: str) -> list:
        """Filter out session reference matching session_id from list of references"""
        from google.cloud import firestore
        updated_refs = []
        for ref in session_refs:
            ref_id = None
            if isinstance(ref, firestore.DocumentReference):
                ref_id = ref.id
            elif isinstance(ref, str):
                ref_id = ref.split('/')[-1]
            
            if ref_id and ref_id != session_id:
                updated_refs.append(ref)
        return updated_refs

    def _remove_session_from_user(self, user_doc, session_id: str) -> None:
        """Remove session reference from a user document"""
        user_data = user_doc.to_dict()
        session_refs = user_data.get('sessions', [])
        if not session_refs:
            return
        
        updated_refs = self._filter_out_session_reference(session_refs, session_id)
        if len(updated_refs) != len(session_refs):
            user_doc.reference.update({'sessions': updated_refs})

    async def _update_session_fields(self, session_id: str, update_data: Dict[str, Any]) -> Optional[SessionModel]:
        """
        Update session fields with a dictionary of updates.
        
        Args:
            session_id: Session identifier
            update_data: Dictionary of field updates (e.g., {"current_step": "2", "next_step": "3"})
            
        Returns:
            Optional[SessionModel]: Updated session model or None if not found
        """
        try:
            add_log(f"SessionService: Updating session {session_id} with fields: {update_data}")
            
            # Get existing document
            existing_doc = await self.__firestore_service._get_document(
                self.__collection_name,
                session_id
            )
            if not existing_doc:
                add_log(f"SessionService: Session {session_id} not found for update")
                return None
            
            # Update document
            await self.__firestore_service._update_document(
                self.__collection_name,
                session_id,
                update_data
            )
            
            # Merge update_data into existing_doc for response
            existing_doc.update(update_data)
            
            # Build response from updated document
            user_id = existing_doc.get('user_id')
            user_model = await self._get_user_model(user_id) if user_id else None
            return SessionModel(
                user=user_model,
                sessionId=session_id,
                dataSource=existing_doc.get('data_source', ''),
                label=existing_doc.get('label', ''),
                status=existing_doc.get('status', 'active'),
                createdOn=existing_doc.get('created_on'),
                currentStep=existing_doc.get('current_step'),
                nextStep=existing_doc.get('next_step'),
                credentialId=existing_doc.get('credential_id'),
                jobIds=existing_doc.get('job_ids', []),
                suggestedQuestions=existing_doc.get('suggested_questions', []),
                suggestedQuestionsSimple=existing_doc.get('suggested_questions_simple', [])
            )
        except Exception as e:
            add_log(f"SessionService: Error updating session fields: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    async def _update_session_status(self, session_id: str, status: str) -> bool:
        """
        Update session status directly.
        
        Args:
            session_id: Session identifier
            status: New status value (e.g., 'initiated', 'in_progress', 'active', 'failed')
            
        Returns:
            bool: True if updated successfully, False if session not found
        """
        try:
            # Check if session exists
            doc = await self.__firestore_service._get_document(
                self.__collection_name,
                session_id
            )
            if not doc:
                add_log(f"SessionService: Cannot update status - session {session_id} not found")
                return False
            
            # Update status
            await self.__firestore_service._update_document(
                self.__collection_name,
                session_id,
                {'status': status}
            )
            add_log(f"SessionService: Updated session {session_id} status to '{status}'")
            return True
        except Exception as e:
            add_log(f"SessionService: Error updating session status: {str(e)} | traceback: {traceback.format_exc()}")
            return False

    async def _delete_session(self, session_id: str) -> bool:
        """Delete a session from Firestore"""
        try:
            # Check if document exists
            doc = await self.__firestore_service._get_document(
                self.__collection_name,
                session_id
            )
            if not doc:
                return False
            
            # Remove session reference from all users' sessions arrays
            user_collection_ref = self.__firestore_service._client.collection(self.__user_collection_name)
            all_users = await asyncio.to_thread(lambda: list(user_collection_ref.stream()))
            
            for user_doc in all_users:
                await asyncio.to_thread(self._remove_session_from_user, user_doc, session_id)
            
            # Delete document
            await self.__firestore_service._delete_document(
                self.__collection_name,
                session_id
            )
            return True
        except Exception as e:
            add_log(f"Error deleting session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
            raise
    
    # ----------------------------
    # Cloud Run deployment methods (matching v1 pattern)
    # ----------------------------
    
    def _load_cloud_run_yaml(self, *, service_name: str, bucket_name: str, api_layer_url: str) -> Dict[str, Any]:
        """Load and render cloud_run_service.yaml for a job-specific service."""
        if not self.cloud_run_yaml_path.exists():
            raise FileNotFoundError(f"Cloud Run YAML not found: {self.cloud_run_yaml_path}")
        
        with open(self.cloud_run_yaml_path, "r", encoding="utf-8") as f:
            text = f.read()

        # Replace placeholders (matching v1 pattern)
        text = text.replace("PROJECT_PLACEHOLDER", self.project_id)
        text = text.replace("SESSION_PLACEHOLDER", service_name.replace("exec-", ""))
        text = text.replace("IMAGE_PLACEHOLDER", self.cloud_run_image)
        text = text.replace("BUCKET_PLACEHOLDER", bucket_name)
        text = text.replace("SERVICE_ACCOUNT_PLACEHOLDER", self.service_account)

        docs = list(yaml.safe_load_all(text))
        service_doc = docs[0] if docs else {}
        if not isinstance(service_doc, dict) or service_doc.get("kind") != "Service":
            raise RuntimeError("First document in YAML must be a Cloud Run Service")

        # Enforce desired service name and inject API layer URL env var
        service_doc.setdefault("metadata", {})["name"] = service_name
        tpl = (service_doc.get("spec") or {}).get("template") or {}
        spec = tpl.get("spec") or {}
        containers = spec.get("containers") or []
        if containers and isinstance(containers, list) and isinstance(containers[0], dict):
            env = containers[0].get("env") or []
            if not isinstance(env, list):
                env = []
            # Normalize existing env to a {name: value} map (matching gcp_cloud_run_service pattern)
            env_map: Dict[str, str] = {}
            for e in env:
                if isinstance(e, dict) and e.get("name") and "value" in e:
                    env_map[str(e["name"])] = str(e.get("value") or "")
            
            # Single source of truth: the API layer process env / constants control what gets injected.
            # Always ensure these are present for the execution layer container:
            env_map["GCS_BUCKET"] = bucket_name
            env_map.setdefault("DATA_DIR", "/data")
            
            # Inject API layer URL for Socket.IO progress streaming (only if provided)
            if api_layer_url:
                env_map["api_layer_url"] = api_layer_url.strip().rstrip("/")
            
            # Forward OpenAI key if available (do NOT hardcode in Dockerfile/YAML).
            # This matches the pattern used in gcp_cloud_run_service.py
            openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()
            if openai_key:
                env_map["OPENAI_API_KEY"] = openai_key
            
            # Rebuild env list
            containers[0]["env"] = [{"name": k, "value": v} for k, v in env_map.items() if v != ""]
        return service_doc

    def _wait_for_operation(self, run_client, op_name: str, poll_seconds: float = 1.0, timeout_seconds: int = 300) -> Dict[str, Any]:
        """Wait for Cloud Run operation to complete. poll_seconds=1 for faster detection (was 2)."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            op = run_client.projects().locations().operations().get(name=op_name).execute()
            if op.get("done"):
                if "error" in op:
                    raise RuntimeError(f"Operation failed: {op['error']}")
                return op
            time.sleep(poll_seconds)
        raise TimeoutError(f"Operation did not complete within {timeout_seconds} seconds: {op_name}")

    def _poll_service_url(self, run_client, service_name: str, max_wait_seconds: int = 300, poll_seconds: float = 1.0) -> Optional[str]:
        """Poll Cloud Run service until URL is available."""
        name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        deadline = time.time() + max_wait_seconds
        while time.time() < deadline:
            try:
                svc = run_client.projects().locations().services().get(name=name).execute()
                url = svc.get("status", {}).get("url")
                if url:
                    return url
            except Exception:
                pass
            time.sleep(poll_seconds)
        return None

    def _set_public_invoker_binding(self, run_client, service_name: str) -> None:
        """Grant allUsers roles/run.invoker to the service."""
        resource = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        policy = run_client.projects().locations().services().getIamPolicy(resource=resource).execute()
        bindings = policy.get("bindings", [])
        binding = next((b for b in bindings if b.get("role") == "roles/run.invoker"), None)
        if binding:
            members = set(binding.get("members", []))
            if "allUsers" in members:
                return
            members.add("allUsers")
            binding["members"] = sorted(list(members))
        else:
            bindings.append({"role": "roles/run.invoker", "members": ["allUsers"]})
        policy["bindings"] = bindings
        run_client.projects().locations().services().setIamPolicy(resource=resource, body={"policy": policy}).execute()

    def deploy_job_service(self, *, job_id: str, session_id: str, user_id: str) -> Dict[str, str]:
        """
        Create/replace a Cloud Run service dedicated to this job and return {service_name, service_url}.
        Matches v1's session_manager.deploy_job_service pattern.
        
        Args:
            job_id: Job identifier
            session_id: Session identifier
            user_id: User ID (for GCS paths - buckets are created during session creation)
        """
        if not self.cloud_run_enabled:
            raise RuntimeError("Cloud Run not available")
        if not self.project_id or not self.region:
            raise RuntimeError("Missing GCP_PROJECT/GCP_REGION configuration for Cloud Run")
        if not self.gcs_bucket:
            raise RuntimeError("Missing GCS_BUCKET configuration for Cloud Run")
        if not self.cloud_run_image:
            raise RuntimeError("Missing CLOUD_RUN_IMAGE configuration for Cloud Run")
        if not self.service_account:
            raise RuntimeError("Missing CLOUD_RUN_SERVICE_ACCOUNT configuration for Cloud Run")
        
        # Read API_LAYER_URL fresh from environment each time (not cached) to ensure .env changes are picked up
        # This ensures that if .env is updated and server restarted, new deployments use the latest URL
        load_dotenv(override=True)  # Reload .env file to pick up latest changes
        from v2.utils.env import init_env
        constants = init_env()
        current_api_layer_url = (os.getenv("API_LAYER_URL") or os.getenv("api_layer_url") or constants.get("api_layer_url") or "").strip().rstrip("/")
        
        if not current_api_layer_url:
            add_log(f"[CLOUD_RUN] Warning: API_LAYER_URL not set - progress streaming will be disabled", job_id=job_id)
        else:
            # Log the URL being used for transparency (truncate for security)
            url_display = current_api_layer_url[:50] + "..." if len(current_api_layer_url) > 50 else current_api_layer_url
            add_log(f"[CLOUD_RUN] Using API_LAYER_URL: {url_display}", job_id=job_id)

        # Cloud Run service names must be lowercase, numbers, hyphens; keep it short.
        raw = f"exec-{job_id}".lower().replace("_", "-")
        service_name = raw[:60].strip("-")

        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        # cache_discovery=True reduces repeated discovery requests (faster subsequent deploys)
        run = build("run", "v1", credentials=creds, cache_discovery=True)

        full_name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        parent = f"projects/{self.project_id}/locations/{self.region}"

        body = self._load_cloud_run_yaml(service_name=service_name, bucket_name=self.gcs_bucket, api_layer_url=current_api_layer_url)
        # Remove namespace for location-based API
        if "namespace" in body.get("metadata", {}):
            body["metadata"].pop("namespace", None)

        try:
            run.projects().locations().services().get(name=full_name).execute()
            add_log(f"[CLOUD_RUN] Updating service: {full_name}", job_id=job_id)
            op = run.projects().locations().services().replaceService(name=full_name, body=body).execute()
            if "name" in op:
                self._wait_for_operation(run, op["name"])
        except HttpError as e:
            if e.resp.status == 404:
                add_log(f"[CLOUD_RUN] Creating service: {full_name}", job_id=job_id)
                op = run.projects().locations().services().create(parent=parent, body=body).execute()
                if "name" in op:
                    self._wait_for_operation(run, op["name"])
            else:
                raise

        self._set_public_invoker_binding(run, service_name)
        svc = run.projects().locations().services().get(name=full_name).execute()
        url = svc.get("status", {}).get("url") or self._poll_service_url(run, service_name, max_wait_seconds=300, poll_seconds=1.0)
        if not url:
            raise RuntimeError(f"Cloud Run service URL not available for {service_name}")
        
        add_log(f"[CLOUD_RUN] Service deployed: {service_name} -> {url}", job_id=job_id)
        return {"service_name": service_name, "service_url": url}

    def delete_job_service(self, *, service_name: str, job_id: str | None = None) -> bool:
        """Delete a job-specific Cloud Run service (best-effort)."""
        try:
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            run = build("run", "v1", credentials=creds, cache_discovery=False)
            full_name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
            run.projects().locations().services().delete(name=full_name).execute()
            add_log(f"[CLOUD_RUN] Deleted service {service_name}", job_id=job_id)
            return True
        except Exception as e:
            add_log(f"[CLOUD_RUN] Warning: could not delete service {service_name}: {str(e)}", job_id=job_id)
            return False