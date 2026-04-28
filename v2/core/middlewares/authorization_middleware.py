"""
Authorization Middleware for FastAPI v2
Handles Firebase token verification and user authorization.

Optimizations:
- Auth cache: After successful Firestore lookup, cache (email -> authorized, role) for 3 min.
  Subsequent requests skip Firestore until cache expires. Revocation takes effect within 3 min.
- Skip firebase_auth.get_user when email is in token (avoids extra Firebase call).
- Single Firestore read for both authorization and role (was 3 reads: is_authorized, get_all, get_role).
"""
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import firebase_admin
from firebase_admin import auth as firebase_auth
from typing import Optional, Dict, Any, Tuple
import os
import time
import threading
from common.logger import add_log
from common.gcp import GcpManager

# Paths that don't require authorization (only docs and system status)
# /internal/* allows execution layer to call milestone API with X-Execution-Layer-Secret header
EXCLUDED_PATHS = ["/docs", "/redoc", "/system/status", "/openapi.json", "/marketing_campaigns_logs", "/users", "/users/", "/internal"]

# Auth cache TTL: 3 min. User can use all APIs without Firestore hit. Revocation takes effect within 3 min.
AUTH_CACHE_TTL_SEC = 180
# Bootstrap cache: "has any users" - 5 min TTL
BOOTSTRAP_CACHE_TTL_SEC = 300


class AuthorizationMiddleware(BaseHTTPMiddleware):
    """Middleware to validate Firebase token and check user authorization"""
    
    def __init__(self, app):
        super().__init__(app)
        self.excluded_paths = EXCLUDED_PATHS
        self.collection_name = 'userCollection'
        self._is_local_env = (os.getenv("ENV", "").strip().lower() in {"local", "localhost"})
        self._auth_cache: Dict[str, Tuple[bool, str, float]] = {}  # email -> (authorized, role, expires_at)
        self._bootstrap_cache: Optional[Tuple[bool, float]] = None   # (has_users, expires_at)
        self._cache_lock = threading.Lock()
        
        add_log("AuthorizationMiddleware: Initializing")
        add_log(f"AuthorizationMiddleware: Excluded paths: {self.excluded_paths}")
        add_log(f"AuthorizationMiddleware: Local auth bypass enabled: {self._is_local_env}")
        
        # Initialize GCP Manager for Firestore access
        try:
            self.gcp_manager = GcpManager._get_instance()
            self.firestore_service = self.gcp_manager._firestore_service
            add_log(f"AuthorizationMiddleware: GCP Manager initialized successfully")
        except Exception as e:
            add_log(f"AuthorizationMiddleware: Failed to initialize GCP Manager: {str(e)}")
            self.gcp_manager = None
            self.firestore_service = None
    
    def _verify_firebase_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify Firebase ID token and return decoded payload. Skip get_user when email is in token."""
        if not token:
            return None
        
        try:
            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app()
            
            decoded_token = firebase_auth.verify_id_token(token)
            
            # Extract email from token first (no extra Firebase call)
            if "email" in decoded_token:
                decoded_token["email"] = (decoded_token.get("email") or "").lower()
            else:
                identities = (decoded_token.get("firebase") or {}).get("identities") or {}
                emails = identities.get("email") or []
                if isinstance(emails, list) and emails:
                    decoded_token["email"] = (emails[0] or "").lower()
            
            # Only call get_user when email is missing (rare)
            if not decoded_token.get("email"):
                user = firebase_auth.get_user(decoded_token['uid'])
                for provider in user.provider_data:
                    if provider.email:
                        decoded_token["email"] = provider.email.lower()
                        return decoded_token
                return None
            
            return decoded_token
        except Exception as e:
            add_log(f"Error verifying Firebase token: {str(e)}")
            print(f"Error verifying token: {e}")
            return None
    
    async def _get_user_auth_and_role(self, email: str) -> Tuple[bool, str]:
        """Single Firestore read: get both authorized status and role."""
        try:
            if not self.firestore_service:
                return False, 'user'
            
            email = email.lower().strip()
            user_doc = await self.firestore_service._get_document(self.collection_name, email)
            
            if not user_doc:
                return False, 'user'
            
            authorized = user_doc.get('status', 'active') == 'active'
            role = user_doc.get('role', 'user')
            return authorized, role
        except Exception as e:
            add_log(f"Failed to get user auth for {email}: {str(e)}")
            return False, 'user'
    
    def _get_cached_auth(self, email: str) -> Optional[Tuple[bool, str]]:
        """Return (authorized, role) if cache hit and not expired."""
        with self._cache_lock:
            entry = self._auth_cache.get(email)
            if not entry:
                return None
            authorized, role, expires_at = entry
            if time.time() > expires_at:
                del self._auth_cache[email]
                return None
            return authorized, role
    
    def _set_cached_auth(self, email: str, authorized: bool, role: str) -> None:
        with self._cache_lock:
            self._auth_cache[email] = (authorized, role, time.time() + AUTH_CACHE_TTL_SEC)
    
    def _get_cached_bootstrap(self) -> Optional[bool]:
        """Return True if we know there are users, False if cache says no users."""
        with self._cache_lock:
            if self._bootstrap_cache is None:
                return None
            has_users, expires_at = self._bootstrap_cache
            if time.time() > expires_at:
                self._bootstrap_cache = None
                return None
            return has_users
    
    def _set_cached_bootstrap(self, has_users: bool) -> None:
        with self._cache_lock:
            self._bootstrap_cache = (has_users, time.time() + BOOTSTRAP_CACHE_TTL_SEC)
    
    async def _is_user_authorized(self, email: str) -> bool:
        """Check if user email is in authorized users collection"""
        try:
            if not self.firestore_service:
                return False
            
            email = email.lower().strip()
            user_doc = await self.firestore_service._get_document(self.collection_name, email)
            
            if user_doc:
                return user_doc.get('status', 'active') == 'active'
            
            return False
        except Exception as e:
            add_log(f"Failed to check user authorization for {email}: {str(e)}")
            return False
    
    async def _get_authorized_emails(self) -> list:
        """Get list of all authorized email addresses"""
        try:
            if not self.firestore_service:
                return []
            
            users = await self.firestore_service._get_all_documents(self.collection_name)
            return [user.get('email', '').lower() for user in users if user.get('status', 'active') == 'active']
        except Exception as e:
            add_log(f"Failed to get authorized emails: {str(e)}")
            return []
    
    async def _get_user_role(self, email: str) -> str:
        """Get user role from Firestore (default: 'user')"""
        try:
            if not self.firestore_service:
                return 'user'
            
            email = email.lower().strip()
            user_doc = await self.firestore_service._get_document(self.collection_name, email)
            
            if user_doc:
                return user_doc.get('role', 'user')
            return 'user'
        except Exception as e:
            add_log(f"Failed to get user role for {email}: {str(e)}")
            return 'user'
    
    async def _add_user_email(self, email: str) -> bool:
        """Add a user email to authorized users collection"""
        try:
            if not self.firestore_service:
                return False
            
            email = email.lower().strip()
            
            # Check if user already exists
            if await self._is_user_authorized(email):
                print(f"✅ User {email} already exists in authorized users")
                return True
            
            # Add user to Firestore
            user_doc = {
                'email': email,
                'role': 'user',  # Default role
                'status': 'active'
            }
            
            await self.firestore_service._set_document(self.collection_name, email, user_doc)
            print(f"✅ User {email} added to authorized users in Firestore")
            return True
        except Exception as e:
            add_log(f"Failed to add user {email} to Firestore: {str(e)}")
            return False
    
    async def _update_user(self, email: str, updates: Dict[str, Any]) -> bool:
        """Update user data in Firestore"""
        try:
            if not self.firestore_service:
                return False
            
            email = email.lower().strip()
            await self.firestore_service._update_document(self.collection_name, email, updates)
            print(f"User {email} updated successfully in Firestore")
            return True
        except Exception as e:
            add_log(f"Failed to update user {email}: {str(e)}")
            return False
    
    async def _try_optional_auth(self, request: Request) -> None:
        """Attempt to authenticate the request without failing if no token or invalid token.
        
        Used for excluded paths so that public endpoints work unauthenticated while
        admin/user-protected endpoints (via Depends) still get request.state.user set
        when a valid token is provided.
        """
        try:
            auth_header = request.headers.get('Authorization', '')
            token = None
            if auth_header.startswith('Bearer '):
                token = auth_header[7:].strip()
                if token.lower().startswith('bearer '):
                    token = token[7:].strip()
            else:
                token = request.query_params.get('token')
            
            if not token:
                return  # No token — that's fine for excluded paths
            
            payload = self._verify_firebase_token(token)
            if payload is None:
                return  # Invalid token — that's fine for excluded paths
            
            email = payload.get('email', '').lower()
            if not email:
                return
            
            # Try to get role from cache or Firestore
            cached = self._get_cached_auth(email)
            if cached is not None:
                is_authorized, user_role = cached
                if is_authorized:
                    payload['role'] = user_role
                    request.state.user = payload
                    add_log(f"AuthorizationMiddleware: Optional auth succeeded for {email} (cached)")
                return
            
            # Cache miss — try Firestore if available
            if self.firestore_service and self.firestore_service._client:
                is_authorized, user_role = await self._get_user_auth_and_role(email)
                self._set_cached_auth(email, is_authorized, user_role)
                if is_authorized:
                    payload['role'] = user_role
                    request.state.user = payload
                    add_log(f"AuthorizationMiddleware: Optional auth succeeded for {email} (Firestore)")
        except Exception as e:
            # Never fail on optional auth — just log and continue
            add_log(f"AuthorizationMiddleware: Optional auth error (non-fatal): {str(e)}")

    async def dispatch(self, request: Request, call_next):
        # Log middleware execution for debugging
        add_log(f"AuthorizationMiddleware: Processing {request.method} {request.url.path}")

        # Local development bypass: skip bearer validation and allow all requests.
        # Non-local environments retain existing authentication flow.
        if self._is_local_env:
            return await call_next(request)
        
        # Allow OPTIONS requests (CORS preflight) to pass through without authentication
        if request.method == "OPTIONS":
            add_log(f"AuthorizationMiddleware: Allowing OPTIONS preflight request for {request.url.path}")
            return await call_next(request)
        
        # Check if path is excluded from mandatory auth
        request_path = request.url.path
        is_excluded = False

        # Check for exact matches first
        if request_path in self.excluded_paths:
            is_excluded = True
        else:
            # Check if path starts with any excluded path (for sub-paths like /docs/static)
            for excluded_path in self.excluded_paths:
                if request_path.startswith(excluded_path + "/"):
                    is_excluded = True
                    break

        if is_excluded:
            # For excluded paths, still attempt optional auth so that endpoints
            # with Depends(require_admin) or Depends(get_current_user) can work
            # when a valid token IS provided. If no token or invalid token, just
            # proceed without setting request.state.user — public endpoints will
            # work fine, and protected endpoints will raise 401 via their dependency.
            add_log(f"AuthorizationMiddleware: Path {request_path} is excluded, attempting optional auth")
            await self._try_optional_auth(request)
            return await call_next(request)
        
        # Extract Bearer token from header or query param (for SSE)
        auth_header = request.headers.get('Authorization', '')
        token = None
        if auth_header.startswith('Bearer '):
            token = auth_header[7:].strip()  # Skip "Bearer "
            # Handle double "Bearer " (e.g. Swagger adding Bearer to token that already had it)
            if token.lower().startswith('bearer '):
                token = token[7:].strip()
            add_log(f"AuthorizationMiddleware: Found Bearer token in Authorization header")
        else:
            token = request.query_params.get('token')
            if token:
                add_log(f"AuthorizationMiddleware: Found token in query params")
        
        if not token:
            add_log(f"AuthorizationMiddleware: No token found for path {request.url.path}")
        
        # Verify Firebase token
        payload = self._verify_firebase_token(token)
        if payload is None:
            add_log(f"AuthorizationMiddleware: Authorization failed - Invalid or missing Bearer token for path {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "result": "fail",
                    "status_code": 401,
                    "message": "Unauthorized",
                    "error": "Valid Bearer token is required"
                }
            )
        
        add_log(f"AuthorizationMiddleware: Token verified successfully for user {payload.get('email', 'unknown')}")
        
        email = payload.get('email', '').lower()
        
        # Check Firebase availability
        if not self.firestore_service or not self.firestore_service._client:
            print(f"⚠️  Firebase unavailable - allowing authenticated user {email} (no-auth mode)")
            payload['role'] = 'admin'  # Grant admin access when Firebase is down
            request.state.user = payload
            return await call_next(request)
        
        # Fast path: check auth cache (skip Firestore)
        cached = self._get_cached_auth(email)
        if cached is not None:
            is_authorized, user_role = cached
            if not is_authorized:
                print(f"🚫 User {email} not authorized (cached)")
                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={
                        "result": "fail",
                        "status_code": 403,
                        "message": "User not authorized to access this API",
                        "error": "Forbidden"
                    }
                )
            payload['role'] = user_role
            request.state.user = payload
            return await call_next(request)
        
        # Cache miss: check bootstrap (cached to avoid fetching all users every time)
        has_users = self._get_cached_bootstrap()
        if has_users is None:
            current_allowed_emails = await self._get_authorized_emails()
            has_users = len(current_allowed_emails) > 0
            self._set_cached_bootstrap(has_users)
        
        # Bootstrap mode: If no emails are stored, allow any authenticated user
        if not has_users:
            print(f"🚀 Bootstrap mode: No emails stored, allowing authenticated user {email}")
            success = await self._add_user_email(email)
            if success:
                await self._update_user(email, {'role': 'admin'})
            self._set_cached_bootstrap(True)  # Now we have at least one user
            payload['role'] = 'admin'
            self._set_cached_auth(email, True, 'admin')
            request.state.user = payload
            return await call_next(request)
        
        # Single Firestore read for both authorized and role
        is_authorized, user_role = await self._get_user_auth_and_role(email)
        self._set_cached_auth(email, is_authorized, user_role)
        
        if not is_authorized:
            print(f"🚫 User {email} not authorized to access the API")
            add_log(f"User {email} not authorized to access the API")
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    "result": "fail",
                    "status_code": 403,
                    "message": "User not authorized to access this API",
                    "error": "Forbidden"
                }
            )
        
        print(f"✅ User {email} authorized for API access")
        add_log(f"AuthorizationMiddleware: User {email} authorized for API access")
        payload['role'] = user_role
        
        request.state.user = payload
        add_log(f"AuthorizationMiddleware: Request authorized, proceeding to handler")
        return await call_next(request)

