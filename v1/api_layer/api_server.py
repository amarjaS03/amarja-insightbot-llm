from flask import Flask, request, jsonify, Response, g
from flask_cors import CORS
from flask_socketio import SocketIO
import threading
from dotenv import load_dotenv
from utils.env import ENV, init_env
from utils.marketingCampaign import marketing_campaign_logs, marketing_campaign_logs_silent
from .session_manager import SessionManager
from .refresh_token import refresh_google_token
from .job_manager import JobManager, JobStatus
from .firebase_user_manager import FirebaseUserManager
from .admin_routes import admin_bp
from .email_routes import email_bp
from .user_routes import user_bp
from .credentials import credentials_bp
import time
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import traceback
from logger import add_log, add_job_log, get_logs, get_job_logs
import traceback
from openai import OpenAI
from flask import request, make_response
import pdfkit
import firebase_admin
from firebase_admin import auth as firebase_auth
# Global variable to store the socketio instance
_global_socketio_instance = None

def get_socketio_instance():
    """Get the global socketio instance for use in other modules"""
    return _global_socketio_instance

def fail(msg: str, status: int = 400):
        return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'Something went wrong'}), status
class ApiServer:
    def __init__(self, constants):
        self.app = Flask(__name__)
        self.app.config['CONSTANTS'] = constants
        # print(f"Constants: {self.app.config}")
        CORS(self.app, 
             origins=self.app.config['CONSTANTS'].get('cors_allowed_origins'),
             methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
             allow_headers=["Content-Type", "Authorization", "X-Requested-With", "X-API-Key", "API-Key"],
             supports_credentials=True)
        
        # Initialize SocketIO for real-time communication with frontend
        self.socketio = SocketIO(self.app, cors_allowed_origins="*", async_mode='threading')
        
        # Set global socketio instance for use in other modules
        global _global_socketio_instance
        _global_socketio_instance = self.socketio
        
        load_dotenv()
        # Configure requests session with longer timeouts
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            # Avoid retrying non-idempotent methods like POST to prevent duplicate analysis runs
            allowed_methods=["HEAD", "GET", "OPTIONS", "TRACE"]
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        
        # Initialize session manager and job manager
        # Docker image parameter commented out - Cloud Run only
        # self.session_manager = SessionManager(docker_image=self.app.config['CONSTANTS'].get('docker_image_name'))
        self.session_manager = SessionManager(docker_image=None)
        self.job_manager = JobManager()

        # Per-session run guards
        self._mutex = threading.Lock()
        self._active_sessions = set()

        # Initialize Firebase User Manager
        self.firebase_user_manager = FirebaseUserManager()
        
        # Load allowed emails from Firestore
        self.allowed_emails = self.load_emails_from_firestore()
        

        
        if self.firebase_user_manager.db:
            print(f"🔥 Firebase connected - {len(self.allowed_emails)} authorized users loaded from Firestore")
        else:
            print("⚠️  Firebase not connected - no users loaded")
        
        # Make firebase_user_manager available to all blueprints
        self.app.firebase_user_manager = self.firebase_user_manager
        
        # Register blueprints
        self.app.register_blueprint(admin_bp)
        self.app.register_blueprint(email_bp)
        self.app.register_blueprint(user_bp)
        self.app.register_blueprint(credentials_bp)
        
        # Expose managers/clients on app for blueprints
        self.app.session_manager = self.session_manager
        self.app.job_manager = self.job_manager
        self.app.requests_session = self.session
        self.app.socketio = self.socketio

        # Register routes and WebSocket events
        self.register_routes()
        self.register_socketio_events()

        # Register extracted blueprints
        try:
            from .blueprints.sessions import sessions_bp
            from .blueprints.data import data_bp
            from .blueprints.salesforce import salesforce_bp
            from .blueprints.acumatica import acumatica_bp
            from .blueprints.db import db_bp
            from .blueprints.n8n import n8n_bp
            from .blueprints.sample_data import sample_data_bp
            self.app.register_blueprint(sessions_bp)
            self.app.register_blueprint(data_bp)
            self.app.register_blueprint(salesforce_bp)
            self.app.register_blueprint(acumatica_bp)
            self.app.register_blueprint(db_bp)
            self.app.register_blueprint(n8n_bp)
            self.app.register_blueprint(sample_data_bp)
        except Exception as e:
            add_log(f"Failed to register blueprints: {e}")
            print(f"⚠️  Failed to register blueprints: {e}")

        # Register global auth middleware
        @self.app.before_request
        def global_auth_middleware():
            if ENV == 'local':
                g.user = {
                    'email': request.headers.get('X-Dev-Email', 'dev@local'),
                    'name': request.headers.get('X-Dev-Name', 'Local Admin'),
                    'role': request.headers.get('X-Dev-Role', 'admin'),
                }
            # Skip auth for OPTIONS preflight and health check or hello route
            if (request.method == 'OPTIONS' or 
                request.path == '/hello' or 
                request.path == '/google_auth' or 
                request.path == '/refresh_token' or
                request.path == '/job_report' or
                request.path == '/api/market_campaign_user' or
                request.path == '/system/status'):
                return None

            # Handle Bearer token authentication for core endpoints
            if True:
                # Use Google OAuth for other endpoints (analysis, sessions, etc.)
                # Extract token from header or query param (for SSE)
                auth_header = request.headers.get('Authorization', '')
                token = None
                if auth_header.startswith('Bearer '):
                    token = auth_header.split('Bearer ')[-1]
                else:
                    token = request.args.get('token')
                payload = self.verify_firebase_token(token)
                if payload is None:
                    return fail('Unauthorized', 401)

                email = payload.get('email', '').lower()
                
                # Check Firebase availability
                if not self.firebase_user_manager.db:
                    print(f"⚠️  Firebase unavailable - allowing authenticated user {email} (no-auth mode)")
                    payload['role'] = 'admin'  # Grant admin access when Firebase is down
                    g.user = payload
                    return None
                
                # Check if user is authorized using Firestore
                is_authorized = self.firebase_user_manager.is_user_authorized(email)
                current_allowed_emails = self.firebase_user_manager.get_authorized_emails()
                
                # Bootstrap mode: If no emails are stored, allow any authenticated user
                # This allows the first user to set up the email system
                if not current_allowed_emails:
                    print(f"🚀 Bootstrap mode: No emails stored, allowing authenticated user {email}")
                    # Add the first user to Firestore as admin
                    success = self.firebase_user_manager.add_user_email(email)
                    if success:
                        self.firebase_user_manager.update_user(email, {'role': 'admin'})
                    payload['role'] = 'admin'
                    g.user = payload
                    return None
                
                # Check if user is authorized
                if not is_authorized:
                    print(f"🚫 User {email} not authorized to access the API")
                    return fail('User not authorized to access this API', 403)

                # User is authorized - get their role from Firestore
                print(f"✅ User {email} authorized for API access")
                user_role = self.firebase_user_manager.get_user_role(email)
                payload['role'] = user_role
                
                g.user = payload
                return None
    
    def register_socketio_events(self):
        """Delegate Socket.IO event registration to the blueprint module without changing behavior."""
        from .blueprints.socketio_events import register_socketio_events
        register_socketio_events(self.socketio, self.job_manager, self.session_manager)
    
    def monitor_job_status(self, job_id):
        """Simple job status monitoring without complex WebSocket forwarding"""
        def run_monitoring():
            try:
                print(f"[API LAYER] 📊 Starting job status monitoring for: {job_id}")
                add_job_log(job_id, f"Started job status monitoring for {job_id}")
                
                while True:
                    # Check job status
                    job_info = self.job_manager.get_job(job_id)
                    if not job_info:
                        print(f"[API LAYER] Job {job_id} not found, stopping monitoring")
                        add_job_log(job_id, f"Job not found during monitoring loop; stopping monitor")
                        break
                    
                    status_str = job_info['status'].value if isinstance(job_info['status'], JobStatus) else job_info['status']
                    
                    # Create status data
                    status_data = {
                        'job_id': job_info['job_id'],
                        'status': status_str,
                        'created_at': job_info['created_at'],
                        'started_at': job_info.get('started_at'),
                        'completed_at': job_info.get('completed_at'),
                        'error': job_info.get('error')
                    }
                    
                    # Emit to session-aware room
                    session_id = job_info.get('session_id', '')
                    if session_id:
                        room_name = f'session_{session_id}_job_{job_id}'
                        self.socketio.emit('job_status', status_data, room=room_name)
                    else:
                        # Fallback to job-only room
                        self.socketio.emit('job_status', status_data, room=f'job_{job_id}')
                    
                    # If job is complete, send final status and break
                    if status_str in ['completed', 'failed', 'cancelled']:
                        print(f"[API LAYER] 🏁 Job {status_str}: {job_id} - Final status reached")
                        add_job_log(job_id, f"Job monitoring detected final status: {status_str}")
                        if session_id:
                            room_name = f'session_{session_id}_job_{job_id}'
                            self.socketio.emit('job_complete', status_data, room=room_name)
                        else:
                            self.socketio.emit('job_complete', status_data, room=f'job_{job_id}')
                        break
                    
                    # Poll every 2 seconds
                    time.sleep(2)
                    
            except Exception as e:
                print(f"[API LAYER] ❌ Job monitoring error for {job_id}: {str(e)}")
                add_job_log(job_id, f"Job monitoring error: {str(e)}")
                # Emit job error only to the session-aware job room
                error_data = {'job_id': job_id, 'error': str(e)}
                job_info = self.job_manager.get_job(job_id)
                if job_info:
                    session_id = job_info.get('session_id', '')
                    if session_id:
                        room_name = f'session_{session_id}_job_{job_id}'
                        self.socketio.emit('job_error', error_data, room=room_name)
                    else:
                        self.socketio.emit('job_error', error_data, room=f'job_{job_id}')
                else:
                    self.socketio.emit('job_error', error_data, room=f'job_{job_id}')
        
        # Start monitoring in a separate thread  
        threading.Thread(target=run_monitoring, daemon=True).start()
    
    def load_emails_from_firestore(self):
        """Load allowed emails from Firestore"""
        try:
            if self.firebase_user_manager.db:
                emails = self.firebase_user_manager.get_authorized_emails()
                print(f"📧 Loaded {len(emails)} authorized users from Firestore")
                return set(emails)
            else:
                print("⚠️  Firebase not available - no users loaded")
            return set()
            
        except Exception as e:
            add_log(f"Error loading emails from Firestore: {str(e)}")
            print(f"❌ Error loading emails from Firestore: {str(e)}")
            return set()
    
    def verify_firebase_token(self, token: str):
        """Verify Firebase Authentication ID token using Firebase Admin SDK.
        Returns payload dict if valid, otherwise None.
        Uses firebase_admin.auth.verify_id_token to validate the token signature and claims.
        """
        if not token:
            return None
        try:
            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app()

            decoded_token = firebase_auth.verify_id_token(token)
            user = firebase_auth.get_user(decoded_token['uid'])

            if "email" not in decoded_token:
                identities = (decoded_token.get("firebase") or {}).get("identities") or {}
                emails = identities.get("email") or []
                if isinstance(emails, list) and emails:
                    decoded_token["email"] = emails[0].lower()
            else:
                decoded_token["email"] = (decoded_token.get("email") or "").lower()

            if not decoded_token.get("email"):
                for provider in user.provider_data:
                    if(provider.email):
                        decoded_token["email"] = provider.email.lower()
                        return decoded_token
                    else: return None
            return decoded_token
        except Exception as e:
            add_log(f"Error verifying token: {str(e)}")
            print("Error verifying token", e)
            return None
    
    def google_auth(self, code: str):
        """Exchange Google auth code for tokens"""
        try:
            resp = requests.post(
                self.app.config['CONSTANTS'].get('google_token_url'),
                data={"code": code, "client_id": os.getenv("GOOGLE_CLIENT_ID"), "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"), "redirect_uri": self.app.config['CONSTANTS'].get('google_redirect_url'), "grant_type": "authorization_code"}
            )
            print("resp", resp.json())
            if resp.status_code != 200:
                return None
            return resp.json()
        except Exception:
            return None
    

    def register_routes(self):
        """Register all API routes"""
        
        @self.app.route('/refresh_token', methods=['POST', 'OPTIONS'])
        def refresh_token_endpoint():
            if request.method == 'OPTIONS':
                return '', 204
            try:
                data = request.get_json()
                if not data or 'refresh_token' not in data:
                    return fail('Missing refresh token', 400)
                
                refresh_token = data['refresh_token']
                tokens = refresh_google_token(refresh_token)
                
                if not tokens:
                    return fail('Failed to refresh token', 500)
                
                # Extract user info from the new ID token to get email
                try:
                    import jwt
                    id_token = tokens.get('id_token')
                    if id_token:
                        # Decode token to get user email (don't verify signature since we just got it from Google)
                        decoded = jwt.decode(id_token, options={"verify_signature": False})
                        email = decoded.get('email', '').lower()
                        
                        if email and self.firebase_user_manager and self.firebase_user_manager.db:
                            # Get user role from database
                            user_role = self.firebase_user_manager.get_user_role(email)
                            if user_role:
                                # Add role to the token response
                                tokens['role'] = user_role
                                print(f"✅ Token refresh with role preserved: {email} -> {user_role}")
                            else:
                                print(f"⚠️  No role found for user {email} during token refresh")
                        else:
                            print(f"⚠️  Could not extract email from token or Firebase unavailable during refresh")
                except Exception as role_error:
                    print(f"⚠️  Error preserving role during token refresh: {str(role_error)}")
                    # Continue without role - don't fail the refresh
                    
                return jsonify(tokens), 200
            except Exception as e:
                add_log(f"Token refresh error: {str(e)}")
                print(f"Token refresh error: {str(e)}")
                return fail(str(e), 500)

        @self.app.route('/google_auth', methods=['POST', 'OPTIONS'])
        def google_auth_endpoint():
            if request.method == 'OPTIONS':
                return '', 204
            try:
                data = request.get_json()
                if not data or 'code' not in data:
                    return fail('Missing authorization code', 400)
                
                auth_code = data['code']
                tokens = self.google_auth(auth_code)
                
                if not tokens:
                    return fail('Failed to exchange authorization code', 500)
                    
                return jsonify(tokens), 200
            except Exception as e:
                add_log(f"Google auth error: {str(e)}")
                print(f"Google auth error: {str(e)}")
                return fail(str(e), 500)

        @self.app.route('/hello')
        def hello():
            return jsonify({'message': 'Insight Bot Running'})
        
        @self.app.route('/system/status')
        def system_status():
            """Get system status and bootstrap information"""
            try:
                # Check Firebase availability first
                firebase_available = bool(self.firebase_user_manager.db)
                
                if not firebase_available:
                    return jsonify({
                        'system_state': 'firebase_unavailable',
                        'message': 'Firebase is not connected - system is in no-auth mode',
                        'firebase_connected': False,
                        'total_authorized_emails': 0,
                        'bootstrap_mode': False,
                        'storage': 'None',
                        'available_actions': {
                            'add_email': False,
                            'view_emails': False,
                            'manage_emails': False,
                            'use_analysis_api': True  # Always allow when Firebase is down
                        }
                    })
                
                # Load current emails from Firestore
                current_allowed_emails = self.firebase_user_manager.get_authorized_emails()
                
                # Check if request is authenticated with API key
                if hasattr(g, 'api_key_auth') and g.api_key_auth:
                    return jsonify({
                        'system_state': 'api_key_authenticated',
                        'message': 'Authenticated with API key - full email management access',
                        'authentication_method': 'API Key',
                        'firebase_connected': True,
                        'total_authorized_emails': len(current_allowed_emails),
                        'storage': 'Firestore',
                        'available_actions': {
                            'add_email': True,
                            'view_emails': True,
                            'manage_emails': True,
                            'delete_emails': True,
                            'update_emails': True
                        }
                    })
                
                # For Bearer token authentication
                user_email = g.user.get('email', '').lower() if hasattr(g, 'user') else None
                
                # Determine system state
                if not current_allowed_emails:
                    system_state = "bootstrap"
                    message = "System is in bootstrap mode - any authenticated user becomes admin"
                elif user_email in current_allowed_emails:
                    system_state = "authorized"
                    message = "You are authorized to use all API endpoints"
                else:
                    system_state = "unauthorized"
                    message = "You are not authorized - contact an admin to add your email"
                
                return jsonify({
                    'system_state': system_state,
                    'message': message,
                    'your_email': user_email,
                    'authentication_method': 'Bearer Token',
                    'firebase_connected': True,
                    'is_authorized': user_email in current_allowed_emails if current_allowed_emails else True,
                    'total_authorized_emails': len(current_allowed_emails),
                    'bootstrap_mode': not bool(current_allowed_emails),
                    'storage': 'Firestore',
                    'available_actions': {
                        'add_email': False,  # Email management requires API key
                        'view_emails': False,  # Email management requires API key
                        'manage_emails': False,  # Email management requires API key
                        'use_analysis_api': not current_allowed_emails or user_email in current_allowed_emails
                    }
                })
                
            except Exception as e:
                add_log(f"Error getting system status: {str(e)} | traceback: {traceback.format_exc()}")
                return fail(f'Failed to get system status: {str(e)}', 500)
        
        # Session routes moved to blueprints.sessions
        
        def check_session_has_input_data(session_id):
            """Check if session has any input data files"""
            try:
                input_data_dir = os.path.join('execution_layer', 'input_data', session_id)
                
                if not os.path.exists(input_data_dir):
                    return False
                    
                # Check if directory has any files
                files = [f for f in os.listdir(input_data_dir) if os.path.isfile(os.path.join(input_data_dir, f))]
                return len(files) > 0
                
            except Exception as e:
                add_log(f"Error checking input data for session {session_id}: {str(e)} | traceback: {traceback.format_exc()}")
                return False

        @self.app.route('/logs')
        @self.app.route('/logs/<session_id>')
        def logs(session_id=None):
            def event_stream():
                try:
                    # Send initial logs
                    current_logs = get_logs()
                    for log in current_logs:
                        yield f"event: log\ndata: {json.dumps(log)}\n\n"
                    
                    # Keep track of last sent log count
                    last_count = len(current_logs)
                    
                    # Keep connection alive and check for new logs
                    while True:
                        current_logs = get_logs()
                        current_count = len(current_logs)
                        
                        # If new logs exist, send them
                        if current_count > last_count:
                            for log in current_logs[last_count:]:
                                yield f"event: log\ndata: {json.dumps(log)}\n\n"
                            last_count = current_count
                        
                        # Send keep-alive every 3 seconds
                        yield f"event: keepalive\ndata: ping\n\n"
                        time.sleep(3)
                except GeneratorExit:
                    # Client disconnected/aborted - exit cleanly without error
                    print(f"[API LAYER] Client disconnected from /logs stream - exiting cleanly")
                    return
            try:
                return Response(
                    event_stream(),
                    mimetype="text/event-stream",
                    headers={
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive',
                        'X-Accel-Buffering': 'no'  # Disable proxy buffering
                    }
                )
            except Exception as e:
                add_log(f"Error getting logs: {str(e)} | traceback: {traceback.format_exc()}")
                return fail(f'Failed to get logs: {str(e)}', 500)
        
        @self.app.route('/job_logs/<job_id>')
        def job_logs(job_id):
            """Stream logs specific to a job with ownership verification"""
            try:
                # Get user info for verification
                token_payload = g.get('user', {})
                user_email = token_payload.get('email', '').lower()
                
                if not user_email:
                    return fail('Unauthorized', 401)
                
                # Verify job ownership
                job_info = self.job_manager.get_job(job_id)
                if not job_info:
                    return fail('Job not found', 404)
                
                job_user_info = job_info.get('user_info', {})
                job_user_email = job_user_info.get('email', '').lower()
                
                if job_user_email != user_email:
                    return fail('Access denied: You do not own this job', 403)
                
                def job_event_stream():
                    try:
                        # Send initial job-specific logs
                        current_job_logs = get_job_logs(job_id)
                        for log in current_job_logs:
                            yield f"event: log\ndata: {json.dumps(log)}\n\n"
                        
                        # Keep track of last sent log count
                        last_count = len(current_job_logs)
                        
                        # Keep connection alive and check for new job logs
                        while True:
                            current_job_logs = get_job_logs(job_id)
                            current_count = len(current_job_logs)
                            
                            # If new logs exist, send them
                            if current_count > last_count:
                                for log in current_job_logs[last_count:]:
                                    yield f"event: log\ndata: {json.dumps(log)}\n\n"
                                last_count = current_count
                            
                            # Send keep-alive every 3 seconds
                            yield f"event: keepalive\ndata: ping\n\n"
                            time.sleep(3)
                    except GeneratorExit:
                        # Client disconnected/aborted - exit cleanly without error
                        print(f"[API LAYER] Client disconnected from /job_logs/{job_id} stream - exiting cleanly")
                        return
                
                return Response(
                    job_event_stream(),
                    mimetype="text/event-stream",
                    headers={
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive',
                        'X-Accel-Buffering': 'no'  # Disable proxy buffering
                    }
                )
            except Exception as e:
                add_log(f"Error getting job logs for {job_id}: {str(e)} | traceback: {traceback.format_exc()}", job_id=job_id)
                return fail(f'Failed to get job logs: {str(e)}', 500)
                
        
        # Backward compatibility routes
        # @self.app.route('/generate_pdf', methods=['POST'])
        # def generate_pdf():
        #     from flask import Flask, request, send_file, make_response
        #     from weasyprint import HTML, CSS
        #     import io
            
        #     data = request.get_json()
        #     html_content = data.get("html")

        #     # Extra CSS to override margins
        #     custom_css = CSS(string="""
        #             @page { size: A4; margin: 10mm; }
        #             body { margin: 0; padding: 0; }
        #             .report-container {
        #                 padding-left: 0% !important;\
        #                 padding-right: 0% !important;
        #             }
        #             .visualization-container {
        #                 padding-top: auto !important;
        #             }
                    
        #         """)

        #     pdf_file = io.BytesIO()
        #     HTML(string=html_content).write_pdf(pdf_file, stylesheets=[custom_css])
        #     pdf_file.seek(0)

        #     response = make_response(pdf_file.read())
        #     response.headers['Content-Type'] = 'application/pdf'
        #     response.headers['Content-Disposition'] = 'attachment; filename=report.pdf'
        #     return response
        @self.app.route('/generate_pdf', methods=['POST'])
        def generate_pdf():
            constants = init_env() 
            # Path to wkhtmltopdf executable (Windows default)
            path_wkhtmltopdf = constants.get("path_wkhtmltopdf")
            config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
            # Add these options
            options = {
                'enable-local-file-access': '',      # allows loading file:// URLs
                'no-stop-slow-scripts': '',
                'load-error-handling': 'ignore',
                'load-media-error-handling': 'ignore',
                "print-media-type": None, 
            }
            data = request.get_json()
            html_content = data.get("html")

            if not html_content:
                return fail("Missing 'html' in request body", 400)

            # Generate PDF from HTML string with default settings
            pdf_data = pdfkit.from_string(html_content, False, configuration=config, options=options)

            # Return as downloadable PDF
            response = make_response(pdf_data)
            response.headers["Content-Type"] = "application/pdf"
            response.headers["Content-Disposition"] = "attachment; filename=report.pdf"

            return response

        @self.app.route('/marketing_campaigns_logs', methods=['POST'])
        def marketing_campaigns_log():
            return marketing_campaign_logs(request.get_json())        
        
    def run(self, host='0.0.0.0', port=5000, debug=True):
        """Run the API server with WebSocket support"""
        # Clean up any old sessions on startup
        try:
            self.session_manager.cleanup_inactive_sessions(max_age_hours=9)  # Clean up sessions older than 1 hour
        except Exception as e:
            add_log(f"Error cleaning up old sessions on startup: {str(e)}")
        
        print(f"[API LAYER] Starting WebSocket server on {host}:{port}")
        self.socketio.run(self.app, debug=debug, host=host, port=port, use_reloader=debug) 