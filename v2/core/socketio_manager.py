"""
SocketIO Manager for FastAPI v2
Handles WebSocket connections for real-time progress streaming from execution layer.

Also creates performance milestones from recognized execution progress events
so the Performance Report can compute sub-task durations.
"""
import socketio
import asyncio
from typing import Optional, Dict, Any, Set
from v2.common.logger import add_log


class SocketIOManager:
    """Manages SocketIO server for FastAPI v2"""
    
    def __init__(self):
        """Initialize SocketIO server with ASGI support for FastAPI"""
        # Create SocketIO server with ASGI support for FastAPI
        # Match v1's configuration: cors_allowed_origins="*" and proper async mode
        self.sio = socketio.AsyncServer(
            cors_allowed_origins="*",
            async_mode='asgi',
            allow_eio3=True,  # Backward compatibility for older execution-layer clients.
            logger=False,  # Disable SocketIO's internal logging (we use our own)
            engineio_logger=False,  # Disable EngineIO logging
            ping_timeout=60,  # Timeout for ping/pong (60 seconds)
            ping_interval=25  # Send ping every 25 seconds (SocketIO handles ping/pong automatically)
        )
        self.app = None  # Will be set when mounting to FastAPI app
        # Track active monitoring tasks to prevent duplicates
        self.active_monitoring_tasks: Dict[str, asyncio.Task] = {}
        self.monitoring_lock = asyncio.Lock()
        # Track which progress-based milestones have been created per job
        # to avoid duplicates. Key = job_id, value = set of stage names.
        self._progress_milestones_created: Dict[str, Set[str]] = {}
        
    def mount_to_app(self, fastapi_app):
        """
        Mount SocketIO to FastAPI app.
        
        Returns the ASGI app that wraps both SocketIO and FastAPI.
        This should be used as the main application entry point.
        
        The SocketIO server primary path is /socket.io (legacy/default path).
        Execution layer clients should connect to: {API_LAYER_URL}/socket.io
        """
        from socketio import ASGIApp

        asgi_app = ASGIApp(self.sio, fastapi_app, socketio_path='socket.io')

        async def socket_compatible_app(scope, receive, send):
            """
            Compatibility wrapper to support both:
            - /socket.io  (primary)
            - /socketio   (alias for already-migrated clients)
            """
            path = scope.get("path", "") or ""
            if path.startswith("/socketio"):
                rewritten_scope = dict(scope)
                rewritten_scope["path"] = path.replace("/socketio", "/socket.io", 1)
                return await asgi_app(rewritten_scope, receive, send)
            return await asgi_app(scope, receive, send)

        self.app = socket_compatible_app
        add_log("SocketIO ASGI app mounted at /socket.io (alias /socketio supported)")
        return self.app
    
    def get_sio(self):
        """Get the SocketIO server instance"""
        return self.sio
    
    def register_events(self, job_manager, session_manager):
        """Register SocketIO event handlers"""
        
        @self.sio.on('connect')
        async def handle_connect(sid, environ):
            """
            Handle client connection (execution layer or frontend).
            
            This matches v1's behavior: accept all connections and log them.
            Execution layer clients will connect and emit 'execution_progress' events.
            
            Note: In python-socketio AsyncServer, returning True/False doesn't control
            connection acceptance - the connection is accepted by default unless we raise an exception.
            """
            try:
                # Extract client info from environ if available
                client_info = environ.get('REMOTE_ADDR', 'unknown')
                user_agent = environ.get('HTTP_USER_AGENT', 'unknown')
                print(f"[API LAYER] ✅ WebSocket client connected: {sid} from {client_info} (UA: {user_agent[:50]})")
                add_log(f"WebSocket client connected: {sid} from {client_info}")
                # Connection is accepted by default in python-socketio (no need to return True)
                # Log successful connection for debugging
                print(f"[API LAYER] ✅ Connection accepted for client: {sid}")
            except Exception as e:
                add_log(f"WS connect handler error: {str(e)}")
                print(f"[API LAYER] ❌ Error in connect handler: {e}")
                # Don't raise exception - accept connection anyway (best-effort, matches v1)
        
        @self.sio.on('disconnect')
        async def handle_disconnect(sid):
            """Handle client disconnection"""
            try:
                print(f"[API LAYER] WebSocket client disconnected: {sid}")
                add_log(f"WebSocket client disconnected: {sid}")
            except Exception as e:
                add_log(f"WS disconnect handler error: {str(e)}")
                print(f"[API LAYER] Error in disconnect handler: {e}")
        
        @self.sio.on('execution_progress')
        async def handle_execution_progress(sid, data: Dict[str, Any]):
            """
            Forward progress events from execution layer to frontend.
            
            This handler receives 'execution_progress' events from the execution layer
            (Cloud Run service) and forwards them to frontend clients via 'job_progress' events.
            
            Additionally creates performance milestones from recognized stage names
            so the Performance Report can compute sub-task durations. Noisy events
            (LLM thinking logs starting with emoji robot prefix) are skipped.
            
            v2 pattern: job_id is now a UUID string (not integer), lookup job from Firestore
            via job_manager, extract session_id, forward to room.
            """
            try:
                job_id_str = data.get('job_id')
                if not job_id_str:
                    print(f"[API LAYER] ⚠️ execution_progress event missing job_id")
                    return
                
                # Normalize job_id: remove "JOB_" prefix if present (legacy format support)
                # job_id is now a UUID string, but execution layer might send "JOB_<uuid>"
                if isinstance(job_id_str, str) and job_id_str.startswith("JOB_"):
                    job_id_str = job_id_str.replace("JOB_", "", 1)
                
                # Ensure job_id is a string (UUID)
                job_id_str = str(job_id_str)
                
                try:
                    # Log progress event
                    add_log(f"Forwarding execution progress: {data.get('stage','')} - {data.get('message','')}")
                except Exception:
                    pass
                
                print(f"[API LAYER] 📡 Forwarding progress for job {job_id_str}: {data.get('emoji', '')} {data.get('stage', '')} - {data.get('message', '')}")
                
                # v2: Get job from Firestore via job_manager (async) using UUID string
                session_id = None
                if job_id_str and job_manager:
                    try:
                        job_response = await job_manager._get_job_by_id(job_id_str)
                        if job_response:
                            session_id = str(job_response.session_id)  # session_id is now also a UUID string
                            add_log(f"SocketIO: Found job {job_id_str} in session {session_id}")
                    except Exception as job_lookup_error:
                        # Silently continue - we'll forward to room anyway
                        add_log(f"SocketIO: Could not lookup job {job_id_str}: {str(job_lookup_error)}")
                        pass
                
                # Forward to session-aware room if session_id found, otherwise fallback to job-only room
                if session_id:
                    room_name = f'session_{session_id}_job_{job_id_str}'
                    await self.sio.emit('job_progress', data, room=room_name)
                    print(f"[API LAYER] ✅ Progress forwarded to room: {room_name}")
                else:
                    # Fallback: forward to job-specific room (execution layer might be ahead of job creation)
                    await self.sio.emit('job_progress', data, room=f'job_{job_id_str}')
                    print(f"[API LAYER] ✅ Progress forwarded to room: job_{job_id_str}")
                
                # ── Create performance milestone from recognized stages ──
                await self._maybe_create_progress_milestone(job_id_str, data, job_manager)
                    
            except Exception as e:
                add_log(f"WS execution_progress error: {str(e)}")
                print(f"[API LAYER] ❌ Error forwarding execution progress: {e}")
                try:
                    add_log(f"WS progress forwarding error: {str(e)}")
                except Exception:
                    pass
        
        @self.sio.on('join_job')
        async def handle_join_job(sid, data: Dict[str, Any]):
            """Handle client joining a job monitoring room"""
            job_id_input = data.get('job_id')
            user_email = data.get('user_email', '').lower()
            session_id_input = data.get('session_id', '')
            
            if not (job_id_input and user_email and session_id_input):
                await self.sio.emit('join_error', {'error': 'Missing job_id, user_email, or session_id'}, room=sid)
                return
            
            try:
                # Normalize job_id: remove "JOB_" prefix if present (legacy format support)
                # job_id is now a UUID string, but might come with "JOB_" prefix
                if isinstance(job_id_input, str) and job_id_input.startswith("JOB_"):
                    job_id_str = job_id_input.replace("JOB_", "", 1)
                else:
                    job_id_str = str(job_id_input)
                
                # session_id is now also a UUID string
                session_id_str = str(session_id_input)
                
                # Check session ownership (v2: session ownership is verified via job ownership check below)
                if hasattr(session_manager, 'check_session_ownership'):
                    try:
                        # session_manager.check_session_ownership expects string UUIDs and is async
                        is_owner = await session_manager.check_session_ownership(session_id_str, user_email)
                        if not is_owner:
                            await self.sio.emit('join_error', {'job_id': job_id_str, 'error': 'Access denied: You do not own this session'}, room=sid)
                            print(f"[API LAYER] Session access denied for user {user_email} to session {session_id_str}")
                            return
                    except Exception as session_check_error:
                        # Continue with job ownership check if session check fails
                        add_log(f"SocketIO: Session ownership check failed: {str(session_check_error)}")
                        pass
                
                # v2: Get job from Firestore via job_manager (async) using UUID string
                job_response = None
                if job_manager:
                    try:
                        job_response = await job_manager._get_job_by_id(job_id_str)
                    except Exception as job_lookup_error:
                        await self.sio.emit('join_error', {'job_id': job_id_str, 'error': f'Job {job_id_str} not found'}, room=sid)
                        print(f"[API LAYER] ❌ Job {job_id_str} not found for user {user_email}: {str(job_lookup_error)}")
                        return
                
                if not job_response:
                    await self.sio.emit('join_error', {'job_id': job_id_str, 'error': f'Job {job_id_str} not found'}, room=sid)
                    print(f"[API LAYER] ❌ Job {job_id_str} not found for user {user_email}")
                    return
                
                # Verify job belongs to user and session
                # v2: job_id, user_id, and session_id are now all UUID strings
                job_user_id = str(job_response.user_id)  # Convert to string for comparison
                job_session_id = str(job_response.session_id)  # Convert to string for comparison
                
                # Check ownership: verify session_id matches (both are UUID strings now)
                if job_session_id == session_id_str:
                    room_name = f'session_{session_id_str}_job_{job_id_str}'
                    await self.sio.enter_room(sid, room_name)
                    
                    # Start job status monitoring task if not already active (similar to v1)
                    async with self.monitoring_lock:
                        if job_id_str not in self.active_monitoring_tasks:
                            # Create monitoring task
                            monitoring_task = asyncio.create_task(
                                self._monitor_job_status(job_id_str, job_manager, room_name)
                            )
                            self.active_monitoring_tasks[job_id_str] = monitoring_task
                            print(f"[API LAYER] 🚀 Started monitoring task for job: {job_id_str}")
                        else:
                            print(f"[API LAYER] ⚠️  Monitoring already active for job: {job_id_str}")
                    
                    # Send current job status immediately for reconnection scenarios
                    status_str = job_response.status.value if hasattr(job_response.status, 'value') else str(job_response.status)
                    current_status_data = {
                        'job_id': job_response.job_id,
                        'status': status_str,
                        'created_at': job_response.created_on.isoformat() if job_response.created_on else None,
                        'started_at': job_response.started_at.isoformat() if job_response.started_at else None,
                        'completed_at': job_response.completed_at.isoformat() if job_response.completed_at else None,
                        'error': job_response.error_message
                    }
                    await self.sio.emit('job_status', current_status_data, room=sid)
                    
                    # If job is already complete, send completion event
                    if status_str in ['completed', 'error', 'cancelled']:
                        await self.sio.emit('job_complete', current_status_data, room=sid)
                        print(f"[API LAYER] 🔄 Reconnection: Job {job_id_str} already {status_str}")
                    
                    await self.sio.emit('joined_job', {
                        'job_id': job_id_str,
                        'session_id': session_id_str,
                        'status': 'joined',
                        'room': room_name
                    }, room=sid)
                    print(f"[API LAYER] User {user_email} joined session-aware job monitoring: {job_id_str} in session {session_id_str}")
                else:
                    await self.sio.emit('join_error', {'job_id': job_id_str, 'error': 'Access denied: Job not found in your session'}, room=sid)
                    print(f"[API LAYER] Job access denied for user {user_email} to job {job_id_str} (session mismatch: expected {session_id_str}, got {job_session_id})")
            except Exception as e:
                add_log(f"WS join_job error: {str(e)}")
                print(f"[API LAYER] ❌ Error in join_job handler: {e}")
                await self.sio.emit('join_error', {'error': str(e)}, room=sid)
        
        @self.sio.on('leave_job')
        async def handle_leave_job(sid, data: Dict[str, Any]):
            """Handle client leaving a job monitoring room"""
            job_id = data.get('job_id')
            session_id = data.get('session_id', '')
            
            if job_id and session_id:
                room_name = f'session_{session_id}_job_{job_id}'
                await self.sio.leave_room(sid, room_name)
                await self.sio.emit('left_job', {
                    'job_id': job_id,
                    'session_id': session_id,
                    'status': 'left',
                    'room': room_name
                }, room=sid)
                print(f"[API LAYER] User left session-aware job monitoring: {job_id} in session {session_id}")
            elif job_id:
                await self.sio.leave_room(sid, f'job_{job_id}')
                await self.sio.emit('left_job', {'job_id': job_id, 'status': 'left'}, room=sid)
                print(f"[API LAYER] User left job monitoring: {job_id}")
        
        @self.sio.on('join_job_logs')
        async def handle_join_job_logs(sid, data: Dict[str, Any]):
            """Handle client joining a job log streaming room"""
            job_id_input = data.get('job_id')
            user_email = data.get('user_email', '').lower()
            session_id_input = data.get('session_id', '')
            
            if not (job_id_input and user_email and session_id_input):
                await self.sio.emit('join_logs_error', {'error': 'Missing job_id, user_email, or session_id'}, room=sid)
                return
            
            try:
                # Normalize job_id: remove "JOB_" prefix if present (legacy format support)
                if isinstance(job_id_input, str) and job_id_input.startswith("JOB_"):
                    job_id_str = job_id_input.replace("JOB_", "", 1)
                else:
                    job_id_str = str(job_id_input)
                
                session_id_str = str(session_id_input)
                
                # Check session ownership
                if hasattr(session_manager, 'check_session_ownership'):
                    try:
                        is_owner = await session_manager.check_session_ownership(session_id_str, user_email)
                        if not is_owner:
                            await self.sio.emit('join_logs_error', {'job_id': job_id_str, 'error': 'Access denied: You do not own this session'}, room=sid)
                            print(f"[API LAYER] Log session access denied for user {user_email} to session {session_id_str}")
                            return
                    except Exception as session_check_error:
                        add_log(f"SocketIO: Session ownership check failed: {str(session_check_error)}")
                        pass
                
                # Get job to verify ownership
                job_response = None
                if job_manager:
                    try:
                        job_response = await job_manager._get_job_by_id(job_id_str)
                    except Exception as job_lookup_error:
                        await self.sio.emit('join_logs_error', {'job_id': job_id_str, 'error': f'Job {job_id_str} not found'}, room=sid)
                        print(f"[API LAYER] ❌ Job {job_id_str} not found for log streaming")
                        return
                
                if job_response:
                    job_session_id = str(job_response.session_id)
                    
                    # Verify job belongs to session
                    if job_session_id == session_id_str:
                        log_room_name = f'job_logs_{job_id_str}'
                        await self.sio.enter_room(sid, log_room_name)
                        
                        try:
                            add_log(f"User {user_email} joined job log stream")
                        except Exception:
                            pass
                        
                        # Send existing logs
                        try:
                            from v2.common.logger import get_job_logs
                            existing_logs = get_job_logs(job_id_str)
                            for log in existing_logs:
                                await self.sio.emit('job_log', log, room=sid)
                        except Exception as log_error:
                            add_log(f"SocketIO: Error getting job logs: {str(log_error)}")
                        
                        await self.sio.emit('joined_job_logs', {
                            'job_id': job_id_str,
                            'session_id': session_id_str,
                            'status': 'joined',
                            'room': log_room_name
                        }, room=sid)
                        print(f"[API LAYER] User {user_email} joined job log streaming: {job_id_str}")
                    else:
                        await self.sio.emit('join_logs_error', {'job_id': job_id_str, 'error': 'Access denied: Job not found in your session'}, room=sid)
                        print(f"[API LAYER] Job log access denied for user {user_email} to job {job_id_str}")
                else:
                    await self.sio.emit('join_logs_error', {'job_id': job_id_str, 'error': 'Job not found'}, room=sid)
            except Exception as e:
                add_log(f"WS join_job_logs error: {str(e)}")
                print(f"[API LAYER] ❌ Error in join_job_logs handler: {e}")
                await self.sio.emit('join_logs_error', {'error': str(e)}, room=sid)
        
        @self.sio.on('leave_job_logs')
        async def handle_leave_job_logs(sid, data: Dict[str, Any]):
            """Handle client leaving a job log streaming room"""
            job_id = data.get('job_id')
            user_email = data.get('user_email', '')
            
            if job_id:
                log_room_name = f'job_logs_{job_id}'
                await self.sio.leave_room(sid, log_room_name)
                await self.sio.emit('left_job_logs', {
                    'job_id': job_id,
                    'status': 'left',
                    'room': log_room_name
                }, room=sid)
                print(f"[API LAYER] User {user_email} left job log streaming: {job_id}")
                try:
                    add_log(f"User {user_email} left job log stream")
                except Exception:
                    pass
    
    # ── Recognized progress stages that warrant a performance milestone ──
    # These are the stage names emitted by the execution layer via
    # progress_callback(). Noisy per-LLM-call events are excluded.
    _MILESTONE_WORTHY_STAGES: Dict[str, str] = {
        "Query Analysis":       "Progress: Query analysis",
        "EDA Started":          "Progress: EDA started",
        "EDA In Progress":      "Progress: EDA in progress",
        "Hypothesis Testing":   "Progress: Hypothesis testing",
        "Story Generation":     "Progress: Story/narrative generation",
        "Report Generation":    "Progress: Report generation",
        "Analysis Complete":    "Progress: Analysis complete",
        "Analysis Cancelled":   "Progress: Analysis cancelled",
        "Analysis Failed":      "Progress: Analysis failed",
    }

    async def _maybe_create_progress_milestone(
        self,
        job_id_str: str,
        data: Dict[str, Any],
        job_manager,
    ) -> None:
        """
        Create a Firestore milestone from a SocketIO progress event if the
        stage is recognized (not a noisy LLM thinking log).

        This lets the Performance Report capture sub-task timing from
        execution-layer progress_callback() events that arrive via SocketIO.
        Each stage is recorded at most once per job.
        """
        stage = data.get("stage", "")
        if not stage or not job_manager:
            return

        # Skip noisy per-LLM-call events (e.g. "🤖 EDA LLM #3")
        if stage.startswith("\U0001f916") or stage.startswith("🤖"):
            return

        milestone_desc = self._MILESTONE_WORTHY_STAGES.get(stage)
        if not milestone_desc:
            return

        # Check if we've already created this milestone for this job
        created_set = self._progress_milestones_created.setdefault(job_id_str, set())
        if stage in created_set:
            return
        created_set.add(stage)

        # Use consistent Progress: X format for report processing (no message append)
        # Sequence matches execution_layer: Query analysis -> EDA -> Hypothesis -> Story -> Report -> Complete

        try:
            from v2.modules.job_framework.models.job_model import MilestoneCreate

            milestone_data = MilestoneCreate(
                job_id=job_id_str,
                name=f"progress_{stage[:40].replace(' ', '_').lower()}",
                description=milestone_desc,
            )
            await job_manager._save_milestone(milestone_data)
            add_log(f"[PERF] Created progress milestone for job {job_id_str}: {stage}")
        except Exception as ms_err:
            # Non-fatal — don't break progress forwarding
            add_log(f"[PERF] Failed to create progress milestone: {str(ms_err)}")

    async def _monitor_job_status(self, job_id_str: str, job_manager, room_name: str):
        """
        Monitor job status and emit updates to the room.
        This matches v1's job monitoring behavior but uses asyncio instead of threads.
        
        Emits:
        - job_status: Periodic status updates
        - job_complete: When job reaches terminal state
        - job_error: If monitoring encounters an error
        """
        try:
            print(f"[API LAYER] 📊 Starting job status monitoring for: {job_id_str}")
            add_log(f"Started job status monitoring for {job_id_str}")
            
            while True:
                try:
                    # Get job status from job_manager
                    job_response = await job_manager._get_job_by_id(job_id_str)
                    
                    if not job_response:
                        print(f"[API LAYER] Job {job_id_str} not found, stopping monitoring")
                        break
                    
                    status_str = job_response.status.value if hasattr(job_response.status, 'value') else str(job_response.status)
                    
                    # Create status data
                    status_data = {
                        'job_id': job_response.job_id,
                        'status': status_str,
                        'created_at': job_response.created_on.isoformat() if job_response.created_on else None,
                        'started_at': job_response.started_at.isoformat() if job_response.started_at else None,
                        'completed_at': job_response.completed_at.isoformat() if job_response.completed_at else None,
                        'error': job_response.error_message
                    }
                    
                    # Emit status update to room
                    try:
                        await self.sio.emit('job_status', status_data, room=room_name)
                    except Exception as emit_error:
                        # Check if this is a client disconnect/abort
                        if 'disconnect' in str(emit_error).lower() or 'abort' in str(emit_error).lower():
                            print(f"[API LAYER] Client disconnected from job {job_id_str} monitoring - exiting cleanly")
                            break
                        print(f"[API LAYER] Error emitting job_status: {emit_error}")
                    
                    # If job is complete, send final status and break
                    if status_str in ['completed', 'error', 'cancelled']:
                        print(f"[API LAYER] 🏁 Job {status_str}: {job_id_str} - Final status reached")
                        add_log(f"Job monitoring detected final status: {status_str}")
                        try:
                            await self.sio.emit('job_complete', status_data, room=room_name)
                        except Exception as emit_error:
                            print(f"[API LAYER] Error emitting job_complete: {emit_error}")
                        break
                    
                    # Poll every 2 seconds (matching v1's behavior)
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    add_log(f"WS job_monitoring error: {str(e)}")
                    print(f"[API LAYER] ❌ Job monitoring error for {job_id_str}: {str(e)}")
                    error_data = {'job_id': job_id_str, 'error': str(e)}
                    try:
                        await self.sio.emit('job_error', error_data, room=room_name)
                    except Exception as emit_error:
                        print(f"[API LAYER] Error emitting job_error: {emit_error}")
                    break
                    
        except Exception as e:
            add_log(f"WS job_monitoring fatal error: {str(e)}")
            print(f"[API LAYER] ❌ Fatal error in job monitoring for {job_id_str}: {str(e)}")
        finally:
            # Clean up monitoring task reference and progress milestone tracking
            async with self.monitoring_lock:
                self.active_monitoring_tasks.pop(job_id_str, None)
                self._progress_milestones_created.pop(job_id_str, None)
                add_log(f"WS job_monitoring cleanup: {job_id_str}")
            print(f"[API LAYER] 🧹 Cleaned up monitoring task for job: {job_id_str}")


# Global SocketIO manager instance
_socketio_manager: Optional[SocketIOManager] = None


def get_socketio_manager() -> SocketIOManager:
    """Get or create the global SocketIO manager instance"""
    global _socketio_manager
    if _socketio_manager is None:
        _socketio_manager = SocketIOManager()
    return _socketio_manager
