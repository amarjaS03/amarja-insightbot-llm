from flask import request
from flask_socketio import emit, join_room, leave_room
import threading
import time
from logger import add_log, add_job_log


def register_socketio_events(socketio, job_manager, session_manager):
    """Register WebSocket event handlers for job progress streaming.

    This mirrors the handlers defined in ApiServer.register_socketio_events
    without changing functionality.
    """
    
    # Track active monitoring threads to prevent duplicates
    active_monitoring_threads = {}
    monitoring_lock = threading.Lock()

    @socketio.on('connect')
    def handle_connect():
        try:
            print(f"[API LAYER] WebSocket client connected: {request.sid}")
        except Exception as e:
            add_log(f"WS connect handler error: {str(e)}")
            print(f"[API LAYER] Error in connect handler: {e}")
            try:
                add_log(f"WS connect handler error: {str(e)}")
            except Exception:
                pass

    @socketio.on('disconnect')
    def handle_disconnect():
        try:
            print(f"[API LAYER] WebSocket client disconnected: {request.sid}")
            # Clean up any monitoring threads for this client
            with monitoring_lock:
                # Note: We can't easily track which threads belong to which client
                # The threads will naturally stop when the job completes or fails
                pass
        except Exception as e:
            add_log(f"WS disconnect handler error: {str(e)}")
            print(f"[API LAYER] Error in disconnect handler: {e}")
            try:
                add_log(f"WS disconnect handler error: {str(e)}")
            except Exception:
                pass

    @socketio.on('execution_progress')
    def handle_execution_progress(data):
        """Forward progress events from execution layer to frontend"""
        try:
            job_id = data.get('job_id')
            if job_id:
                try:
                    add_log(job_id, f"Forwarding execution progress: {data.get('stage','')} - {data.get('message','')}")
                except Exception:
                    pass
                print(f"[API LAYER] 📡 Forwarding progress for job {job_id}: {data.get('emoji', '')} {data.get('stage', '')} - {data.get('message', '')}")
                job_info = job_manager.get_job(job_id)
                if job_info:
                    session_id = job_info.get('session_id', '')
                    if session_id:
                        room_name = f'session_{session_id}_job_{job_id}'
                        socketio.emit('job_progress', data, room=room_name)
                        print(f"[API LAYER] ✅ Progress forwarded to room: {room_name}")
                    else:
                        socketio.emit('job_progress', data, room=f'job_{job_id}')
                        print(f"[API LAYER] ✅ Progress forwarded to room: job_{job_id}")
                else:
                    print(f"[API LAYER] ⚠️  Job not found for progress event: {job_id}")
        except Exception as e:
            add_log(f"WS execution_progress error: {str(e)}")
            print(f"[API LAYER] ❌ Error forwarding execution progress: {e}")
            try:
                add_log(f"WS progress forwarding error: {str(e)}")
            except Exception:
                pass

    @socketio.on('join_job')
    def handle_join_job(data):
        job_id = data.get('job_id')
        user_email = data.get('user_email', '').lower()
        session_id = data.get('session_id', '')

        if job_id and user_email and session_id:
            if not session_manager.check_session_ownership(session_id, user_email):
                emit('join_error', {'job_id': job_id, 'error': 'Access denied: You do not own this session'})
                print(f"[API LAYER] Session access denied for user {user_email} to session {session_id}")
                return

            job_info = job_manager.get_job(job_id)
            if not job_info:
                print(f"[API LAYER] ❌ Job {job_id} not found in memory for user {user_email}")
                emit('join_error', {'job_id': job_id, 'error': f'Job {job_id} not found'})
                return
            
            if job_info:
                try:
                    add_job_log(job_id, f"User {user_email} joined job monitoring in session {session_id}")
                except Exception:
                    pass
                job_user_info = job_info.get('user_info', {})
                job_user_email = job_user_info.get('email', '').lower()
                job_session_id = job_info.get('session_id', '')

                if job_user_email == user_email and job_session_id == session_id:
                    room_name = f'session_{session_id}_job_{job_id}'
                    join_room(room_name)

                    # Check if monitoring is already active for this job to prevent duplicates
                    with monitoring_lock:
                        if job_id not in active_monitoring_threads:
                            def run_monitoring():
                                try:
                                    print(f"[API LAYER] 📊 Starting job status monitoring for: {job_id}")
                                    while True:
                                        ji = job_manager.get_job(job_id)
                                        if not ji:
                                            print(f"[API LAYER] Job {job_id} not found in memory, stopping monitoring")
                                            break
                                        status = ji['status']
                                        status_str = getattr(status, 'value', status)
                                        status_data = {
                                            'job_id': ji['job_id'],
                                            'status': status_str,
                                            'created_at': ji['created_at'],
                                            'started_at': ji.get('started_at'),
                                            'completed_at': ji.get('completed_at'),
                                            'error': ji.get('error')
                                        }
                                        sid = ji.get('session_id', '')
                                        if sid:
                                            room = f'session_{sid}_job_{job_id}'
                                            try:
                                                socketio.emit('job_status', status_data, room=room)
                                            except Exception as emit_error:
                                                # Check if this is a client disconnect/abort
                                                if 'disconnect' in str(emit_error).lower() or 'abort' in str(emit_error).lower():
                                                    print(f"[API LAYER] Client disconnected from job {job_id} monitoring - exiting cleanly")
                                                    break
                                                print(f"[API LAYER] Error emitting job_status: {emit_error}")
                                        else:
                                            try:
                                                socketio.emit('job_status', status_data, room=f'job_{job_id}')
                                            except Exception as emit_error:
                                                # Check if this is a client disconnect/abort
                                                if 'disconnect' in str(emit_error).lower() or 'abort' in str(emit_error).lower():
                                                    print(f"[API LAYER] Client disconnected from job {job_id} monitoring - exiting cleanly")
                                                    break
                                                print(f"[API LAYER] Error emitting job_status: {emit_error}")
                                        if status_str in ['completed', 'failed', 'cancelled']:
                                            print(f"[API LAYER] 🏁 Job {status_str}: {job_id} - Final status reached")
                                            if sid:
                                                room = f'session_{sid}_job_{job_id}'
                                                try:
                                                    socketio.emit('job_complete', status_data, room=room)
                                                except Exception as emit_error:
                                                    print(f"[API LAYER] Error emitting job_complete: {emit_error}")
                                            else:
                                                try:
                                                    socketio.emit('job_complete', status_data, room=f'job_{job_id}')
                                                except Exception as emit_error:
                                                    print(f"[API LAYER] Error emitting job_complete: {emit_error}")
                                            break
                                        time.sleep(2)
                                except Exception as e:
                                    add_log(f"WS job_monitoring error: {str(e)}")
                                    print(f"[API LAYER] ❌ Job monitoring error for {job_id}: {str(e)}")
                                    error_data = {'job_id': job_id, 'error': str(e)}
                                    ji = job_manager.get_job(job_id)
                                    if ji:
                                        sid = ji.get('session_id', '')
                                        if sid:
                                            room = f'session_{sid}_job_{job_id}'
                                            try:
                                                socketio.emit('job_error', error_data, room=room)
                                            except Exception as emit_error:
                                                print(f"[API LAYER] Error emitting job_error: {emit_error}")
                                        else:
                                            try:
                                                socketio.emit('job_error', error_data, room=f'job_{job_id}')
                                            except Exception as emit_error:
                                                print(f"[API LAYER] Error emitting job_error: {emit_error}")
                                    else:
                                        try:
                                            socketio.emit('job_error', error_data, room=f'job_{job_id}')
                                        except Exception as emit_error:
                                            print(f"[API LAYER] Error emitting job_error: {emit_error}")
                                finally:
                                    # Clean up monitoring thread reference
                                    with monitoring_lock:
                                        active_monitoring_threads.pop(job_id, None)
                                        add_log(f"WS job_monitoring cleanup: {job_id}")
                                    print(f"[API LAYER] 🧹 Cleaned up monitoring thread for job: {job_id}")

                            # Start monitoring thread and track it
                            monitoring_thread = threading.Thread(target=run_monitoring, daemon=True)
                            active_monitoring_threads[job_id] = monitoring_thread
                            monitoring_thread.start()
                            print(f"[API LAYER] 🚀 Started monitoring thread for job: {job_id}")
                        else:
                            print(f"[API LAYER] ⚠️  Monitoring already active for job: {job_id}")

                    # Send current job status immediately for reconnection scenarios
                    current_status = job_info['status']
                    status_str = getattr(current_status, 'value', current_status)
                    current_status_data = {
                        'job_id': job_info['job_id'],
                        'status': status_str,
                        'created_at': job_info['created_at'],
                        'started_at': job_info.get('started_at'),
                        'completed_at': job_info.get('completed_at'),
                        'error': job_info.get('error')
                    }
                    emit('job_status', current_status_data)
                    
                    # If job is already complete, send completion event
                    if status_str in ['completed', 'failed', 'cancelled']:
                        emit('job_complete', current_status_data)
                        print(f"[API LAYER] 🔄 Reconnection: Job {job_id} already {status_str}")

                    emit('joined_job', {'job_id': job_id, 'session_id': session_id, 'status': 'joined', 'room': room_name})
                    print(f"[API LAYER] User {user_email} joined session-aware job monitoring: {job_id} in session {session_id}")
                else:
                    emit('join_error', {'job_id': job_id, 'error': 'Access denied: Job not found in your session'})
                    print(f"[API LAYER] Job access denied for user {user_email} to job {job_id}")
            else:
                emit('join_error', {'job_id': job_id, 'error': 'Job not found'})
        else:
            emit('join_error', {'error': 'Missing job_id, user_email, or session_id'})

    @socketio.on('leave_job')
    def handle_leave_job(data):
        job_id = data.get('job_id')
        user_email = data.get('user_email', '').lower()
        session_id = data.get('session_id', '')

        if job_id and user_email and session_id:
            room_name = f'session_{session_id}_job_{job_id}'
            leave_room(room_name)
            emit('left_job', {'job_id': job_id, 'session_id': session_id, 'status': 'left', 'room': room_name})
            print(f"[API LAYER] User {user_email} left session-aware job monitoring: {job_id} in session {session_id}")
            try:
                add_log(job_id, f"User {user_email} left job monitoring in session {session_id}")
            except Exception:
                pass

    @socketio.on('join_job_logs')
    def handle_join_job_logs(data):
        job_id = data.get('job_id')
        user_email = data.get('user_email', '').lower()
        session_id = data.get('session_id', '')

        if job_id and user_email and session_id:
            if not session_manager.check_session_ownership(session_id, user_email):
                emit('join_logs_error', {'job_id': job_id, 'error': 'Access denied: You do not own this session'})
                print(f"[API LAYER] Log session access denied for user {user_email} to session {session_id}")
                return

            job_info = job_manager.get_job(job_id)
            if job_info:
                job_user_info = job_info.get('user_info', {})
                job_user_email = job_user_info.get('email', '').lower()
                job_session_id = job_info.get('session_id', '')

                if job_user_email == user_email and job_session_id == session_id:
                    log_room_name = f'job_logs_{job_id}'
                    join_room(log_room_name)
                    try:
                        add_log(job_id, f"User {user_email} joined job log stream")
                    except Exception:
                        pass

                    from logger import get_job_logs
                    existing_logs = get_job_logs(job_id)
                    for log in existing_logs:
                        emit('job_log', log)

                    emit('joined_job_logs', {'job_id': job_id, 'session_id': session_id, 'status': 'joined', 'room': log_room_name})
                    print(f"[API LAYER] User {user_email} joined job log streaming: {job_id}")
                else:
                    emit('join_logs_error', {'job_id': job_id, 'error': 'Access denied: Job not found in your session'})
                    print(f"[API LAYER] Job log access denied for user {user_email} to job {job_id}")
            else:
                emit('join_logs_error', {'job_id': job_id, 'error': 'Job not found'})
        else:
            emit('join_logs_error', {'error': 'Missing job_id, user_email, or session_id'})

    @socketio.on('leave_job_logs')
    def handle_leave_job_logs(data):
        job_id = data.get('job_id')
        user_email = data.get('user_email', '').lower()
        if job_id and user_email:
            log_room_name = f'job_logs_{job_id}'
            leave_room(log_room_name)
            emit('left_job_logs', {'job_id': job_id, 'status': 'left', 'room': log_room_name})
            print(f"[API LAYER] User {user_email} left job log streaming: {job_id}")
            try:
                add_log(job_id, f"User {user_email} left job log stream")
            except Exception:
                pass


