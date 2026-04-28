from flask import Blueprint, request, jsonify, g
from functools import wraps
from logger import add_log
import traceback
import os
from utils.env import GCP_PROJECT, KEY_FILE_PATH

admin_bp = Blueprint('admin', __name__, url_prefix='/api')

def fail(msg: str, status: int = 400):
        return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'Admin operation failed'}), status

def get_firebase_user_manager():
    """Get Firebase user manager from current app"""
    from flask import current_app
    if hasattr(current_app, 'firebase_user_manager'):
        return current_app.firebase_user_manager
    return None

def admin_required(f):
    """Decorator to require admin role for endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = getattr(g, 'user', None)
        if not user:
            return fail('Authentication required', 401)
        
        if user.get('role') != 'admin':
            return fail('Admin access required', 403)
            
        return f(*args, **kwargs)
    return decorated_function



def _get_secret_from_gcp(secret_id: str) -> str | None:
    """Retrieve a secret from Google Secret Manager using explicit keyfile credentials."""
    try:
        from google.cloud import secretmanager
        from google.oauth2 import service_account
    except Exception:
        return None
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            return None
        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        effective_project = os.getenv('GCP_PROJECT') or GCP_PROJECT or getattr(creds, 'project_id', None)
        if not effective_project:
            return None
        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        secret_name = f"projects/{effective_project}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        secret_value = response.payload.data.decode("UTF-8")
        return (secret_value or "").strip()
    except Exception:
        return None

def _verify_market_campaign_api_key(provided_key: str) -> bool:
    """Verify API key for market campaign endpoints from Secret Manager or env fallback."""
    if not provided_key:
        return False
    expected = os.getenv("market_campaign_api_key", "")
    if not expected:
        expected = _get_secret_from_gcp("market_campaign_api_key") or ""
    return bool(expected) and provided_key == expected


@admin_bp.route('/users', methods=['GET'])
@admin_required
def get_all_users():
    """Get all users (admin only)"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        users_data = firebase_user_manager.get_all_users()
        
        add_log(f"Admin get_all_users success: {len(users_data)} users")
        return jsonify({
            'success': True,
            'users': users_data
        }), 200
        
    except Exception as e:
        print(f"❌ Error fetching users: {str(e)}")
        add_log(f"Admin get_all_users error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to fetch users', 500)

@admin_bp.route('/users/<email>', methods=['GET'])
@admin_required
def get_user_by_email(email):
    """Get specific user by email (admin only)"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        user_data = firebase_user_manager.get_user_by_email(email)
        if user_data:
            add_log(f"Admin get_user_by_email success: {email}")
            return jsonify({
                'success': True,
                'user': user_data
            }), 200
        else:
            return fail('User not found', 404)
                
    except Exception as e:
        print(f"❌ Error fetching user {email}: {str(e)}")
        add_log(f"Admin get_user_by_email error for {email}: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to fetch user', 500)

@admin_bp.route('/users', methods=['POST'])
@admin_required
def create_user():
    """Create new user (admin only)"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['email', 'name', 'role', 'issued_token']
        for field in required_fields:
            if field not in data:
                return fail(f'Missing required field: {field}', 400)
        
        # Optional fields with defaults/normalization
        registration_type = str(data.get('registration_type', 'internal')).lower()
        auth_provider = data.get('auth_provider')  # Optional
        
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        # Check if user already exists
        if firebase_user_manager.is_user_authorized(data['email']):
            return fail('User already exists', 409)
        
        # Add user to Firestore
        success = firebase_user_manager.add_user_email(data['email'])
        if success:
            # Update user with additional data
            updates = {
                'name': data['name'],
                'role': data['role'],
                'issued_token': data['issued_token'],
                'used_token': 0,
                'report_count': 0,
                'registration_type': registration_type,
                'auth_provider': auth_provider
            }
            firebase_user_manager.update_user(data['email'], updates)
            
            add_log(f"Admin create_user success: {data['email']}")
            return jsonify({
                'success': True,
                'message': 'User created successfully'
            }), 201
        else:
            return fail('Failed to create user', 500)
            
    except Exception as e:
        print(f"❌ Error creating user: {str(e)}")
        add_log(f"Admin create_user error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to create user', 500)

@admin_bp.route('/users/<email>', methods=['PUT'])
@admin_required
def update_user(email):
    """Update user (admin only)"""
    try:
        data = request.get_json()
        
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        # Check if user exists
        existing_user = firebase_user_manager.get_user_by_email(email)
        if not existing_user:
            return fail('User not found', 404)
        
        # Update user
        success = firebase_user_manager.update_user(email, data)
        if success:
            add_log(f"Admin update_user success: {email}")
            return jsonify({
                'success': True,
                'message': 'User updated successfully'
            }), 200
        else:
            return fail('Failed to update user', 500)
                
    except Exception as e:
        print(f"❌ Error updating user {email}: {str(e)}")
        add_log(f"Admin update_user error for {email}: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to update user', 500)

@admin_bp.route('/users/<email>', methods=['DELETE'])
@admin_required
def delete_user(email):
    """Delete user (admin only)"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        # Check if user exists
        existing_user = firebase_user_manager.get_user_by_email(email)
        if not existing_user:
            return fail('User not found', 404)
        
        # Delete user
        success = firebase_user_manager.delete_user(email)
        if success:
            add_log(f"Admin delete_user success: {email}")
            return jsonify({
                'success': True,
                'message': 'User deleted successfully'
            }), 200
        else:
            return fail('Failed to delete user', 500)
                
    except Exception as e:
        print(f"❌ Error deleting user {email}: {str(e)}")
        add_log(f"Admin delete_user error for {email}: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to delete user', 500)

@admin_bp.route('/profile', methods=['GET'])
def get_current_user_profile():
    """Get current user's profile with role information"""
    try:
        user = getattr(g, 'user', None)
        if not user:
            return fail('Authentication required', 401)
        
        user_email = user.get('email')
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        user_data = firebase_user_manager.get_user_by_email(user_email)
        if user_data:
            add_log(f"Admin get_current_user_profile success: {user_email}")
            return jsonify({
                'success': True,
                'user': user_data
            }), 200
        else:
            # Create user if doesn't exist (first time login)
            success = firebase_user_manager.add_user_email(user_email)
            if success:
                # Update with additional data
                updates = {
                    'name': user.get('name', 'Unknown'),
                    'role': 'user',  # Default role
                    'used_token': 0,
                    'issued_token': 1000,  # Default allocation
                    'report_count': 0
                }
                firebase_user_manager.update_user(user_email, updates)
                
                user_data = firebase_user_manager.get_user_by_email(user_email)
                return jsonify({
                    'success': True,
                    'user': user_data
                }), 200
            else:
                return fail('Failed to create user profile', 500)
                
    except Exception as e:
        print(f"❌ Error fetching user profile: {str(e)}")
        add_log(f"Admin get_current_user_profile error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to fetch user profile', 500)

@admin_bp.route('/admin/stats', methods=['GET'])
@admin_required
def get_admin_stats():
    """Get admin dashboard statistics"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        # Get all users from Firestore
        users = firebase_user_manager.get_all_users()
        
        stats = {
            'total_users': len(users),
            'total_reports': sum(user.get('report_count', 0) for user in users),
            'total_tokens_used': sum(user.get('used_token', 0) for user in users),
            'active_users': len([user for user in users if user.get('report_count', 0) > 0])
        }
        
        add_log("Admin get_admin_stats success")
        return jsonify({
            'success': True,
            'stats': stats
        }), 200
        
    except Exception as e:
        print(f"❌ Error fetching admin stats: {str(e)}")
        add_log(f"Admin get_admin_stats error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to fetch statistics', 500)



@admin_bp.route('/market_campaign_user', methods=['POST'])
def create_market_campaign_user():
    """Create user via market campaign API key (bypasses bearer/global auth). Params same as /api/users POST."""
    try:
        api_key = request.headers.get('X-API-Key') or request.headers.get('API-Key')
        if not _verify_market_campaign_api_key(api_key):
            return fail('Invalid or missing API Key', 401)

        data = request.get_json() or {}

        # Validate required fields (same as create_user)
        required_fields = ['email', 'name', 'role', 'issued_token']
        for field in required_fields:
            if field not in data:
                return fail(f'Missing required field: {field}', 400)

        registration_type = str(data.get('registration_type', 'internal')).lower()
        auth_provider = data.get('auth_provider')

        firebase_user_manager = get_firebase_user_manager()
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)

        # Check if user already exists
        if firebase_user_manager.is_user_authorized(data['email']):
            return fail('User already exists', 409)

        # Add user
        success = firebase_user_manager.add_user_email(data['email'])
        if success:
            updates = {
                'name': data['name'],
                'role': data['role'],
                'issued_token': data['issued_token'],
                'used_token': 0,
                'report_count': 0,
                'registration_type': registration_type,
                'auth_provider': auth_provider,
                'created_via': 'market_campaign_api'
            }
            firebase_user_manager.update_user(data['email'], updates)

            return jsonify({
                'success': True,
                'message': 'User created successfully via market campaign API'
            }), 201
        else:
            return fail('Failed to create user', 500)

    except Exception as e:
        print(f"❌ Error creating market campaign user: {str(e)}")
        add_log(f"Market campaign create_user error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to create user', 500)

@admin_bp.route('/users/<email>/add-tokens', methods=['POST'])
@admin_required
def add_tokens_to_user(email):
    """Add tokens to user's existing token allocation (admin only)"""
    try:
        data = request.get_json()
        user = getattr(g, 'user', None)
        
        # Validate required fields
        if 'tokens_to_add' not in data:
            return fail('Missing required field: tokens_to_add', 400)
        
        tokens_to_add = data.get('tokens_to_add', 0)
        if not isinstance(tokens_to_add, (int, float)) or tokens_to_add <= 0:
            return fail('tokens_to_add must be a positive number', 400)
        
        reason = data.get('reason', '')  # Optional reason
        added_by = user.get('email') if user else 'system'
        
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        # Check if user exists
        existing_user = firebase_user_manager.get_user_by_email(email)
        if not existing_user:
            return fail('User not found', 404)
        
        # Add tokens with history tracking
        result = firebase_user_manager.add_tokens_with_history(
            user_email=email,
            tokens_to_add=int(tokens_to_add),
            added_by=added_by,
            reason=reason if reason else None
        )
        
        if result['success']:
            add_log(f"Admin add_tokens_to_user success: {email} +{tokens_to_add}")
            return jsonify({
                'success': True,
                'message': f'Added {tokens_to_add} tokens to user',
                'previous_tokens': result['previous_tokens'],
                'new_total': result['new_total'],
                'history_created': result.get('history_created', False)
            }), 200
        else:
            return fail(result.get('error', 'Failed to add tokens'), 500)
                
    except Exception as e:
        print(f"❌ Error adding tokens to user {email}: {str(e)}")
        add_log(f"Admin add_tokens_to_user error for {email}: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to add tokens', 500)

@admin_bp.route('/users/<email>/token-history', methods=['GET'])
@admin_required
def get_user_token_history(email):
    """Get token history for a specific user (admin only)"""
    try:
        # Get optional limit parameter
        limit = request.args.get('limit', 50, type=int)
        if limit < 1 or limit > 100:
            limit = 50  # Default safe limit
        
        firebase_user_manager = get_firebase_user_manager()
        
        if not firebase_user_manager or not firebase_user_manager.db:
            return fail('Firebase not available', 500)
        
        # Check if user exists
        existing_user = firebase_user_manager.get_user_by_email(email)
        if not existing_user:
            return fail('User not found', 404)
        
        # Get token history
        history_records = firebase_user_manager.get_user_token_history(email, limit)
        
        add_log(f"Admin get_user_token_history success: {email} {len(history_records)} records")
        return jsonify({
            'success': True,
            'history': history_records,
            'total_records': len(history_records)
        }), 200
        
    except Exception as e:
        print(f"❌ Error fetching token history for user {email}: {str(e)}")
        add_log(f"Admin get_user_token_history error for {email}: {str(e)} | traceback: {traceback.format_exc()}")
        return fail('Failed to fetch token history', 500)

@admin_bp.route('/admin/health', methods=['GET'])
@admin_required
def admin_health_check():
    """Admin health check endpoint"""
    firebase_user_manager = get_firebase_user_manager()
    firebase_available = firebase_user_manager and firebase_user_manager.db
    
    return jsonify({
        'status': 'healthy',
        'firebase_available': firebase_available
    }), 200
