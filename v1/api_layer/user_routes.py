
from flask import Blueprint, request, jsonify, g
from logger import add_log
import traceback

# Create user management blueprint
user_bp = Blueprint('user', __name__, url_prefix='/users')

def get_firebase_user_manager():
    """Get Firebase user manager from current app"""
    from flask import current_app
    return current_app.firebase_user_manager

def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'User operation failed'}), status

@user_bp.route('/<string:email>', methods=['GET'])
def get_user(email):
    """Get user information by email"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        email = email.strip().lower()
        
        # Check if email exists in Firestore
        exists = firebase_user_manager.is_user_authorized(email)
        user_data = None
        
        if exists:
            user_data = firebase_user_manager.get_user_by_email(email)
        
        response_data = {
            'email': email,
            'exists': exists,
            'authorized': exists,
        }
        
        # Add additional user info if available
        if user_data:
            response_data['role'] = user_data.get('role', 'user')
            response_data['status'] = user_data.get('status', 'active')
            response_data['created_at'] = user_data.get('created_at')
            response_data['updated_at'] = user_data.get('updated_at')
            response_data['registration_type'] = user_data.get('registration_type')
        
        add_log(f"User get_user success: {email}, exists={exists}")
        return jsonify(response_data), 200
        
    except Exception as e:
        add_log(f"Error checking user {email}: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to get user: {str(e)}', 500)

@user_bp.route('', methods=['GET'])
def get_all_users():
    """Get all users (requires admin role)"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Check if user has admin role
        user_info = g.get('user', {})
        user_role = user_info.get('role', 'user')
        
        if user_role != 'admin':
            return fail('Admin role required to view all users', 403)
        
        # Get all users from Firestore
        all_users = firebase_user_manager.get_all_users()
        
        add_log(f"All users retrieved by admin: {user_info.get('email', 'unknown')}")
        add_log(f"User get_all_users success: total={len(all_users)}")
        return jsonify({
            'users': all_users,
            'total': len(all_users),
        })
        
    except Exception as e:
        add_log(f"Error getting all users: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to get users: {str(e)}', 500)

@user_bp.route('/<string:email>/role', methods=['PUT'])
def update_user_role(email):
    """Update user role (admin only)"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Check if user has admin role
        user_info = g.get('user', {})
        user_role = user_info.get('role', 'user')
        
        if user_role != 'admin':
            return fail('Admin role required to update user roles', 403)
        
        # Get new role from request body
        data = request.get_json()
        if not data or 'role' not in data:
            return fail('Missing role in request body', 400)
        
        new_role = data['role'].strip().lower()
        email = email.strip().lower()
        
        # Validate role
        valid_roles = ['user', 'admin']
        if new_role not in valid_roles:
            return fail(f'Role must be one of: {", ".join(valid_roles)}', 400)
        
        # Check if user exists
        if not firebase_user_manager.is_user_authorized(email):
            return fail('User not found', 404)
        
        # Update user role
        success = firebase_user_manager.update_user(email, {'role': new_role})
        
        if success:
            add_log(f"User role updated by admin {user_info.get('email', 'unknown')}: {email} -> {new_role}")
            return jsonify({
                'message': 'User role updated successfully',
                'email': email,
                'new_role': new_role,
                'updated_by': user_info.get('email', 'unknown')
            }), 200
        else:
            return fail('Failed to update user role', 500)
        
    except Exception as e:
        add_log(f"Error updating user role: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to update user role: {str(e)}', 500)

@user_bp.route('/me', methods=['GET'])
def get_current_user():
    """Get current user information"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Get current user info
        user_info = g.get('user', {})
        email = user_info.get('email', '').lower()
        
        if not email:
            return fail('User not authenticated', 401)
        
        # Get user data from Firestore
        user_data = firebase_user_manager.get_user_by_email(email)
        
        if not user_data:
            return fail('User not found', 404)
        
        # Combine token info with Firestore data
        response_data = {
            'email': user_data.get('email'),
            'name': user_info.get('name', ''),
            'picture': user_info.get('picture', ''),
            'role': user_data.get('role', 'user'),
            'status': user_data.get('status', 'active'),
            'created_at': user_data.get('created_at'),
            'updated_at': user_data.get('updated_at'),
            'auth_provider': 'google'
        }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        add_log(f"Error getting current user: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to get current user: {str(e)}', 500)

@user_bp.route('/<string:email>/status', methods=['PUT'])
def update_user_status(email):
    """Update user status (admin only)"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Check if user has admin role
        user_info = g.get('user', {})
        user_role = user_info.get('role', 'user')
        
        if user_role != 'admin':
            return fail('Admin role required to update user status', 403)
        
        # Get new status from request body
        data = request.get_json()
        if not data or 'status' not in data:
            return fail('Missing status in request body', 400)
        
        new_status = data['status'].strip().lower()
        email = email.strip().lower()
        
        # Validate status
        valid_statuses = ['active', 'inactive', 'suspended']
        if new_status not in valid_statuses:
            return fail(f'Status must be one of: {", ".join(valid_statuses)}', 400)
        
        # Check if user exists
        if not firebase_user_manager.is_user_authorized(email):
            return fail('User not found', 404)
        
        # Update user status
        success = firebase_user_manager.update_user(email, {'status': new_status})
        
        if success:
            add_log(f"User status updated by admin {user_info.get('email', 'unknown')}: {email} -> {new_status}")
            return jsonify({
                'message': 'User status updated successfully',
                'email': email,
                'new_status': new_status,
                'updated_by': user_info.get('email', 'unknown')
            }), 200
        else:
            return fail('Failed to update user status', 500)
        
    except Exception as e:
        add_log(f"Error updating user status: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to update user status: {str(e)}', 500)
