from flask import Blueprint, request, jsonify, g, current_app
from logger import add_log
import traceback
import os

# Create email management blueprint
email_bp = Blueprint('email', __name__, url_prefix='/emails')

def get_firebase_user_manager():
    """Get Firebase user manager from current app"""
    return current_app.firebase_user_manager

def verify_api_key(api_key: str) -> bool:
    """Verify API key for email management endpoints"""
    email_management_api_key = os.getenv("EMAIL_MANAGEMENT_API_KEY", "")
    if not api_key or not email_management_api_key:
        return False
    return api_key == email_management_api_key

@email_bp.before_request
def require_api_key():
    """Require API key authentication for all email routes"""
    # Get API key from headers
    api_key = request.headers.get('X-API-Key') or request.headers.get('API-Key')
    
    if not api_key:
        return jsonify({
            'error': 'API Key Required',
            'message': 'Email management endpoints require X-API-Key header'
        }), 401
    
    if not verify_api_key(api_key):
        return jsonify({
            'error': 'Invalid API Key',
            'message': 'The provided API key is invalid'
        }), 401
    
    # Set a flag to indicate API key authentication was used
    g.api_key_auth = True
    g.user = {'email': 'api-key-user', 'name': 'API Key User'}
    print("📧 Email management request authenticated with API key")

@email_bp.route('', methods=['POST'])
def add_email():
    """Add an email to the authorized users in Firestore"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Get email from request body
        data = request.get_json()
        if not data or 'email' not in data:
            return jsonify({'error': 'Missing email in request body'}), 400
        
        email = data['email'].strip().lower()
        
        # Validate email format (basic validation)
        if '@' not in email or '.' not in email.split('@')[-1]:
            return jsonify({'error': 'Invalid email format'}), 400
        
        # Check if email already exists in Firestore
        if firebase_user_manager.is_user_authorized(email):
            return jsonify({'message': 'Email already exists', 'email': email}), 200
        
        # Add user to Firestore
        success = firebase_user_manager.add_user_email(email)
        
        if success:
            all_emails = firebase_user_manager.get_authorized_emails()
            add_log(f"Email added via API key to Firestore: {email}")
            return jsonify({
                'message': 'Email added successfully and is now authorized for API access',
                'email': email,
                'total_emails': len(all_emails),
                'authentication_method': 'API Key',
                'storage': 'Firestore'
            }), 201
        else:
            return jsonify({'error': 'Failed to add email to Firestore'}), 500
        
    except Exception as e:
        add_log(f"Error adding email: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to add email: {str(e)}'}), 500

@email_bp.route('', methods=['GET'])
def get_emails():
    """Get all emails from Firestore"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Get all emails from Firestore
        all_emails = firebase_user_manager.get_authorized_emails()
        
        # API key authenticated users get full access
        if hasattr(g, 'api_key_auth') and g.api_key_auth:
            add_log("Email list accessed via API key from Firestore")
        
        add_log(f"Emails fetched via API key: total={len(all_emails)}")
        return jsonify({
            'emails': all_emails,
            'total': len(all_emails),
            'storage': 'Firestore'
        })
        
    except Exception as e:
        add_log(f"Error getting emails: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to get emails: {str(e)}'}), 500

@email_bp.route('/<string:old_email>', methods=['PUT'])
def update_email(old_email):
    """Update an existing email in Firestore"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Get new email from request body
        data = request.get_json()
        if not data or 'email' not in data:
            return jsonify({'error': 'Missing new email in request body'}), 400
        
        new_email = data['email'].strip().lower()
        old_email = old_email.strip().lower()
        
        # Validate new email format
        if '@' not in new_email or '.' not in new_email.split('@')[-1]:
            return jsonify({'error': 'Invalid email format'}), 400
        
        # Check if old email exists
        old_user = firebase_user_manager.get_user_by_email(old_email)
        if not old_user:
            return jsonify({'error': 'Original email not found'}), 404
        
        # Check if new email already exists (and it's different from old email)
        if new_email != old_email and firebase_user_manager.is_user_authorized(new_email):
            return jsonify({'error': 'New email already exists'}), 409
        
        # If emails are different, we need to create a new document and delete the old one
        if new_email != old_email:
            # Add the new user
            success = firebase_user_manager.add_user_email(new_email)
            if success:
                # Update the new user with the correct role
                firebase_user_manager.update_user(new_email, {'role': old_user.get('role', 'user')})
                # Delete the old user
                firebase_user_manager.delete_user(old_email)
        
        all_emails = firebase_user_manager.get_authorized_emails()
        add_log(f"Email updated via API key in Firestore: {old_email} -> {new_email}")
        return jsonify({
            'message': 'Email updated successfully',
            'old_email': old_email,
            'new_email': new_email,
            'total_emails': len(all_emails),
            'authentication_method': 'API Key',
            'storage': 'Firestore'
        }), 200
        
    except Exception as e:
        add_log(f"Error updating email: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to update email: {str(e)}'}), 500

@email_bp.route('/<string:email>', methods=['DELETE'])
def delete_email(email):
    """Delete an email from Firestore"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        email = email.strip().lower()
        
        # Check if email exists
        if not firebase_user_manager.is_user_authorized(email):
            return jsonify({'error': 'Email not found'}), 404
        
        # Delete the user
        success = firebase_user_manager.delete_user(email)
        
        if success:
            all_emails = firebase_user_manager.get_authorized_emails()
            add_log(f"Email deleted via API key from Firestore: {email}")
            return jsonify({
                'message': 'Email deleted successfully',
                'deleted_email': email,
                'total_emails': len(all_emails),
                'authentication_method': 'API Key',
                'storage': 'Firestore'
            }), 200
        else:
            return jsonify({'error': 'Failed to delete email from Firestore'}), 500
        
    except Exception as e:
        add_log(f"Error deleting email: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to delete email: {str(e)}'}), 500

@email_bp.route('/bulk', methods=['DELETE'])
def delete_all_emails():
    """Delete all emails from Firestore"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Get current email count
        current_emails = firebase_user_manager.get_authorized_emails()
        deleted_count = len(current_emails)
        
        # Clear all users
        success = firebase_user_manager.clear_all_users()
        
        if success:
            add_log(f"All emails deleted via API key from Firestore: {deleted_count} emails removed")
            return jsonify({
                'message': 'All emails deleted successfully',
                'deleted_count': deleted_count,
                'total_emails': 0,
                'authentication_method': 'API Key',
                'storage': 'Firestore'
            }), 200
        else:
            return jsonify({'error': 'Failed to delete all emails from Firestore'}), 500
        
    except Exception as e:
        add_log(f"Error deleting all emails: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to delete all emails: {str(e)}'}), 500

@email_bp.route('/status', methods=['GET'])
def email_system_status():
    """Get system status for email management"""
    try:
        firebase_user_manager = get_firebase_user_manager()
        
        # Check Firebase availability
        firebase_available = bool(firebase_user_manager.db)
        
        if not firebase_available:
            return jsonify({
                'system_state': 'firebase_unavailable',
                'message': 'Firebase is not connected - email management unavailable',
                'firebase_connected': False,
                'total_authorized_emails': 0,
                'storage': 'None',
                'available_actions': {
                    'add_email': False,
                    'view_emails': False,
                    'manage_emails': False,
                    'delete_emails': False,
                    'update_emails': False
                }
            })
        
        # Load current emails from Firestore
        current_allowed_emails = firebase_user_manager.get_authorized_emails()
        
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
        
    except Exception as e:
        add_log(f"Error getting email system status: {str(e)} | traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to get system status: {str(e)}'}), 500
