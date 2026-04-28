"""
Credentials controller for FastAPI v2

Provides REST API endpoints for credential CRUD operations.
All endpoints follow v1 functionality and match v1 behavior.
"""
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Body, Query
from v2.common.model.api_response import ApiResponse
from v2.common.helper import to_dict
from v2.modules.credential_framework.manager.credential.credential_manager import CredentialManager
from v2.common.logger import add_log
from pydantic import BaseModel

from v2.modules.credential_framework.models.credential_model import CredentialApiResponse


router = APIRouter(prefix="/credentials")

def get_credential_manager() -> CredentialManager:
    """Dependency to get credential manager instance"""
    return CredentialManager()


class CredentialRequest(BaseModel):
    """Request model for credential operations"""
    user_id: str


class SaveCredentialRequest(CredentialRequest):
    """Request model for saving credentials"""
    connection_name: str
    credentials: Dict[str, Any]


class UpdateCredentialRequest(CredentialRequest):
    """Request model for updating credentials"""
    connection_name: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None


def handle_encryption_error(e: Exception) -> HTTPException:
    """
    Handle encryption-related errors and return user-friendly API responses.
    """
    error_msg = str(e)
    
    # Check for specific encryption error types
    if 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
        if 'not found' in error_msg.lower() or 'does not exist' in error_msg.lower():
            raise HTTPException(
                status_code=503,
                detail={
                    'result': 'fail',
                    'status_code': 503,
                    'message': 'Encryption service configuration error: Encryption key not found in Secret Manager. Please contact your system administrator.',
                    'error_code': 'ENCRYPTION_KEY_NOT_FOUND'
                }
            )
        elif 'permission' in error_msg.lower() or 'access' in error_msg.lower() or 'role' in error_msg.lower():
            raise HTTPException(
                status_code=503,
                detail={
                    'result': 'fail',
                    'status_code': 503,
                    'message': 'Encryption service configuration error: Insufficient permissions to access encryption key. Please contact your system administrator.',
                    'error_code': 'ENCRYPTION_KEY_ACCESS_DENIED'
                }
            )
        else:
            raise HTTPException(
                status_code=503,
                detail={
                    'result': 'fail',
                    'status_code': 503,
                    'message': 'Encryption service error: Unable to access encryption key from Secret Manager. Please contact your system administrator.',
                    'error_code': 'ENCRYPTION_SERVICE_ERROR'
                }
            )
    elif 'Invalid encryption key' in error_msg or ('invalid' in error_msg.lower() and 'key' in error_msg.lower()):
        raise HTTPException(
            status_code=503,
            detail={
                'result': 'fail',
                'status_code': 503,
                'message': 'Encryption service error: Invalid encryption key format. Please contact your system administrator.',
                'error_code': 'ENCRYPTION_KEY_INVALID'
            }
        )
    else:
        raise HTTPException(
            status_code=503,
            detail={
                'result': 'fail',
                'status_code': 503,
                'message': 'Credential encryption service error. Please contact your system administrator.',
                'error_code': 'ENCRYPTION_ERROR'
            }
        )


@router.post("/{connector_type}/save", response_model=CredentialApiResponse)
async def save_credential(
    connector_type: str,
    request_data: SaveCredentialRequest = Body(...),
    manager: CredentialManager = Depends(get_credential_manager)
):
    """
    Save a new credential for a connector type
    
    Collection: connections (with user_id and state fields)
    """
    try:
        result = await manager.save_credential(
            request_data.user_id,
            connector_type,
            request_data.connection_name,
            request_data.credentials
        )
        
        # if request_data.job_id:
        #     add_log(request_data.job_id, f"Credential saved for {connector_type} as {request_data.connection_name}")
        
        return CredentialApiResponse(
            status="success",
            statusCode=201,
            connector_type=connector_type,
            message=f"Credential saved successfully",
            data=result
        )
    except ValueError as e:
        error_msg = str(e)
        if 'already exists' in error_msg:
            # Return proper error response for duplicate credential name
            add_log(f"Duplicate credential name error: {error_msg}")
            error_response = {
                'status': 'fail',
                'statusCode': 409,
                'message': f'A credential with the same name already exists. Please use a different name.',
                'data': {
                    'error_code': 'DUPLICATE_CREDENTIAL_NAME',
                    'connection_name': request_data.connection_name,
                    'connector_type': connector_type
                }
            }
            raise HTTPException(status_code=409, detail=error_response)
        # Return proper error response for validation errors
        error_response = {
            'status': 'fail',
            'statusCode': 400,
            'message': error_msg,
            'data': {
                'error_code': 'VALIDATION_ERROR'
            }
        }
        raise HTTPException(status_code=400, detail=error_response)
    except (RuntimeError, ValueError) as e:
        error_msg = str(e)
        if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
            add_log(f"Encryption error saving credential: {error_msg}")
            handle_encryption_error(e)
        # Return proper error response for other errors
        error_response = {
            'status': 'fail',
            'statusCode': 500,
            'message': f'Error saving credential: {error_msg}',
            'data': {
                'error_code': 'CREDENTIAL_SAVE_ERROR'
            }
        }
        raise HTTPException(status_code=500, detail=error_response)
    except HTTPException:
        # Re-raise HTTPException as-is (from encryption errors, etc.)
        raise
    except Exception as e:
        add_log(f"Error saving credential: {str(e)}")
        error_response = {
            'status': 'fail',
            'statusCode': 500,
            'message': 'An unexpected error occurred while saving the credential. Please try again.',
            'data': {
                'error_code': 'UNEXPECTED_ERROR'
            }
        }
        raise HTTPException(status_code=500, detail=error_response)


@router.post("/all", response_model=CredentialApiResponse)
async def get_all_credentials(
    request_data: CredentialRequest = Body(...),
    manager: CredentialManager = Depends(get_credential_manager)
):
    """Get all credentials for all connector types"""
    try:
        result = await manager.get_all_connector_credentials(request_data.user_id)
        
        # if request_data.job_id:
        #     add_log(request_data.job_id, f"All credentials listed for user_id {request_data.user_id}")
        
        # Wrap the result in the desired format
        return CredentialApiResponse(
            status="success",
            statusCode=200,
            message="All credentials fetched successfully",
            data={
                'status': result.get('status', 'success'),
                'credentials': result.get('credentials', {})
            }
        )
    except (RuntimeError, ValueError) as e:
        error_msg = str(e)
        if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
            add_log(f"Encryption error retrieving all credentials: {error_msg}")
            handle_encryption_error(e)
        raise HTTPException(status_code=500, detail=f'Error retrieving credentials: {error_msg}')
    except Exception as e:
        add_log(f"Error retrieving all credentials: {str(e)}")
        raise HTTPException(status_code=500, detail='An unexpected error occurred while retrieving credentials. Please try again.')


@router.post("/{connector_type}", response_model=CredentialApiResponse)
async def get_connector_credentials(
    connector_type: str,
    request_data: CredentialRequest = Body(...),
    connection_id: Optional[str] = Query(None, description="Optional connection_id to filter the results"),
    manager: CredentialManager = Depends(get_credential_manager)
):
    """Get all credentials for a connector type, optionally filtered by connection_id"""
    try:
        # First, fetch all credentials for the connector type
        result = await manager.get_connector_credentials(
            request_data.user_id,
            connector_type
        )
        
        connections = result.get('connections', [])
        
        # If connection_id is provided, filter the connections
        if connection_id:
            filtered_connection = next(
                (conn for conn in connections if conn.get('connection_id') == connection_id),
                None
            )
            
            if not filtered_connection:
                raise HTTPException(
                    status_code=404, 
                    detail={
                        "status": "fail",
                        "statusCode": 404,
                        "message": f"Credential with connection_id '{connection_id}' not found for connector_type '{connector_type}'",
                        "data": {
                            "error_code": "CREDENTIAL_NOT_FOUND",
                            "connection_id": connection_id,
                            "connector_type": connector_type
                        }
                    }
                )
            
            # if request_data.job_id:
            #     add_log(request_data.job_id, f"Credential fetched: {connection_id} for connector_type: {connector_type}")
            
            return CredentialApiResponse(
                status="success",
                statusCode=200,
                message="Credential fetched successfully",
                connector_type=connector_type,
                data=filtered_connection
            )
        
        # Return all connections for the connector type
        # if request_data.job_id:
        #     add_log(request_data.job_id, f"Connector credentials listed for {connector_type}")
        
        return CredentialApiResponse(
            status="success",
            statusCode=200,
            message=f"Credentials fetched successfully",
            connector_type=result.get('connector_type', connector_type),
            data={
                'connections': result.get('connections', []),
                'total': result.get('total', len(result.get('connections', [])))
            }
        )
    except HTTPException:
        raise
    except (RuntimeError, ValueError) as e:
        error_msg = str(e)
        if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
            add_log(f"Encryption error retrieving credentials: {error_msg}")
            handle_encryption_error(e)
        raise HTTPException(status_code=500, detail=f'Error retrieving credentials: {error_msg}')
    except Exception as e:
        add_log(f"Error retrieving credentials: {str(e)}")
        raise HTTPException(status_code=500, detail='An unexpected error occurred while retrieving credentials. Please try again.')


@router.post("/{connection_id}/update", response_model=CredentialApiResponse)
async def update_credential(
    connection_id: str,
    request_data: UpdateCredentialRequest = Body(...),
    manager: CredentialManager = Depends(get_credential_manager)
):
    """
    Update credential document fields.
    Only the following fields are allowed to be updated:
      - connection_name
      - credentials  (raw dict or already-encrypted string)
      - updated_at   (will be overridden by server to current time)
    """
    try:
        updates = {}
        if request_data.connection_name is not None:
            updates['connection_name'] = request_data.connection_name
        if request_data.credentials is not None:
            updates['credentials'] = request_data.credentials
        
        if not updates:
            raise HTTPException(status_code=400, detail="At least one field (connection_name or credentials) must be provided for update")
        
        result = await manager.update_credential(
            request_data.user_id,
            connection_id,
            updates
        )
        
        print(f"UpdateResult: {result}")
        # if request_data.job_id:
        #     add_log(request_data.job_id, f"Credential updated: {connection_id} fields: {', '.join(result.get('updated_fields', []))}")
        
        return CredentialApiResponse(
            status="success",
            statusCode=200,
            connector_type=result.get('connector_type'),
            message=result.get('message', 'Credential updated successfully'),
            data=result
        )
    except (RuntimeError, ValueError) as e:
        error_msg = str(e)
        if 'encryption' in error_msg.lower() or 'Secret Manager' in error_msg or 'secret' in error_msg.lower():
            add_log(f"Encryption error updating credential: {error_msg}")
            handle_encryption_error(e)
        raise HTTPException(status_code=500, detail=f'Error updating credential: {error_msg}')
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"Error updating credential: {str(e)}")
        raise HTTPException(status_code=500, detail='An unexpected error occurred while updating the credential. Please try again.')


@router.post("/{connection_id}/delete", response_model=CredentialApiResponse)
async def delete_credential(
    connection_id: str,
    request_data: CredentialRequest = Body(...),
    manager: CredentialManager = Depends(get_credential_manager)
):
    """Delete a credential"""
    try:
        result = await manager.delete_credential(
            request_data.user_id,
            connection_id
        )
        
        # if request_data.job_id:
        #     add_log(request_data.job_id, f"Credential deleted: {connection_id}")
        
        return CredentialApiResponse(
            status="success",
            statusCode=200,
            message="Credential deleted successfully",
            data=result
        )
    except ValueError as e:
        # Credential not found
        error_msg = str(e)
        add_log(f"Credential not found error: {error_msg}")
        error_response = {
            'status': 'fail',
            'statusCode': 404,
            'message': error_msg,
            'data': {
                'error_code': 'CREDENTIAL_NOT_FOUND',
                'connection_id': connection_id
            }
        }
        raise HTTPException(status_code=404, detail=error_response)
    except RuntimeError as e:
        # Deletion failed
        error_msg = str(e)
        add_log(f"Error deleting credential: {error_msg}")
        error_response = {
            'status': 'fail',
            'statusCode': 500,
            'message': error_msg,
            'data': {
                'error_code': 'DELETE_ERROR',
                'connection_id': connection_id
            }
        }
        raise HTTPException(status_code=500, detail=error_response)
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"Unexpected error deleting credential: {str(e)}")
        error_response = {
            'status': 'fail',
            'statusCode': 500,
            'message': 'An unexpected error occurred while deleting the credential. Please try again.',
            'data': {
                'error_code': 'UNEXPECTED_ERROR',
                'connection_id': connection_id
            }
        }
        raise HTTPException(status_code=500, detail=error_response)


@router.post("/{connector_type}/{connection_id}/validate", response_model=CredentialApiResponse)
async def validate_credential(
    connector_type: str,
    connection_id: str,
    request_data: CredentialRequest = Body(...),
    manager: CredentialManager = Depends(get_credential_manager)
):
    """Validate a credential (mark as validated with current timestamp)"""
    try:
        result = await manager.validate_credential(
            request_data.user_id,
            connector_type,
            connection_id
        )
        
        # if request_data.job_id:
        #     add_log(request_data.job_id, f"Credential validated: {connector_type}/{connection_id}")
        
        return CredentialApiResponse(
            status="success",
            statusCode=200,
            message=result.get('message', 'Credential validated successfully')
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"Error validating credential: {str(e)}")
        raise HTTPException(status_code=500, detail=f'Error: {str(e)}')
