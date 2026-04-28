import urllib
from flask import Blueprint, request, jsonify
import os
import json
import re
import pandas as pd
from sqlalchemy import create_engine, inspect
from typing import Dict, Any
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound, AlreadyExists, PermissionDenied
from google.oauth2 import service_account
from logger import add_log
import traceback
from .data import _clear_session_input_data
from utils.env import KEY_FILE_PATH, GCP_PROJECT
from api_layer.firebase_data_models import get_data_manager, CredentialDocument


db_bp = Blueprint('db_bp', __name__)

def fail(msg: str, status: int = 400):
    return jsonify({'result': 'fail', 'status_code': status, 'message': msg, 'error': 'DB operation failed'}), status


def _validate_non_empty_string(value: Any, field: str, min_len: int = 1) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field} is required")
    if len(trimmed) < min_len:
        raise ValueError(f"{field} must be at least {min_len} characters")
    return trimmed


def _sanitize_secret_id_component(text: str) -> str:
    component = (text or '').strip().lower()
    component = re.sub(r'[^a-z0-9_-]+', '-', component)
    component = component.strip('-_') or 'user'
    return component[:128]


def _get_secret_from_gcp(secret_id: str) -> str:
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            raise RuntimeError("KEY_FILE_PATH is not set or file does not exist; cannot access Secret Manager without ADC")

        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        effective_project = os.getenv('GCP_PROJECT') or GCP_PROJECT or getattr(creds, 'project_id', None)
        if not effective_project:
            raise RuntimeError("GCP project not provided and could not be inferred from keyfile")

        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        secret_name = f"projects/{effective_project}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        secret_value = response.payload.data.decode("UTF-8")
        return secret_value.strip()
    except Exception as e:
        add_log(f"DB _get_secret_from_gcp error: Failed to retrieve secret {secret_id}: {str(e)}")
        raise RuntimeError(f"Failed to retrieve secret {secret_id}: {str(e)}")


def _store_secret_in_gcp(secret_id: str, payload: Dict[str, Any]) -> None:
    try:
        keyfile_path = KEY_FILE_PATH
        if not keyfile_path or not keyfile_path.exists():
            raise RuntimeError("KEY_FILE_PATH is not set or file does not exist; cannot access Secret Manager without ADC")

        creds = service_account.Credentials.from_service_account_file(str(keyfile_path.resolve()))
        effective_project = os.getenv('GCP_PROJECT') or GCP_PROJECT or getattr(creds, 'project_id', None)
        if not effective_project:
            raise RuntimeError("GCP project not provided and could not be inferred from keyfile")

        client = secretmanager.SecretManagerServiceClient(credentials=creds)

        parent = f"projects/{effective_project}"
        secret_name = f"projects/{effective_project}/secrets/{secret_id}"

        try:
            client.create_secret(
                parent=parent,
                secret_id=secret_id,
                secret={"replication": {"automatic": {}}},
            )
        except AlreadyExists:
            pass
        except PermissionDenied:
            pass

        payload_bytes = json.dumps(payload).encode('utf-8')
        client.add_secret_version(
            parent=secret_name,
            payload={"data": payload_bytes},
        )
    except Exception as e:
        add_log(f"DB _store_secret_in_gcp error: Failed to store secret {secret_id}: {str(e)}")
        raise RuntimeError(f"Failed to store secret: {str(e)}")


def get_secret_from_secret_manager(secret_name):
    try:
        return _get_secret_from_gcp(secret_name)
    except Exception as e:
        add_log(f"DB: Failed to retrieve secret {secret_name}: {str(e)}")
        return None


@db_bp.route('/db/check_credentials', methods=['POST'])
def check_db_credentials():
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        if job_id:
            add_log(job_id, "DB: check_credentials invoked")
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        connection_type = _validate_non_empty_string(data.get('connection_type'), 'connection_type', min_len=4).lower()
        
        original_user_email = user_email
        sanitized_email_for_secret = original_user_email.replace('@', '_').replace('.', '_')
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_db_{connection_type}"

        secret_exists = False
        try:
            creds = None
            if KEY_FILE_PATH and KEY_FILE_PATH.exists():
                creds = service_account.Credentials.from_service_account_file(str(KEY_FILE_PATH.resolve()))
            project_id = (
                os.getenv('GCP_PROJECT')
                or GCP_PROJECT
                or os.getenv('GOOGLE_CLOUD_PROJECT')
                or (getattr(creds, 'project_id', None) if creds else None)
            )
            if not project_id:
                raise RuntimeError("GCP project not configured. Set GCP_PROJECT or GOOGLE_CLOUD_PROJECT, or provide a keyfile with project_id.")
            client = secretmanager.SecretManagerServiceClient(credentials=creds) if creds else secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}"
            try:
                client.get_secret(name=secret_name)
                secret_exists = True
            except NotFound:
                secret_exists = False
            except PermissionDenied:
                # Treat permission denied as exists to avoid leaking existence
                secret_exists = True
        except Exception:
            secret_exists = False

        if secret_exists:
            return jsonify({
                'status': 'success',
                'status_code': 200,
                'message': 'Credentials already exist',
                'secret_id': secret_id
            }), 200

        return fail('Credentials not found for user', 404)
    except ValueError as ve:
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"DB check_credentials error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to check credentials: {str(e)}', 500)


@db_bp.route('/db/save_credentials', methods=['POST'])
def save_db_credentials():
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)

        connection_url = (data.get('connection_url') or '').strip()
        if not connection_url:
            # Attempt to build from parts with connection_type
            connection_type = _validate_non_empty_string(data.get('connection_type'), 'connection_type', min_len=4).lower()
            if connection_type not in ['mysql', 'mssql']:
                return fail('Unsupported connection_type. Use "mysql" or "mssql"', 400)

            username = _validate_non_empty_string(data.get('username'), 'username', min_len=1)
            password = _validate_non_empty_string(data.get('password'), 'password', min_len=1)
            host = _validate_non_empty_string(data.get('host'), 'host', min_len=1)
            port = _validate_non_empty_string(str(data.get('port')), 'port', min_len=1)
            database = _validate_non_empty_string(data.get('database'), 'database', min_len=1)

            if connection_type == 'mysql':
                driver = 'mysql+pymysql'
            else:
                driver = 'mssql+pymssql'
            password = urllib.parse.quote_plus(password)
            connection_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"

        original_user_email = user_email
        sanitized_email_for_secret = original_user_email.replace('@', '_').replace('.', '_')
        secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_db_{connection_type}"

        payload = { 'connection_url': connection_url }
        _store_secret_in_gcp(secret_id, payload)

        add_log(f"DB: saved credentials for {user_email}")
        if job_id:
            add_log(job_id, f"DB: credentials saved for {user_email}")
        return jsonify({
            'status': 'success',
            'status_code': 201,
            'message': 'Credentials saved',
            'secret_id': secret_id
        }), 201
    except ValueError as ve:
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"DB save_credentials error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to save credentials: {str(e)}', 500)


@db_bp.route('/db/fetch_data', methods=['POST'])
def db_fetch_data():
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)
        connection_type = _validate_non_empty_string(data.get('connection_type'), 'connection_type', min_len=4).lower()
        session_id = _validate_non_empty_string(data.get('session_id'), 'session_id', min_len=8)
        tables = data.get('tables') or []
        connection_id = data.get('connection_id')
        if not isinstance(tables, list) or not tables:
            return fail('tables must be a non-empty array of table names', 400)

        # sanitized_email_for_secret = user_email.replace('@', '_').replace('.', '_')
        # secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_db_{connection_type}"
        # secret_value = get_secret_from_secret_manager(secret_id)
        data_manager = get_data_manager()
        credential = data_manager.get_credential(user_email, connection_id)

        
        if not credential:
            return fail('No DB credentials found. Please save credentials first.', 404)


        username = _validate_non_empty_string(credential.get('username'), 'username', min_len=1)
        password = _validate_non_empty_string(credential.get('password'), 'password', min_len=1)
        host = _validate_non_empty_string(credential.get('host'), 'host', min_len=1)
        port = _validate_non_empty_string(str(credential.get('port')), 'port', min_len=1)
        database = _validate_non_empty_string(credential.get('database'), 'database', min_len=1)


        print("ConnectionType",connection_type)
        if connection_type == 'mysql':
            driver = 'mysql+pymysql'
        else:
            driver = 'mssql+pymssql'

        password = urllib.parse.quote_plus(password)
        connection_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"

        # try:
        #     creds = json.loads(secret_value)
        # except json.JSONDecodeError:
        #     return fail('Invalid credentials format', 500)
        
        # connection_url = (creds.get('connection_url') or '').strip()
        # if not connection_url:
        #     return fail('Missing connection_url in credentials', 500)

        # Create engine and preparer
        try:
            engine = create_engine(connection_url)
            preparer = engine.dialect.identifier_preparer
        except Exception as db_err:
            add_log(f"DB engine error: {str(db_err)} | traceback: {traceback.format_exc()}")
            return fail(f'Failed to initialize DB engine: {str(db_err)}', 500)

        # Clear input data once; then save each table as its own PKL
        _clear_session_input_data(session_id)
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        input_data_dir = os.path.join(base_dir, 'execution_layer', 'input_data', session_id)
        os.makedirs(input_data_dir, exist_ok=True)

        saved_files = []
        preview_map = {}

        for table in tables:
            # validate table name basic characters
            if not isinstance(table, str) or not table.strip():
                continue
            table_name = table.strip()
            try:
                # Quote identifiers safely
                # if schema:
                #     full_ident = f"{preparer.quote(schema)}.{preparer.quote(table_name)}"
                # else:
                full_ident = f"{preparer.quote(table_name)}"

                query_sql = f"SELECT * FROM {full_ident}"
                df = pd.read_sql_query(query_sql, con=engine)

                pkl_filename = f"{table_name}.pkl"
                pkl_path = os.path.join(input_data_dir, pkl_filename)
                df.to_pickle(pkl_path)
                saved_files.append(pkl_filename)

                # Build preview per table
                try:
                    top = df.head(10).to_dict('records')
                    preview_rows = []
                    for rec in top:
                        preview_rows.append({str(k): ("null" if v is None else str(v)) for k, v in rec.items()})
                    preview_map[pkl_filename] = {"top_records": preview_rows}
                except Exception:
                    preview_map[pkl_filename] = {"top_records": []}
            except Exception as db_err:
                add_log(f"DB fetch error for table {table_name}: {str(db_err)} | traceback: {traceback.format_exc()}")
                # continue with other tables
                continue

        add_log(f"DB: saved {len(saved_files)} files for session {session_id}")
        if job_id:
            add_log(job_id, f"DB: data fetched and saved {len(saved_files)} files for session {session_id}")
        return jsonify({
            'status': 'success',
            'status_code': 200,
            'message': 'Data fetched and saved',
            'session_id': session_id,
            'saved_files': saved_files,
            'preview_data': preview_map
        }), 200
    except ValueError as ve:
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"DB fetch_data error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to fetch data: {str(e)}', 500)

@db_bp.route('/db/list_tables', methods=['POST'])
def db_list_tables():
    try:
        data = request.get_json(silent=True) or {}
        job_id = (data.get('job_id') or '').strip() if isinstance(data, dict) else ''
        # Validate required inputs
        user_email = _validate_non_empty_string(data.get('user_email'), 'user_email', min_len=3)

        # Normalize connection_id (frontends may send number/object) - optional if connection_name provided
        raw_conn = data.get('connection_id')
        if isinstance(raw_conn, dict):
            # Try common shapes
            candidate = raw_conn.get('connection_id') or raw_conn.get('id') or raw_conn.get('value') or raw_conn.get('name')
            connection_id = str(candidate) if candidate is not None else ''
        elif raw_conn is None:
            connection_id = ''
        else:
            connection_id = str(raw_conn)

        connection_type = _validate_non_empty_string(data.get('connection_type'), 'connection_type', min_len=4).lower()

        # Optional fallback by connection_name
        connection_name = (data.get('connection_name') or '').strip()
        if not connection_id and not connection_name:
            return fail('connection_id or connection_name is required', 400)

        data_manager = get_data_manager()

        # Support fallback by connection_name when connection_id is not provided
        credential = None
        if connection_id:
            credential = data_manager.get_credential(user_email, connection_id)
        if not credential:
            # Try by connection_name (optional UX improvement)
            if connection_name:
                try:
                    creds_list = data_manager.get_all_credentials(user_email, connection_type)
                    for cdoc in creds_list:
                        if getattr(cdoc, 'connection_name', '') == connection_name:
                            credential = getattr(cdoc, 'credentials', None)
                            break
                except Exception:
                    credential = None

        # Ensure credential exists before accessing fields
        if not credential:
            return fail('No DB credentials found. Please save credentials first or provide a valid connection_id/connection_name.', 404)

        username = _validate_non_empty_string(credential.get('username'), 'username', min_len=1)
        password = _validate_non_empty_string(credential.get('password'), 'password', min_len=1)
        host = _validate_non_empty_string(credential.get('host'), 'host', min_len=1)
        port = _validate_non_empty_string(str(credential.get('port')), 'port', min_len=1)
        database = _validate_non_empty_string(credential.get('database'), 'database', min_len=1)


        print("ConnectionType",connection_type)
        if connection_type == 'mysql':
            driver = 'mysql+pymysql'
        else:
            driver = 'mssql+pymssql'
        password = urllib.parse.quote_plus(password)
        connection_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"

        # Load credentials
        # sanitized_email_for_secret = user_email.replace('@'SS, '_').replace('.', '_')
        # secret_id = f"{_sanitize_secret_id_component(sanitized_email_for_secret)}_db_{connection_type}"
        # secret_value = get_secret_from_secret_manager(secret_id)

        print("new credential: ",credential)

        # try:
        #     creds = connection_url
        # except json.JSONDecodeError:
        #     return fail('Invalid credentials format', 500)

        # # connection_url = (creds.get('connection_url') or '').strip()
        # if not connection_url:
        #     return fail('Missing connection_url in credentials', 500)

        try:
            engine = create_engine(connection_url)
            inspector = inspect(engine)
            # Removed schema usage; fetch all tables and views without schema
            try:
                tables = inspector.get_table_names() or []
            except Exception:
                tables = []
            try:
                views = inspector.get_view_names() or []
            except Exception:
                views = []
        except Exception as db_err:
            add_log(f"DB list tables error: {str(db_err)} | traceback: {traceback.format_exc()}")
            return fail(f'Failed to list tables: {str(db_err)}', 500)

        response = jsonify({
            'status': 'success',
            'status_code': 200,
            'message': 'DB objects fetched successfully',
            'data': {
                'tables': tables if isinstance(tables, list) else [],
                'views': views if isinstance(views, list) else []
            }
        }), 200
        if job_id:
            add_log(job_id, f"DB: listed tables and views for connection")
        return response
    except ValueError as ve:
        return fail(str(ve), 400)
    except Exception as e:
        add_log(f"DB list_tables error: {str(e)} | traceback: {traceback.format_exc()}")
        return fail(f'Failed to list tables: {str(e)}', 500)

