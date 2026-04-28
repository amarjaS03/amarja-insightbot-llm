# ============================================================================
# IMPORTS AND INITIALIZATION
# ============================================================================

from firebase_functions import https_fn
from firebase_admin import initialize_app, storage
import json
import requests
import time
import io
from datetime import datetime
from google.cloud import secretmanager

# Initialize Firebase Admin SDK
initialize_app()

# ============================================================================
# SECRET MANAGER UTILITIES
# ============================================================================

def get_secret_from_secret_manager(secret_name):
    try:
        # Initialize Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
                
        # Access the secret version
        response = client.access_secret_version(request={"name": f"projects/insightbot-467305/secrets/{secret_name}/versions/latest"})
        
        # Decode the secret value
        secret_value = response.payload.data.decode("UTF-8")
        
        print(f"Successfully retrieved secret: {secret_name}")
        return secret_value.strip()
        
    except Exception as e:
        print(f"Failed to retrieve secret {secret_name}: {str(e)}")
        return None

def convert_email_to_secret_name(email):
    try:
        # Replace @ with _ and . with _ to create valid secret name
        secret_name = email.replace('@', '_').replace('.', '_')
        
        # Add _salesforce suffix
        secret_name = f"{secret_name}_salesforce"
        
        print(f"Converted email '{email}' to secret name: '{secret_name}'")
        return secret_name
        
    except Exception as e:
        print(f"Error converting email to secret name: {str(e)}")
        return None

def get_salesforce_credentials(user_email):
    try:
        # Convert email to secret name format
        secret_name = convert_email_to_secret_name(user_email)
        
        if not secret_name:
            print(f"Failed to convert email '{user_email}' to secret name")
            return None
        
        credentials_json = get_secret_from_secret_manager(secret_name)
        
        if not credentials_json:
            print(f"Failed to retrieve secret '{secret_name}' from Secret Manager")
            return None
        
        print(f"Raw secret value: {credentials_json[:100]}...")  # Show first 100 chars for debugging
        
        # Parse the JSON string to get credentials dictionary
        credentials = json.loads(credentials_json)
        
        # Validate that all required fields are present
        required_fields = ['client_id', 'client_secret', 'username', 'password']
        missing_fields = [field for field in required_fields if field not in credentials]
        
        if missing_fields:
            print(f"Missing required fields: {missing_fields}")
            return None
        
        print("Successfully retrieved and parsed Salesforce credentials from Secret Manager")
        return credentials
        
    except json.JSONDecodeError as e:
        return None
    except Exception as e:
        print(f"Error retrieving Salesforce credentials: {str(e)}")
        return None

# ============================================================================
# SALESFORCE AUTHENTICATION
# ============================================================================

def login_to_salesforce(user_email, environment='sandbox', credentials=None):
    # Get credentials from Secret Manager first to check for stored environment preference
    # credentials = get_salesforce_credentials(user_email)
    
    # Use stored environment preference if available, otherwise use passed parameter
    if credentials and 'environment' in credentials:
        environment = credentials['environment']
    
    # Salesforce OAuth 2.0 token endpoint - select based on environment
    if environment.lower() == 'production':
        url = 'https://login.salesforce.com/services/oauth2/token'
    else:  # Default to sandbox
        url = 'https://test.salesforce.com/services/oauth2/token'
    
    if not credentials:
        return {
            "status": "connection failed",
            "message": "Failed to retrieve Salesforce credentials from Secret Manager",
            "data": None,
        }

    # OAuth payload with credentials from Secret Manager
    payload = {
        'grant_type': 'password',  # Username-password flow
        'client_id': credentials['client_id'],
        'client_secret': credentials['client_secret'],
        'username': credentials['username'],
        'password': credentials['password']  # password + security_token
    }

    try:
        # Make authentication request to Salesforce
        response = requests.post(url, data=payload, timeout=60)

        # Handle successful authentication
        if response.status_code == 200:
            data = response.json()
            access_token = data['access_token']
            instance_url = data['instance_url']
                        
            return {
                "status": "success",
                "message": "Access token obtained successfully!",
                "data": data,
            }
        else:
            # Handle authentication failure
            error_data = response.json() if response.text else None
            error_message = "Authentication failed - check credentials"
            
            if error_data and 'error' in error_data:
                if error_data['error'] == 'invalid_grant':
                    error_message = "Invalid credentials - please check username, password, and security token. Credentials may have expired."
                elif error_data['error'] == 'invalid_client':
                    error_message = "Invalid client credentials - please check client_id and client_secret."
                elif error_data['error'] == 'invalid_client_id':
                    error_message = "Invalid client_id - please check your Salesforce connected app configuration."
                else:
                    error_message = f"Authentication error: {error_data.get('error_description', error_data['error'])}"
            
            return {
                "status": "connection failed",
                "message": error_message,
                "data": error_data,
            }
            
    except requests.RequestException as e:
        print(f"Network error during authentication: {str(e)}")
        return {
            "status": "connection failed",
            "message": f"Network error: {str(e)}",
            "data": None,
        }

# ============================================================================
# SALESFORCE BULK API OPERATIONS
# ============================================================================

def create_bulk_query_job(soql_query, access_token, instance_url, api_version="v59.0"):
    # Construct the bulk API endpoint URL
    url = f"{instance_url}/services/data/{api_version}/jobs/query"
    
    # Set up authentication headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Job configuration payload
    payload = {
        "operation": "query",  # Bulk query operation
        "query": soql_query,   # SOQL query to execute
    }
    
    try:
        # Submit the job creation request
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            job = response.json()
            job_id = job['id']
            print(f"Bulk job created successfully: {job_id}")
            return job_id
        else:
            error_msg = f"Job creation failed: {response.status_code} - {response.text}"
            print(f"{error_msg}")
            raise Exception(error_msg)
            
    except requests.RequestException as e:
        error_msg = f"Network error during job creation: {str(e)}"
        print(f"{error_msg}")
        raise Exception(error_msg)

def wait_for_job_completion(job_id, access_token, instance_url, api_version="v59.0"):
    # Construct the job status endpoint URL
    url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}"
    
    # Set up authentication headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    print(f"Waiting for job {job_id} to complete...")
    
    # Poll until job completion
    while True:
        try:
            # Check job status
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                job_state = data['state']
                
                print(f"Job status: {job_state}")
                
                # Check for completion
                if job_state == 'JobComplete':
                    print("Job completed successfully!")
                    return data
                elif job_state in ['Failed', 'Aborted']:
                    error_msg = f"Job {job_state}: {data.get('stateMessage', 'Unknown error')}"
                    print(f"{error_msg}")
                    raise Exception(error_msg)
                
                # Wait before next status check (5 seconds)
                time.sleep(5)
            else:
                error_msg = f"Failed to check job status: {response.status_code} - {response.text}"
                print(f"{error_msg}")
                raise Exception(error_msg)
                
        except requests.RequestException as e:
            error_msg = f"Network error during status check: {str(e)}"
            print(f"{error_msg}")
            raise Exception(error_msg)

def get_job_results(job_id, access_token, instance_url, api_version="v59.0"):
    # Construct the job results endpoint URL
    url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}/results"
    
    # Set up authentication headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        print(f"Downloading results for job {job_id}...")
        
        # Download the results
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            csv_data = response.text
            print(f"Results downloaded successfully ({len(csv_data)} characters)")
            return csv_data
        else:
            error_msg = f"Failed to retrieve results: {response.status_code} - {response.text}"
            print(f"{error_msg}")
            raise Exception(error_msg)
            
    except requests.RequestException as e:
        error_msg = f"Network error during result download: {str(e)}"
        print(f"{error_msg}")
        raise Exception(error_msg)

# ============================================================================
# MAIN DATA FETCHING FUNCTIONS
# ============================================================================

def fetch_salesforce_data(soql_query, access_token, instance_url, api_version="v59.0", bucket_name=None, object_name="salesforce_data", user_email=None):
    try:
        print(f"Starting Salesforce data fetch...")
        print(f"Query: {soql_query[:100]}{'...' if len(soql_query) > 100 else ''}")
        
        # Step 1: Create bulk query job
        job_id = create_bulk_query_job(soql_query, access_token, instance_url, api_version)
        
        # Step 2: Wait for job completion
        job_data = wait_for_job_completion(job_id, access_token, instance_url, api_version)
        
        # Step 3: Download results
        csv_data = get_job_results(job_id, access_token, instance_url, api_version)
        
        # Step 4: Convert CSV Data into DataFrame
        import pandas as pd
        df = pd.read_csv(io.StringIO(csv_data))
        if 'CreatedDate' in df.columns:
            df['CreatedDate'] = pd.to_datetime(df['CreatedDate'], errors='coerce').dt.date.astype('datetime64[ns]')
        if 'CloseDate' in df.columns:
            df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce').dt.date.astype('datetime64[ns]')

        # Step 5: Save DataFrame to Firebase Storage as pickle file (if bucket_name provided)
        firebase_result = None
        delete_result = None
        if bucket_name:
            # Step 5a: Delete old files for this user and object
            if user_email:
                delete_result = delete_old_files_from_firebase_storage(bucket_name, user_email, object_name)
                if delete_result["status"] == "success":
                    print(f"Cleaned up {delete_result['deleted_count']} old files")
                else:
                    print(f"Warning: Failed to clean up old files: {delete_result.get('error', 'Unknown error')}")
            
            # Step 5b: Generate file path with user email and timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if user_email:
                # Sanitize user email for file path (replace @ with _ and remove special chars)
                safe_email = user_email.replace('@', '_').replace('.', '_').replace('+', '_')
                file_path = f"{safe_email}/data/salesforce/{object_name}_{timestamp}.pkl"
            else:
                file_path = f"salesforce_data/{object_name}_{timestamp}.pkl"
            
            # Step 5c: Save DataFrame as pickle file to Firebase Storage
            firebase_result = save_dataframe_to_firebase_storage(df, bucket_name, file_path)
            
            if firebase_result["status"] == "success":
                print(f"Successfully saved DataFrame to Firebase Storage as pickle file")
            else:
                print(f"Failed to save to Firebase Storage: {firebase_result.get('error', 'Unknown error')}")
    
        print(f"Successfully fetched DataFrame with {len(df)} records!")
        
        # Return DataFrame, Firebase Storage save result, and deletion result
        return {
            "dataframe": df,
            "firebase_save_result": firebase_result,
            "delete_result": delete_result,
            "record_count": len(df)
        }
        
    except Exception as e:
        print(f"Data fetch failed: {str(e)}")
        raise

# ============================================================================
# FIREBASE STORAGE UTILITIES
# ============================================================================

def delete_old_files_from_firebase_storage(bucket_name, user_email, object_name):
    try:
        # Use explicit Firebase Storage bucket
        bucket = storage.bucket(bucket_name)
        
        # Sanitize user email for file path
        safe_email = user_email.replace('@', '_').replace('.', '_').replace('+', '_')
        folder_path = f"{safe_email}/data/salesforce/"
        
        print(f"Looking for old files in folder: {folder_path}")
        
        # List all blobs in the user's folder
        blobs = bucket.list_blobs(prefix=folder_path)
        
        deleted_files = []
        for blob in blobs:
            # Check if this blob is for the specific object
            if blob.name.endswith('.pkl') and object_name in blob.name:
                try:
                    print(f"Deleting old file: {blob.name}")
                    blob.delete()
                    deleted_files.append(blob.name)
                except Exception as e:
                    print(f"Failed to delete {blob.name}: {e}")
        
        print(f"Deleted {len(deleted_files)} old files for {object_name}")
        
        return {
            "status": "success",
            "deleted_files": deleted_files,
            "deleted_count": len(deleted_files)
        }
        
    except Exception as e:
        error_msg = f"Failed to delete old files: {str(e)}"
        print(error_msg)
        return {
            "status": "error",
            "error": error_msg,
            "deleted_files": [],
            "deleted_count": 0
        }

def save_dataframe_to_firebase_storage(dataframe, bucket_name, file_path):
    try:
        # Use explicit Firebase Storage bucket
        bucket = storage.bucket(bucket_name)
        
        print(f"Using Firebase Storage bucket: {bucket.name}")
        
        # Create blob object
        blob = bucket.blob(file_path)
        
        # Serialize DataFrame to pickle format using pandas
        import io as pandas_io
        pickle_buffer = pandas_io.BytesIO()
        dataframe.to_pickle(pickle_buffer)
        pickle_data = pickle_buffer.getvalue()
        
        # Upload to Firebase Storage
        blob.upload_from_string(pickle_data, content_type='application/octet-stream')
        
        # Set public access (optional - you can remove this if you want private files)
        blob.make_public()
        
        # Get public URL (no private key required)
        public_url = blob.public_url
        
        print(f"DataFrame saved to Firebase Storage: gs://{bucket.name}/{file_path}")
        print(f"DataFrame shape: {dataframe.shape}")
        print(f"File size: {len(pickle_data)} bytes")
        print(f"Public URL: {public_url}")
        
        return {
            "status": "success",
            "bucket": bucket.name,
            "file_path": file_path,
            "file_size": len(pickle_data),
            "dataframe_shape": dataframe.shape,
            "firebase_url": f"gs://{bucket.name}/{file_path}",
            "public_url": public_url
        }
        
    except Exception as e:
        error_msg = f"Failed to save DataFrame to Firebase Storage: {str(e)}"
        print(error_msg)
        return {
            "status": "error",
            "error": error_msg,
            "bucket": bucket_name,
            "file_path": file_path
        }

# ============================================================================
# CLOUD FUNCTION MAIN ENDPOINT
# ============================================================================

@https_fn.on_request()
def zingworks_salesforce_connector(req: https_fn.Request) -> https_fn.Response:
    try:        
        # ====================================================================
        # HANDLE CORS PREFLIGHT REQUESTS
        # ====================================================================
        if req.method == 'OPTIONS':
            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '3600'
            }
            return https_fn.Response('', status=204, headers=headers)
        
        # ====================================================================
        # SET RESPONSE HEADERS
        # ====================================================================
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        }
        
        # Single subject area configuration - only Sales is supported
        SUBJECT_AREA_OBJECTS = {
            'sales': ['Account', 'Opportunity']
        }
        
        # Firebase Storage configuration - explicit bucket name
        FIREBASE_BUCKET_NAME = "insightbot-467305.firebasestorage.app"  # Your Firebase project's default bucket
        
        # Extract parameters from request body
        try:
            request_data = req.get_json() if req.method == 'POST' else {}
            user_email = request_data.get('email') or request_data.get('user_email') or request_data.get('username')
            is_oauth_login = request_data.get('isOAuthLogin', False)
            is_credential_login = request_data.get('isCredentialLogin', False)  # No default - must be explicitly set
            instance_url = request_data.get('instanceURL') or request_data.get('instance_url')
            access_token = request_data.get('accessToken') or request_data.get('access_token')
            environment = request_data.get('environment', 'sandbox')  # Default to sandbox
            
            # Subject area parameters
            subject_area = request_data.get('subject_area', 'sales')  # Default to sales
            subject_area_name = request_data.get('subject_area_name', 'Sales')
            subject_area_description = request_data.get('subject_area_description', 'Sales data analysis')
            
            # credentials
            credentials = request_data.get('credentials')

            # Validate subject area
            if subject_area not in SUBJECT_AREA_OBJECTS:
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": f"Invalid subject area '{subject_area}'. Supported areas: {list(SUBJECT_AREA_OBJECTS.keys())}",
                        "error_type": "InvalidSubjectArea"
                    }),
                    status=400,
                    headers=headers
                )
            
            # Get objects for the selected subject area
            sobject_names = SUBJECT_AREA_OBJECTS[subject_area]
            print(f"Processing subject area '{subject_area}' with objects: {sobject_names}")
            
            # Validate that at least one login method is specified
            if not is_oauth_login and not is_credential_login:
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": "At least one login method must be specified. Set either 'isOAuthLogin' or 'isCredentialLogin' to true.",
                        "error_type": "NoLoginMethodSpecified"
                    }),
                    status=400,
                    headers=headers
                )
            
            # Validate parameters based on login type
            if is_oauth_login:
                # For OAuth login, we need instanceURL and accessToken
                if not instance_url or not access_token:
                    return https_fn.Response(
                        json.dumps({
                            "status": "error",
                            "message": "For OAuth login, 'instanceURL' and 'accessToken' are required in request body.",
                            "error_type": "MissingOAuthParams"
                        }),
                        status=400,
                        headers=headers
                    )
                print(f"Processing OAuth request for user: {user_email}")
            elif is_credential_login:
                # For credential login, we need user email
                if not user_email:
                    return https_fn.Response(
                        json.dumps({
                            "status": "error",
                            "message": "For credential login, email is required in request body. Send POST request with JSON body containing 'email' field.",
                            "error_type": "MissingEmail"
                        }),
                        status=400,
                        headers=headers
                    )
                print(f"Processing credential request for user: {user_email}")
            else:
                # This should not happen due to the validation above, but keeping for safety
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": "Invalid login method configuration.",
                        "error_type": "InvalidLoginConfig"
                    }),
                    status=400,
                    headers=headers
                )
            
        except Exception as e:
            return https_fn.Response(
                json.dumps({
                    "status": "error",
                    "message": f"Failed to parse request body: {str(e)}",
                    "error_type": "RequestParseError"
                }),
                status=400,
                headers=headers
            )
        
        # Column mappings for Sales subject area objects only
        object_columns = {
            'Account': [
                "AccountNumber", "AccountSource", "AnnualRevenue",
                "BillingCity", "BillingCountry", "BillingCountryCode", "BillingPostalCode",
                "BillingState", "BillingStateCode", "BillingStreet",
                "CreatedById", "CreatedDate", "Description",
                "Id", "Industry", "Name", "NumberOfEmployees",
                "OperatingHoursId", "ParentId", "Rating", "ShippingCity", "ShippingCountry",
                "ShippingCountryCode", "ShippingPostalCode", "ShippingState", "ShippingStateCode",
                "ShippingStreet", "Site", "Type"
            ],
            'Opportunity': [
                "AccountId", "Amount", "CampaignId", "CloseDate", "CreatedById", "CreatedDate",
                "Description", "ExpectedRevenue", "ForecastCategoryName", "Id", "IsWon", "LeadSource",
                "Name", "NextStep", "OwnerId", "Pricebook2Id", "Probability", "StageName", "TotalOpportunityQuantity"
            ]
        }
                
        # ====================================================================
        # STEP 1: AUTHENTICATE WITH SALESFORCE
        # ====================================================================
        if is_oauth_login:
            # For OAuth login, use provided access token and instance URL directly
            auth_result = {
                "status": "success",
                "message": "OAuth authentication successful",
                "data": {
                    "access_token": access_token,
                    "instance_url": instance_url
                }
            }
            print(f"Using OAuth authentication with instance URL: {instance_url}")
        else:
            # For credential login, authenticate using stored credentials
            auth_result = login_to_salesforce(user_email, environment, credentials)
            print(auth_result)
            if auth_result['status'] != 'success':
                return https_fn.Response(json.dumps(auth_result), status=401, headers=headers)
            
            # Extract credentials from authentication result
            access_token = auth_result['data']['access_token']
            instance_url = auth_result['data']['instance_url']
            print(f"Credential authentication successful with instance URL: {instance_url}")
        
        # Verify authentication was successful
        if auth_result['status'] != 'success':
            return https_fn.Response(json.dumps(auth_result), status=401, headers=headers)
        
        # ====================================================================
        # STEP 2: PROCESS EACH OBJECT (FETCH ALL RECORDS + SAVE TO FIREBASE)
        # ====================================================================
        
        results = {}
        total_records = 0
        successful_objects = 0
        
        for i, sobject_name in enumerate(sobject_names, 1):
            try:
                # Use hardcoded field names for this object
                fields = object_columns.get(sobject_name, [])
                
                if not fields:
                    print(f"No hardcoded fields found for {sobject_name} - skipping")
                    continue

                # Generate SOQL query with hardcoded field names
                fields_str = ', '.join(fields)
                soql_query = f"SELECT {fields_str} FROM {sobject_name}"
                
                # Fetch all data for this object using Bulk API
                fetch_result = fetch_salesforce_data(soql_query, access_token, instance_url, bucket_name=FIREBASE_BUCKET_NAME, object_name=sobject_name, user_email=user_email)
                df = fetch_result["dataframe"]
                firebase_result = fetch_result["firebase_save_result"]
                delete_result = fetch_result["delete_result"]
                
                if df is not None and len(df) > 0:
                    # Generate simple preview data - only top 10 records as strings
                    preview_data = {
                        "top_records": []
                    }
                    
                    # Convert top records to strings
                    top_records = df.head(10).to_dict('records')
                    for record in top_records:
                        string_record = {}
                        for key, value in record.items():
                            if value is None:
                                string_record[str(key)] = "null"
                            elif isinstance(value, (dict, list)):
                                string_record[str(key)] = str(value)
                            else:
                                string_record[str(key)] = str(value)
                        preview_data["top_records"].append(string_record)
                    
                    results[sobject_name] = {
                        "record_count": len(df),
                        "dataframe_shape": df.shape,
                        "firebase_pickle_file": firebase_result.get("firebase_url") if firebase_result and firebase_result.get("status") == "success" else None,
                        "firebase_public_url": firebase_result.get("public_url") if firebase_result and firebase_result.get("status") == "success" else None,
                        "firebase_file_size": firebase_result.get("file_size") if firebase_result and firebase_result.get("status") == "success" else None,
                        "firebase_save_status": firebase_result.get("status") if firebase_result else "not_attempted",
                        "deleted_old_files": delete_result.get("deleted_files", []) if delete_result else [],
                        "deleted_files_count": delete_result.get("deleted_count", 0) if delete_result else 0,
                        "preview_data": preview_data,
                        "status": "success"
                    }
                    total_records += len(df)
                    successful_objects += 1
                else:
                    results[sobject_name] = {
                        "record_count": 0,
                        "dataframe_shape": (0, 0),
                        "firebase_pickle_file": None,
                        "status": "no_data"
                    }
                    
            except Exception as obj_error:
                error_msg = str(obj_error)
                results[sobject_name] = {
                    "record_count": 0,
                    "status": "error",
                    "error": error_msg
                }
        
        # ====================================================================
        # STEP 3: PREPARE AND RETURN RESULTS
        # ====================================================================
        result = {
            "status": "success",
            "message": f"Subject area '{subject_area}' extraction complete: {successful_objects}/{len(sobject_names)} objects processed, {total_records} total records",
            "subject_area": {
                "id": subject_area,
                "name": subject_area_name,
                "description": subject_area_description,
                "objects": sobject_names
            },
            "summary": {
                "total_objects_requested": len(sobject_names),
                "successful_objects": successful_objects,
                "total_records_extracted": total_records
            },
            "results": results
        }
        return https_fn.Response(json.dumps(result), headers=headers)
    
    # ========================================================================
    # GLOBAL ERROR HANDLER
    # ========================================================================
    except Exception as e:
        error_msg = str(e)        
        error_result = {
            "status": "error",
            "message": error_msg,
            "error_type": type(e).__name__
        }
        
        return https_fn.Response(
            json.dumps(error_result),
            status=500,
            headers={'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'}
        )



# NOTE
# 1. This function is used to fetch data from Salesforce and save it to Firebase Storage
# 2. Data will store in the following path: {user_email}/data/salesforce/{object_name}_{timestamp}.pkl
# 3. If data fetch again then previous one will delete and new one will be saved
# TODO
# 1. Apply an API key to use this function
# 2. instead of saving the data to Firebase Storage with email use UserID