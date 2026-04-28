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
from dataProcessing import (
    processed_data, 
    process_sales_order_data, 
    process_shipment_data, 
    process_sales_invoice_data,
    process_invoice_data,
    process_item_warehouse_data,
    process_inventory_receipt_data,
    generate_preview_data
)
import pandas as pd
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
        secret_name = f"{secret_name}_acumatica"
        
        print(f"Converted email '{email}' to secret name: '{secret_name}'")
        return secret_name
        
    except Exception as e:
        print(f"Error converting email to secret name: {str(e)}")
        return None

def get_acumatica_credentials(user_email):
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
        required_fields = ['client_id', 'client_secret', 'tenant_url']
        missing_fields = [field for field in required_fields if field not in credentials]
        
        if missing_fields:
            print(f"Missing required fields: {missing_fields}")
            return None
        
        print("Successfully retrieved and parsed Acumatica credentials from Secret Manager")
        return credentials 
        
    except json.JSONDecodeError as e:
        return None
    except Exception as e:
        print(f"Error retrieving Acumatica credentials: {str(e)}")
        return None

# ============================================================================
# ACUMATICA DATA RETRIEVAL
# ============================================================================

def fetch_acumatica_data(tenant_url, entity_endpoint, access_token, redirect_url, bucket_name, user_email) -> tuple[int, dict]:
    """
    Fetch data from Acumatica API with entity-specific field selection and expansion.
    
    Different entities have different structures, so we need to customize the query
    based on the entity type.
    """
    try:
        # Define entity-specific configurations
        entity_configs = {
            'StockItem': {
                'select_fields': [
                    "InventoryID",
                    "Description",
                    "ItemClass",
                    "BaseUOM",
                    "DefaultPrice",
                    "DefaultWarehouseID",
                    "IsAKit",
                    "ItemType",
                    "ItemStatus",
                    "WarehouseDetails/WarehouseID",
                    "WarehouseDetails/QtyOnHand",
                    "WarehouseDetails/ReplenishmentWarehouse"
                ],
                'expand': 'WarehouseDetails'
            },
            'ItemWarehouse': {
                'select_fields': [
                    "InventoryID",
                    "WarehouseID",
                    "QtyOnHand",
                    "QtyAvailable",
                    "ReplenishmentSource"
                ],
                'expand': None
            },
            'InventoryReceipt': {
                'select_fields': [
                    "ReferenceNbr",
                    "Date",
                    "Status",
                    "TotalQty",
                    "TotalCost"
                ],
                'expand': None
            },
            'SalesOrder': {
                'select_fields': [
                    "OrderType",
                    "CustomerID",
                    "CustomerOrder",
                    "OrderNbr",
                    "LocationID",
                    "Status",
                    "Date",
                    "RequestedOn",
                    "CustomerOrder",
                    "Description",
                    "OrderedQty",
                    "OrderTotal",
                    "ShipVia",
                    "Details/Branch",
                    "Details/InventoryID",
                    "Details/FreeItem",
                    "Details/TaxZone",
                    "Details/UOM",

                    "Details/WarehouseID",
                    "Details/SalespersonID",


                    "Details/OrderQty",
                    "Details/UnitPrice",
                    "Details/ExtendedPrice",
                    "Details/DiscountPercent",
                    "Details/DiscountAmount",
                    "Details/Amount",

                    "Details/UnbilledAmount",
                    "Details/RequestedOn",
                    "Details/ShipOn",

                    "Details/Completed",
                    "Details/POSource",
                    "Details/TaxCategory",
                    "Details/LineNbr",
                    "Details/TaxZone",
                    "Details/UnitCost",
                    "Details/LineType",

                    "TaxDetails/TaxID",
                    "TaxDetails/TaxRate",
                    "TaxDetails/TaxableAmount",
                    "TaxDetails/TaxAmount",

                    "ShipToContact/BusinessName",
                    "ShipToContact/Attention",
                    "ShipToContact/Phone1",
                    "ShipToContact/Email",

                    "ShipToAddress/AddressLine1",
                    "ShipToAddress/AddressLine2",
                    "ShipToAddress/City",
                    "ShipToAddress/Country",
                    "ShipToAddress/State",
                    "ShipToAddress/PostalCode",

                    "BillToContact/Attention",
                    "BillToContact/Phone1",
                    "BillToContact/Email",
                    "BillToContact/BusinessName",

                    "BillToAddress/AddressLine1",   
                    "BillToAddress/AddressLine2",   
                    "BillToAddress/City",   
                    "BillToAddress/Country",    
                    "BillToAddress/State",  
                    "BillToAddress/PostalCode", 

                    "Shipments/ShipmentType",   
                    "Shipments/ShipmentNbr",    
                    "Shipments/Status", 
                    "Shipments/ShipmentDate",   
                    "Shipments/ShippedQty",    
                    "Shipments/ShippedWeight",      
                    "Shipments/InvoiceNbr", 
                    "Shipments/InventoryDocType",   
                    "Shipments/InventoryRefNbr",    

                    # for Payment Confirmation pending from acumatica team
                    # "Payments/DocType", 
                    # "Payments/ReferenceNbr",    
                    # "Payments/AppliedToOrder",  
                    # "Payments/TransferredToInvoice",    
                    # "Payments/Balance", 
                    # "Payments/PaymentAmount",  
                    # "Payments/Status",


                    # SOShipmentContact fields already taken
                    # ShipToAddress fields already taken    
                ],
                'expand': "Details,TaxDetails,ShipToContact,BillToContact,ShipToAddress,BillToAddress,Shipments,Payments",
            },
            'Shipment' : {
                'select_fields': [
                    "ShippingSettings/ShipToContact/Attention",
                    "ShippingSettings/ShipToContact/BusinessName",
                    "ShippingSettings/ShipToContact/Email",
                    "ShippingSettings/ShipToContact/Phone1",

                    "ShippingSettings/ShipToAddress/AddressLine1",
                    "ShippingSettings/ShipToAddress/AddressLine2",
                    "ShippingSettings/ShipToAddress/City",
                    "ShippingSettings/ShipToAddress/Country",
                    "ShippingSettings/ShipToAddress/State",
                    "ShippingSettings/ShipToAddress/PostalCode",

                    "Details/OrderType",
                    "Details/OrderNbr",
                    "Details/InventoryID",
                    "Details/LocationID",
                    "Details/UOM",
                    "Details/OrderedQty",
                    "Details/ShippedQty",

                    "Packages/BoxID",
                    "Packages/Confirmed",       
                    "Packages/Type",
                    "Packages/Length",  
                    "Packages/Width",   
                    "Packages/Height",  
                    "Packages/UOM",
                    "Packages/Weight",
                    "Packages/TrackingNbr",

                    "ShipmentNbr",
                    "Type",
                    "Status",
                    "Operation",
                    "ShipmentDate",
                    "Description",
                    "CustomerID",
                    "LocationID",
                    "WarehouseID",
                    "ShippedQty",
                    "ShippedWeight",
                    "ShippedVolume",
                    "PackageWeight",
                    "ShipVia",
                ],'expand': "Details,Packages,ShippingSettings/ShipToContact,ShippingSettings/ShipToAddress"
            },
            'SalesInvoice' : {
                'select_fields': [
                
                    "BillingSettings/BillToContact/BusinessName",
                    "BillingSettings/BillToContact/Attention",
                    "BillingSettings/BillToContact/Email",
                    "BillingSettings/BillToContact/Phone1",

                    "BillingSettings/BillToAddress/AddressLine1",
                    "BillingSettings/BillToAddress/AddressLine2",
                    "BillingSettings/BillToAddress/City",
                    "BillingSettings/BillToAddress/Country",
                    "BillingSettings/BillToAddress/State",
                    "BillingSettings/BillToAddress/PostalCode",

                    "Commissions/CommissionAmount",
                    "Commissions/TotalCommissionableAmount",

                    "ReferenceNbr",
                    "Status",
                    "Date",
                    "Type",
              
                    "Description",
                    "CustomerID",
                    # "Terms",
                    "DetailTotal",
                    "TaxTotal",
                    "Amount",
                    "Balance",

                    "Details/Location",
                    "Details/BranchID",
                    "Details/ShipmentNbr",
                    "Details/OrderType",
                    "Details/OrderNbr",
                    "Details/InventoryID",
                    "Details/TransactionDescr",
                    "Details/WarehouseID",
                    "Details/Location",
                    "Details/UOM",
                    "Details/Qty",
                    "Details/UnitPrice",
                    "Details/DiscountAmount",
                    "Details/DiscountPercent",
                    # "Details/ManualPrice",
                    "Details/Amount",
                    # "Details/Account",
                    # "Details/SubAccount",

                    # "TaxDetails/TaxID",
                    # "TaxDetails/TaxableAmount",
                    # "TaxDetails/TaxAmount",

                    "DiscountDetails/DiscountCode",
                    "DiscountDetails/SequenceID",
                    "DiscountDetails/Type",
                    "DiscountDetails/DiscountableAmount",
                    "DiscountDetails/DiscountableQty",
                    "DiscountDetails/DiscountPercent",
                    "DiscountDetails/DiscountAmount",

                ],
                'expand': "Details,Commissions,DiscountDetails,BillingSettings/BillToContact,BillingSettings/BillToAddress"
            },
            'Invoice' : {
                'select_fields': [
                    "ReferenceNbr",
                    "LocationID",
                    "PostPeriod",
                    "Details/ExtendedPrice",
                ],
                'expand': "Details"
            }
        }
        
        # Get entity-specific config or use default
        config = entity_configs.get(entity_endpoint, {
            'select_fields': [],
            'expand': None
        })
        
            # Build API URL 
        # api_url = f'{tenant_url}/entity/Default/20.200.001/{entity_endpoint}'
        api_url = f'{tenant_url}/entity/Default/23.200.001/{entity_endpoint}'
        
        # Add query parameters
        query_params = []
        if config['select_fields']:
            select_string = ",".join(config['select_fields'])
            query_params.append(f"$select={select_string}")
        if config['expand']:
            query_params.append(f"$expand={config['expand']}")
        
        # Add query string if we have parameters
        if query_params:
            api_url += '?' + '&'.join(query_params)
        
        print(f"Fetching from Acumatica API: {api_url}")
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        resp = requests.get(api_url, headers=headers, timeout=(10, 3600))
        
        data = {}
        try:
            data = resp.json()
        except Exception:
            data = {'raw': resp.text}
        
        print(f"API Response Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"API Error Response: {data}")
            
        return resp.status_code, data
        
    except requests.RequestException as e:
        return 500, {'error': f'Network error: {str(e)}'}

# ============================================================================
# FIREBASE STORAGE UTILITIES
# ============================================================================

def delete_old_files_from_firebase_storage(bucket_name, user_email, entity_name):
    try:
        # Use explicit Firebase Storage bucket
        bucket = storage.bucket(bucket_name)
        
        # Sanitize user email for file path
        safe_email = user_email.replace('@', '_').replace('.', '_').replace('+', '_')
        folder_path = f"{safe_email}/data/acumatica/"
        
        print(f"Looking for old files in folder: {folder_path}")
        
        # List all blobs in the user's folder
        blobs = bucket.list_blobs(prefix=folder_path)
        
        deleted_files = []
        for blob in blobs:
            # Check if this blob is for the specific entity (supports both .pkl and .csv files)
            if (blob.name.endswith('.pkl') or blob.name.endswith('.csv')) and entity_name in blob.name:
                try:
                    print(f"Deleting old file: {blob.name}")
                    blob.delete()
                    deleted_files.append(blob.name)
                except Exception as e:
                    print(f"Failed to delete {blob.name}: {e}")
        
        print(f"Deleted {len(deleted_files)} old files for {entity_name}")
        
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

def save_dataframe_to_csv_firebase_storage(dataframe, bucket_name, file_path):
    """
    Save DataFrame to Firebase Storage as CSV file.
    
    Args:
        dataframe: pandas DataFrame to save
        bucket_name: Firebase Storage bucket name
        file_path: Path where CSV file should be saved
        
    Returns:
        Dictionary with save status and file information
    """
    try:
        # Use explicit Firebase Storage bucket
        bucket = storage.bucket(bucket_name)
        
        print(f"Using Firebase Storage bucket: {bucket.name}")
        
        # Create blob object
        blob = bucket.blob(file_path)
        
        # Convert DataFrame to CSV format
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode('utf-8')
        
        # Upload to Firebase Storage
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        # Set public access
        blob.make_public()
        
        # Get public URL
        public_url = blob.public_url
        
        print(f"CSV saved to Firebase Storage: gs://{bucket.name}/{file_path}")
        print(f"DataFrame shape: {dataframe.shape}")
        print(f"File size: {len(csv_data)} bytes")
        print(f"Public URL: {public_url}")
        
        return {
            "status": "success",
            "bucket": bucket.name,
            "file_path": file_path,
            "file_size": len(csv_data),
            "dataframe_shape": dataframe.shape,
            "firebase_url": f"gs://{bucket.name}/{file_path}",
            "public_url": public_url,
            "file_type": "csv"
        }
        
    except Exception as e:
        error_msg = f"Failed to save CSV to Firebase Storage: {str(e)}"
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

@https_fn.on_request(timeout_sec=3600)
def zingworks_acumatica_connector(req: https_fn.Request) -> https_fn.Response:
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
        
        # Subject area DATA for Acumatica
        # NOTE: Only StockItem is currently supported with full data processing
        # Other entities will require additional processing logic in dataProcessing.py
        SUBJECT_AREA_ENTITIES = {
            'Financial': ['Customer', 'Vendor', 'JournalTransaction'],
            'Sales': ['SalesOrder', 'SalesInvoice', 'Shipment', 'Invoice'],
            'Inventory': ['StockItem']  # Only StockItem for now - has WarehouseDetails expansion
        }
        
        # Firebase Storage configuration - explicit bucket name
        FIREBASE_BUCKET_NAME = "insightbot-dev-474509.firebasestorage.app"
        
        # Extract parameters from request body
        try:
            request_data = req.get_json() if req.method == 'POST' else {}
            user_email = request_data.get('email') or request_data.get('user_email') or request_data.get('username')
            access_token = request_data.get('accessToken') or request_data.get('access_token')

            # Acumatica credentials
            client_id = request_data.get('client_id')
            client_secret = request_data.get('client_secret')
            tenant_url = request_data.get('tenant_url')
           
            # Subject area parameters
            subject_area = request_data.get('subject_area', 'inventory')  # Default to financial
            subject_area_name = request_data.get('subject_area_name', 'inventory')
            subject_area_description = request_data.get('subject_area_description', 'Inventory data')
            
            if not client_id or not client_secret or not tenant_url:
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": "Client ID, client secret, and tenant URL are required in request body.",
                        "error_type": "MissingCredentials"
                    }),
                    status=400,
                    headers=headers
                )
            
            # Validate required parameters
            if not user_email:
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": "Email is required in request body to retrieve configuration from Secret Manager.",
                        "error_type": "MissingEmail"
                    }),
                    status=400,
                    headers=headers
                )
            
            if not access_token:
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": "Access token is required in request body. Provide 'accessToken' field.",
                        "error_type": "MissingAccessToken"
                    }),
                    status=400,
                    headers=headers
                )
            
            print(f"Processing request for user: {user_email}")
            
            # Validate subject area
            if subject_area not in SUBJECT_AREA_ENTITIES:
                return https_fn.Response(
                    json.dumps({
                        "status": "error",
                        "message": f"Invalid subject area '{subject_area}'. Supported areas: {list(SUBJECT_AREA_ENTITIES.keys())}",
                        "error_type": "InvalidSubjectArea"
                    }),
                    status=400,
                    headers=headers
                )
            
            # Get entities for the selected subject area
            entity_endpoints = SUBJECT_AREA_ENTITIES[subject_area]  
            print(f"Processing subject area '{subject_area}' with entities: {entity_endpoints}")
            
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
        
        # ====================================================================
        # STEP 1: RETRIEVE ACUMATICA CONFIGURATION FROM SECRET MANAGER
        # ====================================================================
        # config_result = get_acumatica_credentials(user_email)
        config_result = {
            "tenant_url": tenant_url,
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        if not config_result:
            return https_fn.Response(
                json.dumps({
                    "status": "error",
                    "message": "Failed to retrieve Acumatica credentials from Secret Manager",
                    "error_type": "ConfigRetrievalFailed"
                }), 
                status=401, 
                headers=headers
            )
        
        # Validate required credential fields
        if not config_result.get('tenant_url') or not config_result.get('client_id') or not config_result.get('client_secret'):
            return https_fn.Response(
                json.dumps({
                    "status": "error",
                    "message": "Incomplete Acumatica credentials in Secret Manager",
                    "error_type": "IncompleteCredentials",
                    "missing_fields": [
                        field for field in ['tenant_url', 'client_id', 'client_secret'] 
                        if not config_result.get(field)
                    ]
                }), 
                status=401, 
                headers=headers
            )
		

        
        # Extract instance URL from configuration
        instance_url = config_result['tenant_url']
        redirect_url = "http://localhost:3000/acumatica/callback"
        print(f"Using instance URL from Secret Manager: {instance_url}")
        print(f"Using access token from request for authentication")
        
        # ====================================================================
        # STEP 2: PROCESS EACH ENTITY (FETCH ALL RECORDS + SAVE TO FIREBASE)
        # ====================================================================
        
        results = {}
        total_records = 0
        successful_entities = 0
        # Store SalesInvoice dataframe temporarily for merging with Invoice
        sales_invoice_df_temp = None
        
        for entity_endpoint in entity_endpoints:
            try:
                # Fetch all data for this entity using access token
                status_code,fetch_result = fetch_acumatica_data(
                    instance_url,
					entity_endpoint, 
                    access_token,
					redirect_url,
                    bucket_name=FIREBASE_BUCKET_NAME, 
                    user_email=user_email
                )
                if status_code != 200:
                    print(f"ERROR: Failed to fetch data from Acumatica. Status code: {status_code}")
                    print(f"ERROR: Response: {fetch_result}")
                    return https_fn.Response(
						json.dumps({
							"status": "error",
							"message": f"Failed to fetch data from Acumatica. Status code: {status_code}",
							"error_type": "DataFetchError",
                            "status_code": status_code,
                            "response": fetch_result
						}),
                        status=status_code,
                        headers=headers
					)
                else:
                    # Debug: Print raw fetch result structure
                    print(f"DEBUG: Fetch result type: {type(fetch_result)}")
                    print(f"DEBUG: Fetch result keys: {fetch_result.keys() if isinstance(fetch_result, dict) else 'Not a dict'}")
                    if isinstance(fetch_result, dict):
                        if 'value' in fetch_result:
                            print(f"DEBUG: Found 'value' key with {len(fetch_result.get('value', []))} items")
                        else:
                            print(f"DEBUG: No 'value' key found. Keys are: {list(fetch_result.keys())}")
                    print(f"DEBUG: Raw fetch_result (first 500 chars): {str(fetch_result)[:500]}")
                    
                    if entity_endpoint == 'SalesOrder':
                        # Process SalesOrder data (returns single merged DataFrame with all data)
                        sales_order_df = process_sales_order_data(fetch_result)
                        print(f"DEBUG: SalesOrder DF shape: {sales_order_df.shape if sales_order_df is not None else 'None'}")
                        if sales_order_df is not None and not sales_order_df.empty:
                            print(f"DEBUG: SalesOrder DF head:\n{sales_order_df.head()}")
                        else:
                            print("DEBUG: SalesOrder DF is empty or None")
                        
                        # Check if dataframe is None
                        if sales_order_df is None:
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Processed SalesOrder dataframe is None',
                                    'error_type': 'DataProcessingError'
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Check if dataframe is empty
                        if sales_order_df.empty:
                            print(f"WARNING: SalesOrder dataframe is empty for entity {entity_endpoint}")
                            print(f"WARNING: This usually means the API returned no data or data in unexpected format")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Always compute paths and cleanup before saving (following StockItem pattern)
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        sales_order_path = f"{safe_email}/data/acumatica/sales_order_{timestamp}.pkl"
                        
                        # Delete any existing files for this user/object prefix before saving new ones
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "sales_order")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Save dataframe to Firebase Storage (following StockItem pattern)
                        sales_order_save = (
                            save_dataframe_to_firebase_storage(sales_order_df, FIREBASE_BUCKET_NAME, sales_order_path)
                            if not sales_order_df.empty else {"status": "empty"}
                        )
                        
                        # Validate save
                        if isinstance(sales_order_save, dict) and sales_order_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save sales_order to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': sales_order_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Generate preview data for SalesOrder
                        preview_data_sales_order = generate_preview_data(sales_order_df)
                        print("PREVIEW DATA SALES ORDER", preview_data_sales_order)
                        
                        # Build results with storage information and preview data (following StockItem pattern)
                        results[entity_endpoint] = {
                            "record_count": len(sales_order_df) if not sales_order_df.empty else 0,
                            "dataframe_shape": sales_order_df.shape if not sales_order_df.empty else (0, 0),
                            "storage": {
                                "sales_order": sales_order_save
                            },
                            "preview_data": preview_data_sales_order,
                            "status": "success"
                        }
                        total_records += (len(sales_order_df) if not sales_order_df.empty else 0)
                        successful_entities += 1
                    
                    elif entity_endpoint == 'Shipment':
                        # Process Shipment data (simple entity without expansion)
                        shipment_df = process_shipment_data(fetch_result)
                        print(f"DEBUG: Shipment DF shape: {shipment_df.shape if shipment_df is not None else 'None'}")
                        if shipment_df is not None and not shipment_df.empty:
                            print(f"DEBUG: Shipment DF head:\n{shipment_df.head()}")
                        else:
                            print("DEBUG: Shipment DF is empty or None")
                        
                        # Check if dataframe is None or empty
                        if shipment_df is None or shipment_df.empty:
                            print(f"WARNING: Shipment dataframe is empty for entity {entity_endpoint}")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Always compute paths and cleanup before saving (following StockItem pattern)
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        shipment_path = f"{safe_email}/data/acumatica/shipment_{timestamp}.pkl"
                        
                        # Delete any existing files for this user/object prefix before saving new ones
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "shipment")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Save dataframe to Firebase Storage (following StockItem pattern)
                        shipment_save = (
                            save_dataframe_to_firebase_storage(shipment_df, FIREBASE_BUCKET_NAME, shipment_path)
                            if not shipment_df.empty else {"status": "empty"}
                        )
                        
                        # Validate save
                        if isinstance(shipment_save, dict) and shipment_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save shipment to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': shipment_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Generate preview data for Shipment
                        preview_data_shipment = generate_preview_data(shipment_df)
                        print("PREVIEW DATA SHIPMENT", preview_data_shipment)
                        
                        # Build results with storage information and preview data (following StockItem pattern)
                        results[entity_endpoint] = {
                            "record_count": len(shipment_df) if not shipment_df.empty else 0,
                            "dataframe_shape": shipment_df.shape if not shipment_df.empty else (0, 0),
                            "storage": {
                                "shipment": shipment_save
                            },
                            "preview_data": preview_data_shipment,
                            "status": "success"
                        }
                        total_records += (len(shipment_df) if not shipment_df.empty else 0)
                        successful_entities += 1
                    
                    elif entity_endpoint == 'SalesInvoice':
                        # Process SalesInvoice data (returns single merged DataFrame with all data)
                        sales_invoice_df = process_sales_invoice_data(fetch_result)
                        print(f"DEBUG: SalesInvoice DF shape: {sales_invoice_df.shape if sales_invoice_df is not None else 'None'}")
                        if sales_invoice_df is not None and not sales_invoice_df.empty:
                            print(f"DEBUG: SalesInvoice DF head:\n{sales_invoice_df.head()}")
                        else:
                            print("DEBUG: SalesInvoice DF is empty or None")
                        
                        # Check if dataframe is None
                        if sales_invoice_df is None:
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Processed SalesInvoice dataframe is None',
                                    'error_type': 'DataProcessingError'
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Check if dataframe is empty
                        if sales_invoice_df.empty:
                            print(f"WARNING: SalesInvoice dataframe is empty for entity {entity_endpoint}")
                            print(f"WARNING: This usually means the API returned no data or data in unexpected format")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Always compute paths and cleanup before saving (following StockItem pattern)
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        sales_invoice_path = f"{safe_email}/data/acumatica/sales_invoice_{timestamp}.pkl"
                        
                        # Delete any existing files for this user/object prefix before saving new ones
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "sales_invoice")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Save dataframe to Firebase Storage (following StockItem pattern)
                        sales_invoice_save = (
                            save_dataframe_to_firebase_storage(sales_invoice_df, FIREBASE_BUCKET_NAME, sales_invoice_path)
                            if not sales_invoice_df.empty else {"status": "empty"}
                        )
                        
                        # Validate save
                        if isinstance(sales_invoice_save, dict) and sales_invoice_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save sales_invoice to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': sales_invoice_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Store SalesInvoice dataframe temporarily for merging with Invoice later
                        # Don't save it yet - will merge with Invoice and save together
                        sales_invoice_df_temp = sales_invoice_df.copy()
                        print("Stored SalesInvoice dataframe for merging with Invoice")
                    
                    elif entity_endpoint == 'Invoice':
                        # Process Invoice data (returns single merged DataFrame with Details expansion)
                        invoice_df = process_invoice_data(fetch_result)
                        print(f"DEBUG: Invoice DF shape: {invoice_df.shape if invoice_df is not None else 'None'}")
                        if invoice_df is not None and not invoice_df.empty:
                            print(f"DEBUG: Invoice DF head:\n{invoice_df.head()}")
                        else:
                            print("DEBUG: Invoice DF is empty or None")
                        
                        # Check if dataframe is None
                        if invoice_df is None:
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Processed Invoice dataframe is None',
                                    'error_type': 'DataProcessingError'
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Check if dataframe is empty
                        if invoice_df.empty:
                            print(f"WARNING: Invoice dataframe is empty for entity {entity_endpoint}")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Merge SalesInvoice and Invoice dataframes on ReferenceNbr if SalesInvoice exists
                        merged_invoice_df = invoice_df.copy()
                        if sales_invoice_df_temp is not None and not sales_invoice_df_temp.empty:
                            print("Merging SalesInvoice and Invoice dataframes on ReferenceNbr (row-by-row)")
                            # Add row numbers per ReferenceNbr for row-by-row matching
                            sales_invoice_df_temp = sales_invoice_df_temp.copy()
                            invoice_df_temp = invoice_df.copy()
                            
                            sales_invoice_df_temp['row_num'] = sales_invoice_df_temp.groupby('ReferenceNbr').cumcount()
                            invoice_df_temp['row_num'] = invoice_df_temp.groupby('ReferenceNbr').cumcount()
                            
                            # Merge Invoice data into SalesInvoice (left join to keep all SalesInvoice records)
                            # Merge on both ReferenceNbr and row_num for row-by-row matching
                            merged_invoice_df = sales_invoice_df_temp.merge(
                                invoice_df_temp, 
                                on=['ReferenceNbr', 'row_num'], 
                                how='left', 
                                suffixes=('', '_Invoice')
                            )
                            
                            # Drop helper column
                            merged_invoice_df.drop(columns=['row_num'], inplace=True)
                            
                            print(f"DEBUG: Merged Invoice DF shape: {merged_invoice_df.shape}")
                            print(f"DEBUG: Merged Invoice DF columns: {list(merged_invoice_df.columns)}")
                        else:
                            print("WARNING: SalesInvoice dataframe not available for merging, using Invoice dataframe only")
                            merged_invoice_df = invoice_df.copy()
                        
                        # Compute paths and cleanup before saving
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        sales_invoice_path = f"{safe_email}/data/acumatica/sales_invoice_{timestamp}.pkl"
                        
                        # Delete any existing SalesInvoice files for this user
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "sales_invoice")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Save merged SalesInvoice dataframe to Firebase Storage as PKL
                        sales_invoice_save = (
                            save_dataframe_to_firebase_storage(merged_invoice_df, FIREBASE_BUCKET_NAME, sales_invoice_path)
                            if not merged_invoice_df.empty else {"status": "empty"}
                        )
                        
                        # Validate save
                        if isinstance(sales_invoice_save, dict) and sales_invoice_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save merged sales_invoice to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': sales_invoice_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Generate preview data for merged SalesInvoice
                        preview_data_sales_invoice = generate_preview_data(merged_invoice_df)
                        print("PREVIEW DATA MERGED SALES INVOICE", preview_data_sales_invoice)
                        
                        # Build results for merged SalesInvoice (includes Invoice data)
                        results['SalesInvoice'] = {
                            "record_count": len(merged_invoice_df) if not merged_invoice_df.empty else 0,
                            "dataframe_shape": merged_invoice_df.shape if not merged_invoice_df.empty else (0, 0),
                            "storage": {
                                "sales_invoice": sales_invoice_save
                            },
                            "preview_data": preview_data_sales_invoice,
                            "status": "success"
                        }
                        total_records += (len(merged_invoice_df) if not merged_invoice_df.empty else 0)
                        successful_entities += 1
                    
                    elif entity_endpoint == 'ItemWarehouse':
                        # Process ItemWarehouse data (simple entity, no expansions)
                        item_warehouse_df = process_item_warehouse_data(fetch_result)
                        print(f"DEBUG: ItemWarehouse DF shape: {item_warehouse_df.shape if item_warehouse_df is not None else 'None'}")
                        if item_warehouse_df is not None and not item_warehouse_df.empty:
                            print(f"DEBUG: ItemWarehouse DF head:\n{item_warehouse_df.head()}")
                        else:
                            print("DEBUG: ItemWarehouse DF is empty or None")
                        
                        # Check if dataframe is None or empty
                        if item_warehouse_df is None or item_warehouse_df.empty:
                            print(f"WARNING: ItemWarehouse dataframe is empty for entity {entity_endpoint}")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Compute paths and cleanup before saving
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        item_warehouse_path = f"{safe_email}/data/acumatica/item_warehouse_{timestamp}.pkl"
                        
                        # Delete any existing ItemWarehouse files for this user
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "item_warehouse")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Save ItemWarehouse dataframe to Firebase Storage as PKL
                        item_warehouse_save = save_dataframe_to_firebase_storage(item_warehouse_df, FIREBASE_BUCKET_NAME, item_warehouse_path)
                        
                        # Validate save
                        if isinstance(item_warehouse_save, dict) and item_warehouse_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save ItemWarehouse to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': item_warehouse_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Generate preview data for ItemWarehouse
                        preview_data_item_warehouse = generate_preview_data(item_warehouse_df)
                        print("PREVIEW DATA ITEM WAREHOUSE", preview_data_item_warehouse)
                        
                        # Build results for ItemWarehouse
                        results[entity_endpoint] = {
                            "record_count": len(item_warehouse_df) if not item_warehouse_df.empty else 0,
                            "dataframe_shape": item_warehouse_df.shape if not item_warehouse_df.empty else (0, 0),
                            "storage": {
                                "item_warehouse": item_warehouse_save
                            },
                            "preview_data": preview_data_item_warehouse,
                            "status": "success"
                        }
                        total_records += (len(item_warehouse_df) if not item_warehouse_df.empty else 0)
                        successful_entities += 1
                    
                    elif entity_endpoint == 'InventoryReceipt':
                        # Process InventoryReceipt data (simple entity, no expansions)
                        inventory_receipt_df = process_inventory_receipt_data(fetch_result)
                        print(f"DEBUG: InventoryReceipt DF shape: {inventory_receipt_df.shape if inventory_receipt_df is not None else 'None'}")
                        if inventory_receipt_df is not None and not inventory_receipt_df.empty:
                            print(f"DEBUG: InventoryReceipt DF head:\n{inventory_receipt_df.head()}")
                        else:
                            print("DEBUG: InventoryReceipt DF is empty or None")
                        
                        # Check if dataframe is None or empty
                        if inventory_receipt_df is None or inventory_receipt_df.empty:
                            print(f"WARNING: InventoryReceipt dataframe is empty for entity {entity_endpoint}")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Compute paths and cleanup before saving
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        inventory_receipt_path = f"{safe_email}/data/acumatica/inventory_receipt_{timestamp}.pkl"
                        
                        # Delete any existing InventoryReceipt files for this user
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "inventory_receipt")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Save InventoryReceipt dataframe to Firebase Storage as PKL
                        inventory_receipt_save = save_dataframe_to_firebase_storage(inventory_receipt_df, FIREBASE_BUCKET_NAME, inventory_receipt_path)
                        
                        # Validate save
                        if isinstance(inventory_receipt_save, dict) and inventory_receipt_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save InventoryReceipt to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': inventory_receipt_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Generate preview data for InventoryReceipt
                        preview_data_inventory_receipt = generate_preview_data(inventory_receipt_df)
                        print("PREVIEW DATA INVENTORY RECEIPT", preview_data_inventory_receipt)
                        
                        # Build results for InventoryReceipt
                        results[entity_endpoint] = {
                            "record_count": len(inventory_receipt_df) if not inventory_receipt_df.empty else 0,
                            "dataframe_shape": inventory_receipt_df.shape if not inventory_receipt_df.empty else (0, 0),
                            "storage": {
                                "inventory_receipt": inventory_receipt_save
                            },
                            "preview_data": preview_data_inventory_receipt,
                            "status": "success"
                        }
                        total_records += (len(inventory_receipt_df) if not inventory_receipt_df.empty else 0)
                        successful_entities += 1
                    
                    else:
                        # Handle other entities (StockItem, etc.) with existing logic
                        stock_df, warehouse_df = processed_data(fetch_result)
                        print(f"DEBUG: Stock DF shape: {stock_df.shape if stock_df is not None else 'None'}")
                        print(f"DEBUG: Warehouse DF shape: {warehouse_df.shape if warehouse_df is not None else 'None'}")
                        if stock_df is not None and not stock_df.empty:
                            print(f"DEBUG: Stock DF head:\n{stock_df.head()}")
                        else:
                            print("DEBUG: Stock DF is empty or None")
                        if warehouse_df is not None and not warehouse_df.empty:
                            print(f"DEBUG: Warehouse DF head:\n{warehouse_df.head()}")
                        else:
                            print("DEBUG: Warehouse DF is empty or None")
                        
                        # Always compute paths and cleanup before saving
                        safe_email = (user_email or '').replace('@', '_').replace('.', '_').replace('+', '_')
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        stock_path = f"{safe_email}/data/acumatica/stock_items_{timestamp}.pkl"
                        wh_path = f"{safe_email}/data/acumatica/warehouse_{timestamp}.pkl"
                        
                        # Delete any existing files for this user/object prefix before saving new ones
                        try:
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "stock_items")
                            delete_old_files_from_firebase_storage(FIREBASE_BUCKET_NAME, user_email, "warehouse")
                        except Exception as delete_error:
                            print(f"Warning: Failed to delete old files: {delete_error}")
                        
                        # Check if dataframes are None
                        if stock_df is None or warehouse_df is None:
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Processed dataframes are None',
                                    'error_type': 'DataProcessingError'
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Check if both dataframes are empty
                        if stock_df.empty and warehouse_df.empty:
                            print(f"WARNING: Both dataframes are empty for entity {entity_endpoint}")
                            print(f"WARNING: This usually means the API returned no data or data in unexpected format")
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': f'No data returned from Acumatica API for entity {entity_endpoint}',
                                    'error_type': 'EmptyDataResponse',
                                    'entity': entity_endpoint,
                                    'debug_info': {
                                        'fetch_result_type': str(type(fetch_result)),
                                        'fetch_result_keys': list(fetch_result.keys()) if isinstance(fetch_result, dict) else None,
                                        'fetch_result_preview': str(fetch_result)[:500]
                                    }
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Save dataframes to Firebase Storage
                        stock_save = (
                            save_dataframe_to_firebase_storage(stock_df, FIREBASE_BUCKET_NAME, stock_path)
                            if not stock_df.empty else {"status": "empty"}
                        )
                        wh_save = (
                            save_dataframe_to_firebase_storage(warehouse_df, FIREBASE_BUCKET_NAME, wh_path)
                            if not warehouse_df.empty else {"status": "empty"}
                        )
                        
                        # Validate saves
                        if isinstance(stock_save, dict) and stock_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save stock_items to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': stock_save
                                }),
                                status=500,
                                headers=headers
                            )
                        if isinstance(wh_save, dict) and wh_save.get('status') == 'error':
                            return https_fn.Response(
                                json.dumps({
                                    'status': 'error',
                                    'message': 'Failed to save warehouse to storage',
                                    'error_type': 'StorageWriteError',
                                    'detail': wh_save
                                }),
                                status=500,
                                headers=headers
                            )
                        
                        # Generate preview data for both stock_items and warehouse
                        preview_data_stock = generate_preview_data(stock_df)
                        preview_data_warehouse = generate_preview_data(warehouse_df)
                        print("PREVIEW DATA STOCK", preview_data_stock)
                        print("PREVIEW WAREHOUSE STOCK", preview_data_warehouse)

                        # Build results with storage information and preview data
                        results[entity_endpoint] = {
                            "record_count": len(stock_df) if not stock_df.empty else 0,
                            "dataframe_shape": {
                                "stock_items": stock_df.shape if not stock_df.empty else (0, 0),
                                "warehouse": warehouse_df.shape if not warehouse_df.empty else (0, 0)
                            },
                            "storage": {
                                "stock_items": stock_save,
                                "warehouse": wh_save
                            },
                            "preview_data": {
                                "stock_items": preview_data_stock,
                                "warehouse": preview_data_warehouse
                            },
                            "status": "success"
                        }
                        total_records += (len(stock_df) if not stock_df.empty else 0)
                        successful_entities += 1
                    
            except Exception as entity_error:
                error_msg = str(entity_error)
                results[entity_endpoint] = {
                    "record_count": 0,
                    "status": "error",
                    "error": error_msg
                }
        
        # ====================================================================
        # STEP 3: PREPARE AND RETURN RESULTS
        # ====================================================================
        # Determine overall status based on per-entity results
        any_success = any(v.get("status") == "success" for v in results.values())
        overall_status = "success" if any_success else "error"
        overall_message = (
            f"Subject area '{subject_area}' extraction complete: {successful_entities}/{len(entity_endpoints)} entities processed, {total_records} total records"
            if any_success
            else "All entity fetches failed for requested subject area"
        )

        # Ensure results are returned in the same order as entity_endpoints (SalesOrder, SalesInvoice, Shipment)
        ordered_results = {}
        for entity in entity_endpoints:
            if entity in results:
                ordered_results[entity] = results[entity]

        result = {
            "status": overall_status,
            "message": overall_message,
            "subject_area": {
                "id": subject_area,
                "name": subject_area_name,
                "description": subject_area_description,
                "entities": entity_endpoints
            },
            "summary": {
                "total_entities_requested": len(entity_endpoints),
                "successful_entities": successful_entities,
                "total_records_extracted": total_records
            },
            "results": ordered_results
        }
        print("RESULT", result)

        return https_fn.Response(json.dumps(result), headers=headers, status=(200 if any_success else 502))
    
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
        print("ERROR RESULT", error_result)
        
        return https_fn.Response(
            json.dumps(error_result),
            status=500,
            headers={'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'}
        )


# NOTE
# 1. This function is used to fetch data from Acumatica and save it to Firebase Storage
# 2. Data will be stored in the following path: {user_email}/data/acumatica/{entity_name}_{timestamp}.pkl
# 3. If data is fetched again, the previous file will be deleted and a new one will be saved
# 4. Authentication: Uses OAuth access token from request body + instance URL from Secret Manager
#    - Access token must be provided in request body (obtained via OAuth flow)
#    - Instance URL and configuration retrieved from Secret Manager using user email
#    - No login/logout flow needed (token-based authentication)
# 5. Entity endpoints follow Acumatica's REST API structure
# TODO
# 1. Apply an API key to use this function
# 2. Instead of saving the data to Firebase Storage with email, use UserID
# 3. Add support for custom entity endpoints and OData queries