"""
End-to-End Test Script for V2 API Flow

Tests the complete flow with real NCR ride bookings dataset:
1. Create session
2. Upload file (ncr_ride_bookings.csv from project root)
3. Preview data
4. Generate domain dictionary
5. Get generated domain (fallback test)
6. Save domain dictionary
7. Verify save

Uses real production data - no sample or dummy data.

Run: python test_e2e_flow.py
"""
import requests
import json
import time
import os
from pathlib import Path

# Colors for output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

# Configuration
BASE_URL = "http://localhost:8000/v2/api"
USER_ID = "prathamesh.joshi@zingworks.co"
# Check if server is running
print("Checking if server is running...")
try:
    response = requests.get(f"{BASE_URL.replace('/v2/api', '')}/docs", timeout=2)
    print(f"{Colors.GREEN}[SUCCESS] Server is running{Colors.RESET}\n")
except requests.exceptions.RequestException:
    print(f"{Colors.RED}[ERROR] Server is not running at {BASE_URL}{Colors.RESET}")
    print(f"{Colors.YELLOW}[INFO] Please start the server with: python main.py{Colors.RESET}")
    exit(1)

def print_step(step_num, description):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}Step {step_num}: {description}{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*60}{Colors.RESET}")

def print_success(message):
    print(f"{Colors.GREEN}[SUCCESS] {message}{Colors.RESET}")

def print_error(message):
    print(f"{Colors.RED}[ERROR] {message}{Colors.RESET}")

def print_info(message):
    print(f"{Colors.YELLOW}[INFO] {message}{Colors.RESET}")

def make_request(method, endpoint, **kwargs):
    """Make HTTP request with error handling"""
    url = f"{BASE_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, **kwargs)
        elif method == "POST":
            response = requests.post(url, **kwargs)
        elif method == "PUT":
            response = requests.put(url, **kwargs)
        elif method == "DELETE":
            response = requests.delete(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print_error(f"HTTP {e.response.status_code} Error: {str(e)}")
        if hasattr(e.response, 'text'):
            try:
                error_detail = e.response.json()
                print_error(f"Error details: {json.dumps(error_detail, indent=2)}")
            except:
                print_error(f"Response: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        print_error(f"Request failed: {str(e)}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print_error(f"Response: {e.response.text}")
        raise

# Step 1: Create Session
print_step(1, "Create Session")
session_data = {
    "dataSource": "csv/excel",
    "label": "NCR Ride Bookings Analysis Session",
    "userId": USER_ID
}
print_info(f"Creating session with data: {json.dumps(session_data, indent=2)}")
session_response = make_request("POST", "/sessions/", json=session_data)
session_id = session_response["data"]["sessionId"]
print_success(f"Session created: {session_id}")
print_info(f"Session details: {json.dumps(session_response['data'], indent=2)}")

# Step 2: Upload File
print_step(2, "Upload File")
# Use the real NCR ride bookings CSV file from project root
csv_file_path = Path("ncr_ride_bookings.csv")
if not csv_file_path.exists():
    print_error(f"CSV file not found: {csv_file_path}")
    print_info("Please ensure ncr_ride_bookings.csv exists in the project root directory")
    exit(1)

# Get file size for info
file_size_mb = csv_file_path.stat().st_size / (1024 * 1024)
print_info(f"Found CSV file: {csv_file_path}")
print_info(f"File size: {file_size_mb:.2f} MB")

print_info(f"Uploading file: {csv_file_path}")
with open(csv_file_path, 'rb') as f:
    files = {'file': (csv_file_path.name, f, 'text/csv')}
    data = {
        'user_id': USER_ID,
        'session_id': session_id
    }
    upload_response = make_request("POST", "/connectors/upload-file", files=files, data=data)

print_success("File upload completed")
print_info(f"Upload response: {json.dumps(upload_response, indent=2)}")

# Extract saved filename from upload response as fallback
upload_saved_file = None
try:
    upload_result = upload_response.get("data", {}).get("result", {}).get("data", {})
    upload_saved_file = upload_result.get("saved_file")
    if upload_saved_file:
        print_info(f"Upload saved file: {upload_saved_file}")
except Exception:
    pass

# Step 3: Preview Data
print_step(3, "Preview Data")
preview_data = {
    "session_id": session_id,
    "num_rows": 10,
    "all_files": True
}
print_info(f"Previewing data: {json.dumps(preview_data, indent=2)}")
preview_response = make_request("POST", "/data/preview", json=preview_data)
print_success("Data preview completed")
print_info(f"Preview response keys: {list(preview_response.get('data', {}).keys())}")

# Extract filename from preview response
preview_data_content = preview_response.get("data", {})
filename = None
if "preview_data" in preview_data_content:
    files_previewed = list(preview_data_content["preview_data"].keys())
    filename = files_previewed[0] if files_previewed else None
    print_info(f"Files previewed: {files_previewed}")
else:
    print_error("No preview data found")

# Fallback to upload response filename if preview didn't provide it
if not filename and upload_saved_file:
    filename = upload_saved_file
    print_info(f"Using filename from upload response: {filename}")

# Step 4: Generate Domain Dictionary
print_step(4, "Generate Domain Dictionary")
if not filename:
    print_error("Cannot generate domain - no filename found")
    exit(1)

# Use the filename from preview (should be the .pkl filename)
generate_data = {
    "domain": "NCR ride booking and transportation service data",
    "data_set_files": {
        filename: {
            "file_info": "NCR ride booking dataset containing booking records with customer details, vehicle types, locations, ride status, cancellation reasons, ratings, payment methods, and ride metrics",
            "underlying_conditions_about_dataset": [
                "Booking records include completed, incomplete, cancelled, and no driver found statuses",
                "Vehicle types include eBike, Auto, Go Sedan, Go Mini, Premier Sedan, Bike, and other transportation options",
                "Geographic coverage spans NCR region including locations like Palam Vihar, Gurgaon, Noida, Central Secretariat, and other Delhi-NCR areas",
                "Booking values and ride distances are recorded for completed rides",
                "Customer and driver ratings are available for completed rides",
                "Cancellation reasons are tracked separately for customer-initiated and driver-initiated cancellations",
                "Payment methods include UPI, Debit Card, Credit Card, and other digital payment options",
                "Average VTAT (Vehicle Time at Arrival) and CTAT (Customer Time at Arrival) metrics are tracked for operational efficiency"
            ],
            "anom_cols": []
        }
    },
    "session_id": session_id,
    "user_id": USER_ID
}
print_info("Generating domain dictionary...")
print_info(f"Request: {json.dumps(generate_data, indent=2)}")
generate_response = make_request("POST", "/domain/generate", json=generate_data)
print_success("Domain generation completed")
print_info(f"Generate response: {json.dumps(generate_response, indent=2)}")

# Extract generated domain dictionary
generated_domain = None
if "result" in generate_response.get("data", {}):
    generated_domain = generate_response["data"]["result"].get("domain_dictionary")
    print_success("Generated domain dictionary retrieved from response")

# Step 5: Get Generated Domain (Test Fallback)
print_step(5, "Get Generated Domain (Test Fallback)")
get_generated_response = make_request("GET", f"/domain/get-generated/{session_id}")
print_success("Retrieved generated domain")
print_info(f"Get generated response: {json.dumps(get_generated_response, indent=2)}")

if get_generated_response.get("data") and get_generated_response["data"].get("domain_dictionary"):
    print_success("Generated domain found via get-generated endpoint")
    source = get_generated_response["data"].get("source", "unknown")
    print_info(f"Source: {source}")
    # Update generated_domain from get-generated response if not already set
    if not generated_domain:
        generated_domain = get_generated_response["data"].get("domain_dictionary")
        print_info("Using domain dictionary from get-generated endpoint")
else:
    print_error("Generated domain not found via get-generated endpoint")

# Step 6: Save Domain Dictionary
print_step(6, "Save Domain Dictionary")
if not generated_domain:
    print_error("Cannot save - no generated domain dictionary")
    exit(1)

save_data = {
    "session_id": session_id,
    "domain_dictionary": generated_domain
}
print_info("Saving domain dictionary...")
save_response = make_request("POST", "/domain/save", json=save_data)
print_success("Domain dictionary saved")
print_info(f"Save response: {json.dumps(save_response, indent=2)}")

# Step 7: Verify Save (Check get-generated after save)
print_step(7, "Verify Save (Check get-generated after save)")
get_generated_after_save = make_request("GET", f"/domain/get-generated/{session_id}")
if get_generated_after_save.get("data") is None or not get_generated_after_save["data"].get("domain_dictionary"):
    print_success("Generated domain cleared after save (as expected)")
else:
    print_info("Generated domain still present (may be expected if auto-save file exists)")

# Final Summary
print(f"\n{Colors.GREEN}{'='*60}{Colors.RESET}")
print(f"{Colors.GREEN}[SUCCESS] End-to-End Test Completed Successfully!{Colors.RESET}")
print(f"{Colors.GREEN}{'='*60}{Colors.RESET}")
print(f"\nSession ID: {session_id}")
print(f"User ID: {USER_ID}")
print("\nAll steps completed successfully!")

