"""
End-to-End Test Script for Simple QnA v2 API Flow

Tests Simple QnA v2 flow:
1. Create chat (deploys Cloud Run service automatically)
2. Run 5-10 queries over the session's data
3. Poll for query completions
4. Verify results

The chat creation uses session_id and user_id to locate input dir and domain directory.
No need to create session, upload file, or generate domain dictionary separately.

Run: python test_simple_qna_e2e.py
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
    CYAN = '\033[96m'
    RESET = '\033[0m'

# Configuration
BASE_URL = "http://localhost:8000/v2/api"
USER_ID = "samarth.mali@zingworks.co"
SESSION_ID = None  # Use existing session or create one
CHAT_ID = None

# Test queries to run (5-10 queries about the uploaded data)
TEST_QUERIES = [
    "What is the total number of records in the dataset?",
    "What are the column names in the dataset?",
    "What are the top 5 most common values in the dataset?",
    "What is the average value of numeric columns?",
    "What are the unique values in the first categorical column?",
    "What is the data distribution across different categories?",
    "Are there any missing values in the dataset?",
    "What are the statistical summary statistics for numeric columns?",
    "What patterns or trends can you identify in the data?",
    "What insights can you provide about the dataset?"
]

# Check if server is running
print("Checking if server is running...")
try:
    response = requests.get(f"{BASE_URL.replace('/v2/api', '')}/docs", timeout=2)
    print(f"{Colors.GREEN}[SUCCESS] Server is running{Colors.RESET}\n")
except requests.exceptions.RequestException:
    print(f"{Colors.RED}[ERROR] Server is not running at {BASE_URL}{Colors.RESET}")
    print(f"{Colors.YELLOW}[INFO] Please start the server with: python v2/main.py{Colors.RESET}")
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

def print_warning(message):
    print(f"{Colors.CYAN}[WARNING] {message}{Colors.RESET}")

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

def poll_query_status(chat_id, query_id, max_wait=300, poll_interval=5):
    """
    Poll for query completion.
    
    Args:
        chat_id: Chat ID
        query_id: Query ID
        max_wait: Maximum seconds to wait
        poll_interval: Seconds between polls
        
    Returns:
        QueryResponse if completed, None if timeout
    """
    print_info(f"Polling for query {query_id[:8]}... (max wait: {max_wait}s)")
    start_time = time.time()
    last_status = None
    
    while time.time() - start_time < max_wait:
        try:
            response = make_request("GET", f"/simple-qna/chats/{chat_id}/queries/{query_id}")
            query = response.get("data")
            
            if query:
                status = query.get("status")
                elapsed = int(time.time() - start_time)
                
                # Only print status if it changed
                if status != last_status:
                    print_info(f"  Status: {status} (elapsed: {elapsed}s)")
                    last_status = status
                
                if status == "completed":
                    print_success(f"  Query completed in {elapsed}s!")
                    return query
                elif status == "error":
                    error_msg = query.get("metadata", {}).get("error", "Unknown error")
                    print_error(f"  Query failed after {elapsed}s: {error_msg}")
                    return query
            
            time.sleep(poll_interval)
        except Exception as e:
            print_warning(f"  Error polling: {str(e)}")
            time.sleep(poll_interval)
    
    elapsed = int(time.time() - start_time)
    print_error(f"  Query did not complete within {max_wait} seconds")
    return None

# Step 0: Get or Create Session (optional - can use existing session)
print_step(0, "Get/Create Session (Optional)")
try:
    # Try to get existing sessions for the user
    sessions_response = make_request("GET", f"/sessions/getByUserId/{USER_ID}")
    sessions = sessions_response.get("data", [])
    
    if sessions:
        # Use the most recent session
        SESSION_ID = sessions[0].get("sessionId")
        print_success(f"Using existing session: {SESSION_ID}")
        print_info(f"Found {len(sessions)} existing session(s)")
    else:
        # Create a new session if none exists
        print_info("No existing sessions found, creating new session...")
        session_data = {
            "dataSource": "csv/excel",
            "label": "Simple QnA Test Session",
            "userId": USER_ID
        }
        session_response = make_request("POST", "/sessions/", json=session_data)
        SESSION_ID = session_response["data"]["sessionId"]
        print_success(f"Created new session: {SESSION_ID}")
        print_warning("Note: Make sure to upload data and generate domain dictionary for this session before running queries")
except Exception as e:
    print_warning(f"Could not get/create session: {str(e)}")
    print_info("You can manually specify SESSION_ID in the script if needed")
    # For testing, you can set SESSION_ID here:
    # SESSION_ID = "your-session-id-here"
    if not SESSION_ID:
        print_error("SESSION_ID is required. Please create a session first or set it in the script.")
        exit(1)

# Step 1: Create Chat (deploys Cloud Run service automatically)
print_step(1, "Create Chat (Deploys Cloud Run Service)")
chat_data = {
    "user_id": USER_ID,
    "session_id": SESSION_ID,
    "label": "E2E Test Chat with Data Analysis",
    "model": "gpt-5.4"
}
print_info(f"Creating chat with data: {json.dumps(chat_data, indent=2)}")
print_info("Note: Chat creation will deploy a Cloud Run service that uses session_id and user_id")
print_info("      to locate the input directory for data and domain directory")
try:
    chat_response = make_request("POST", "/simple-qna/chats", json=chat_data)
    CHAT_ID = chat_response["data"]["chat_id"]
    print_success(f"Chat created: {CHAT_ID}")
    print_info("Cloud Run service should be deployed automatically")
except Exception as e:
    print_error(f"Failed to create chat: {str(e)}")
    exit(1)

# Step 2: Run Multiple Queries (5-10 queries)
print_step(2, f"Run {len(TEST_QUERIES)} Queries Over Session Data")
query_results = []

for idx, query_text in enumerate(TEST_QUERIES, 1):
    print_info(f"\n--- Query {idx}/{len(TEST_QUERIES)} ---")
    print_info(f"Question: {query_text}")
    
    try:
        # Add query (uses chat's Cloud Run service)
        query_data = {
            "chat_id": CHAT_ID,
            "query": query_text,
            "model": "gpt-5.4"
        }
        query_response = make_request("POST", f"/simple-qna/chats/{CHAT_ID}/queries", json=query_data)
        query_id = query_response["data"]["query_id"]
        print_success(f"Query {idx} created: {query_id[:8]}...")
        
        # Poll for completion
        result = poll_query_status(CHAT_ID, query_id, max_wait=300, poll_interval=5)
        
        if result:
            status = result.get("status")
            answer = result.get("answer", "")
            query_results.append({
                "query_number": idx,
                "query_id": query_id,
                "query": query_text,
                "status": status,
                "answer_length": len(answer),
                "answer_preview": answer[:200] if answer else "No answer"
            })
            
            if status == "completed":
                print_success(f"Query {idx} completed successfully!")
                print_info(f"Answer preview: {answer[:200] if answer else 'No answer'}...")
            else:
                print_warning(f"Query {idx} status: {status}")
        else:
            query_results.append({
                "query_number": idx,
                "query_id": query_id,
                "query": query_text,
                "status": "timeout",
                "answer_length": 0,
                "answer_preview": "Query did not complete"
            })
            print_warning(f"Query {idx} did not complete within timeout")
        
        # Small delay between queries
        if idx < len(TEST_QUERIES):
            time.sleep(2)
            
    except Exception as e:
        print_error(f"Query {idx} failed: {str(e)}")
        query_results.append({
            "query_number": idx,
            "query": query_text,
            "status": "error",
            "error": str(e)
        })

# Step 3: Get Chat Details
print_step(3, "Get Chat Details")
try:
    chat_details_response = make_request("GET", f"/simple-qna/chats/{CHAT_ID}")
    chat_details = chat_details_response["data"]
    print_success("Chat details retrieved")
    print_info(f"Chat status: {chat_details.get('status')}")
    print_info(f"Total queries: {chat_details.get('total_queries')}")
    print_info(f"Last query at: {chat_details.get('last_query_at')}")
except Exception as e:
    print_warning(f"Failed to get chat details: {str(e)}")

# Step 4: Get All Queries
print_step(4, "Get All Queries")
try:
    queries_response = make_request("GET", f"/simple-qna/chats/{CHAT_ID}/queries?order_direction=asc")
    queries_list = queries_response["data"]["queries"]
    total_queries = queries_response["data"]["total"]
    print_success(f"Retrieved {total_queries} queries")
    
    print_info("\nQuery Summary:")
    for query in queries_list:
        status_icon = "✓" if query.get("status") == "completed" else "✗" if query.get("status") == "error" else "○"
        print_info(f"  {status_icon} Query {query.get('query_number')}: {query.get('status')} - {query.get('query', '')[:50]}...")
except Exception as e:
    print_warning(f"Failed to get queries: {str(e)}")

# Final Summary
print(f"\n{Colors.GREEN}{'='*60}{Colors.RESET}")
print(f"{Colors.GREEN}[SUCCESS] Simple QnA v2 End-to-End Test Completed!{Colors.RESET}")
print(f"{Colors.GREEN}{'='*60}{Colors.RESET}")
print(f"\nSession ID: {SESSION_ID}")
print(f"Chat ID: {CHAT_ID}")
print(f"User ID: {USER_ID}")
print(f"\nQueries Executed: {len(query_results)}")
print(f"\nQuery Results Summary:")
completed = sum(1 for r in query_results if r.get("status") == "completed")
failed = sum(1 for r in query_results if r.get("status") in ["error", "timeout"])
print(f"  ✓ Completed: {completed}")
print(f"  ✗ Failed/Timeout: {failed}")
print(f"  ○ Other: {len(query_results) - completed - failed}")

print("\nDetailed Results:")
for result in query_results:
    status_icon = "✓" if result.get("status") == "completed" else "✗" if result.get("status") in ["error", "timeout"] else "○"
    print(f"  {status_icon} Query {result.get('query_number')}: {result.get('status')}")
    if result.get("answer_preview"):
        print(f"    Preview: {result.get('answer_preview')[:100]}...")

print("\nAll test steps completed!")
