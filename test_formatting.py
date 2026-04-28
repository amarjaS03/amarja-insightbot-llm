import requests
import uuid

job_id = str(uuid.uuid4())
url = "http://127.0.0.1:5001/analyze_job"
payload = {
    "query": "What are the most common cancellation reasons for each vehicle type?",
    "job_id": job_id,
    "session_id": "no_session",
    "user_email": "anonymous",
    "dataset_filename": "some_dataset.csv", # will use mocked dataset in local anyway
    "analysis_mode": "slim"
}
print(f"Executing test for job_id {job_id}...")
response = requests.post(url, json=payload)
print(f"Status Code: {response.status_code}")
try:
    print(response.json())
except Exception:
    print(response.text)
