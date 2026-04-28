"""
Session object for v2 SessionManager
Represents a session tied to a user with all fields from Firestore
"""
from typing import Optional, List
from datetime import datetime
from v2.common.logger import add_log
from v2.modules.session_framework.services.session.session_service import SessionService


class Session(SessionService):
    """
    Session object that mirrors exactly what is stored in Firestore.
    Contains all fields from the Firestore session document.
    """
    def __init__(
        self,
        user_id: str,
        session_id: str,
        data_source: str,
        label: str,
        status: str = "active",
        created_on: Optional[datetime] = None,
        current_step: Optional[str] = None,
        next_step: Optional[str] = None,
        credential_id: Optional[int] = None,
        job_ids: Optional[List[str]] = None,
    ) -> None:
        self.user_id: str = user_id
        self.session_id: str = session_id
        self.data_source: str = data_source
        self.label: str = label
        self.status: str = status
        self.created_on: Optional[datetime] = created_on
        self.current_step: Optional[str] = current_step
        self.next_step: Optional[str] = next_step
        self.credential_id: Optional[int] = credential_id
        self.job_ids: List[str] = job_ids or []
        super().__init__()

    def to_dict(self) -> dict:
        """Convert session to dictionary representation with all Firestore fields"""
        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "data_source": self.data_source,
            "label": self.label,
            "status": self.status,
            "created_on": self.created_on.isoformat() if self.created_on else None,
            "current_step": self.current_step,
            "next_step": self.next_step,
            "credential_id": self.credential_id,
            "job_ids": self.job_ids,
        }

    def trigger_depseudonymization(self, job_id: str, container_url: str) -> dict:
        """
        Call v2 pseudonymization API to depseudonymize report.
        payload: {'user_id': self.user_id, 'session_id': self.session_id, 'job_id': job_id}
        """
        import requests
        # container_url should be API base URL, e.g. "http://localhost:8000/api/v2"
        base = container_url.rstrip("/").rstrip("/utility").rstrip("/pseudonymization")
        url = f"{base}/pseudonymization/depseudonymize-report"
        payload = {
            'user_id': self.user_id,
            'session_id': self.session_id,
            'job_id': job_id,
            'firestore_job_id': job_id,  # Same as job_id when job_id is Firestore doc ID
        }
        try:
            response = requests.post(url, json=payload, timeout=300)
            if response.status_code == 200:
                return response.json()
            else:
                return {'error': f"Failed with status {response.status_code}", 'details': response.text}
        except Exception as e:
            add_log(f"Error triggering depseudonymization: {str(e)}")
            return {'error': str(e)}
