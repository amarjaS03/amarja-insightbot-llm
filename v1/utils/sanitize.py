import re

def sanitize_email_for_storage(email: str) -> str:
    """Sanitize email for use in GCS paths and IDs. Uses underscores uniformly."""
    if not email:
        return "anonymous"
    return re.sub(r"[^a-zA-Z0-9]+", "_", email.lower()).strip("_")

