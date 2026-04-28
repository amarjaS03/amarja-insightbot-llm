import requests
import os
from utils.env import init_env
from logger import add_log
constants = init_env()
def refresh_google_token(refresh_token: str):
    """Exchange refresh token for new access and ID tokens"""
    try:
        resp = requests.post(
            constants.get('google_token_url'),
            data={
                "refresh_token": refresh_token,
                "client_id": os.getenv("GOOGLE_CLIENT_ID"),
                "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
                "grant_type": "refresh_token"
            }
        )
        if resp.status_code != 200:
            try:
                add_log(f"Refresh token request failed: HTTP {resp.status_code} - {resp.text[:200]}")
            except Exception:
                pass
            return None
        try:
            add_log("Refresh token request succeeded")
        except Exception:
            pass
        return resp.json()
    except Exception as e:
        try:
            add_log(f"Refresh token request exception: {str(e)}")
        except Exception:
            pass
        return None
