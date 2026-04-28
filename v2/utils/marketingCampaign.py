import threading
import requests
from utils.env import init_env

def _post_marketing_log(data: dict, timeout: int = 5) -> None:
    try:
        constants = init_env()
        marketing_campaigns_logs_api_url = constants.get("marketing_campaigns_logs_api_url")
        if not marketing_campaigns_logs_api_url:
            return
        requests.post(marketing_campaigns_logs_api_url, json=data, timeout=timeout)
    except Exception:
        # Silent by design
        pass

def marketing_campaign_logs_silent(data: dict, background: bool = True, timeout: int = 5) -> None:
    if background:
        threading.Thread(target=_post_marketing_log, args=(data, timeout), daemon=True).start()
    else:
        _post_marketing_log(data, timeout)

def marketing_campaign_logs(data: dict):
    try:
        constants = init_env()
        marketing_campaigns_logs_api_url = constants.get("marketing_campaigns_logs_api_url")
        if not marketing_campaigns_logs_api_url:
            return {"error": "marketing_campaigns_logs_api_url not configured"}, 500
        response = requests.post(marketing_campaigns_logs_api_url, json=data)
        # Best-effort JSON; if fails, return raw text
        try:
            body = response.json()
        except Exception:
            body = {"text": response.text}
        return body, response.status_code
    except Exception:
        # Silent failure with generic response
        return {"error": "failed to send marketing campaign log"}, 500