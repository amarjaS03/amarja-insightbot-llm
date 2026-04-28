from typing import Dict, Any


def fail(msg: str, status: int = 400) -> Dict[str, Any]:
    return {
        'result': 'fail',
        'status_code': status,
        'message': msg,
        'error': 'Token operation failed'
    }


def check_token_limit_internal(state: Dict[str, Any], estimated_tokens: int = 0) -> tuple[bool, str, bool]:
    """
    Per-step token check - DISABLED.

    Token validation is done once at the API layer before starting (available >= required).
    We do NOT check at each intermediate step. Total tokens used are calculated and added
    to used_tokens when the process completes.

    Returns:
        Tuple of (can_proceed, message, should_complete_job)
        - can_proceed: Always True (per-step checks disabled)
        - message: Status message
        - should_complete_job: Always False
    """
    # Per-step checks disabled: API layer validates available >= required before start.
    # Total tokens are added to used_tokens at process completion.
    return True, "Per-step token check disabled - API layer validated before start", False


def complete_job_gracefully(state: Dict[str, Any]) -> Dict[str, Any]:
    """Complete job gracefully when tokens are exhausted"""
    user_email = state.get("user_email", "")
    token_message = state.get("token_exhaustion_message", "Token limit reached")
    
    print(f"🔥 [GRACEFUL COMPLETION] Completing job for user {user_email}: {token_message}")
    
    # Set completion flags
    state["analysis_completed_early"] = True
    state["completion_reason"] = "token_limit_reached"
    state["final_message"] = f"Analysis completed with available tokens. {token_message}"
    
    # Ensure we have some basic structure for the report
    if not state.get("final_html_report"):
        partial_report = f"""
        <!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <style>
    .mck-report {{
      font-family: Arial, Helvetica, sans-serif;
      margin: 40px;
      color: #333;
    }}
    .mck-container {{
      max-width: 800px;
      margin: auto;
      background: #fff;
      border-radius: 8px;
    }}
    .mck-title {{
      font-size: 24px;
      margin-bottom: 10px;
      color: #222;
    }}
    .mck-subtitle {{
      font-size: 20px;
      margin-top: 20px;
      color: #b00020;
    }}
    .mck-text {{
      font-size: 14px;
      line-height: 1.6;
    }}
    .mck-error-box {{
      border-left: 4px solid #b00020;
      padding: 15px;
      margin-top: 15px;
      border-radius: 4px;
    }}
    .mck-strong {{
      color: #b00020;
      font-weight: bold;
    }}
    .mck-em {{
      font-style: italic;
    }}
  </style>
</head>
<body>
  <div class="mck-report">
    <div class="mck-container">
        <h2 class="mck-subtitle">Generation Error</h2>
      <div class="mck-error-box">
        <p class="mck-text"><span class="mck-strong">Issue:</span> Token limit exhausted.</p>
        <p class="mck-text">User <span class="mck-strong">{user_email}</span> has no tokens remaining.</p>
        <p class="mck-text"><span class="mck-em">Next step:</span> Contact your admin to allocate additional tokens before retrying.</p>
      </div>
    </div>
  </div>
</body>
</html>

        """
        state["final_html_report"] = partial_report
    
    return state


class TokenLimitExceededException(Exception):
    """Exception raised when user's token limit is exceeded"""

    def __init__(self, message: str, status_code: int = 429):
        super().__init__(message)
        self.message = message
        self.status_code = status_code

    def to_dict(self) -> Dict[str, Any]:
        return {
            'result': 'fail',
            'status_code': self.status_code,
            'message': self.message,
            'error': 'Token operation failed'
        }
