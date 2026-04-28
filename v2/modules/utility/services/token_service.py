"""
Token service for user token validation and updates.

Supports both user ID formats:
- Old: user_id = email (e.g. prathamesh.joshi@zingworks.co) -> userCollection/{email}
- New: user_id = UUID (e.g. OAxdcjvJKwU0wbpQnVmc) -> userCollection/{uuid} with name, email inside

Flow:
- available = issued_token - used_token
- Before process: check available >= required for process type
- After process: add total_used to used_token in Firestore (canonical field: singular)

Canonical Firestore fields: used_token, issued_token (singular). We read both used_token and
used_tokens for backward compat. If you have duplicate fields from a bug, remove used_tokens
and keep used_token in Firestore.
"""

import asyncio
from typing import Dict, Any, Optional, Tuple

from v2.common.logger import add_log
from v2.utils.env import init_env

USER_COLLECTION = "userCollection"

# Required tokens per process type (from constants, with defaults)
_DEFAULT_REQ_TOKENS = {
    "analysis": 50000,
    "simple_qna": 4000,
    "domain_generation": 2000,
    "depseudonymization": 4000,
}


def _get_req_tokens() -> Dict[str, int]:
    """Get required tokens map from constants."""
    constants = init_env()
    req = constants.get("req_token") or constants.get("req_tokens") or {}
    if not isinstance(req, dict):
        return _DEFAULT_REQ_TOKENS.copy()
    out = _DEFAULT_REQ_TOKENS.copy()
    for k, v in req.items():
        try:
            out[str(k).lower()] = int(v)
        except (TypeError, ValueError):
            pass
    return out


async def get_user_tokens(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch user token info from userCollection/{user_id}.
    Supports both old (email as doc id) and new (UUID as doc id) formats.

    Returns:
        {"issued_tokens": int, "used_tokens": int, "available": int, "user_id": str} or None
    """
    if not user_id or not str(user_id).strip():
        return None
    try:
        from v2.common.gcp import GcpManager
        gcp = GcpManager._get_instance()
        fs = gcp._firestore_service
        doc = await fs._get_document(USER_COLLECTION, str(user_id).strip())
        if not doc:
            add_log(f"[TokenService] User not found: {user_id}")
            return None
        # Canonical: used_token, issued_token (singular). Read both for backward compat.
        issued = doc.get("issued_token") or doc.get("issued_tokens", 0)
        used = doc.get("used_token") or doc.get("used_tokens", 0)
        try:
            issued = int(issued)
        except (TypeError, ValueError):
            issued = 0
        try:
            used = int(used)
        except (TypeError, ValueError):
            used = 0
        available = max(0, issued - used)
        return {
            "issued_tokens": issued,
            "used_tokens": used,
            "available": available,
            "user_id": user_id,
            # Legacy shape for execution layer
            "issued_token": issued,
            "used_token": used,
            "remaining_token": available,
        }
    except Exception as e:
        add_log(f"[TokenService] Error fetching user tokens: {e}")
        return None


def get_required_tokens(process_type: str) -> int:
    """Get required tokens for a process type."""
    req = _get_req_tokens()
    return req.get(str(process_type).lower(), 0)


async def check_can_proceed(
    user_id: str,
    process_type: str,
) -> Tuple[bool, str]:
    """
    Check if user has sufficient tokens before starting a process.

    Returns:
        (can_proceed: bool, message: str)
    """
    token_info = await get_user_tokens(user_id)
    if not token_info:
        # No token info: allow (backward compat) but log
        add_log(f"[TokenService] No token info for user {user_id} - allowing operation")
        return True, "No token info available - allowing operation"

    required = get_required_tokens(process_type)
    available = token_info.get("available", 0)

    if required <= 0:
        return True, "No token requirement for this process"

    if available >= required:
        return True, (
            f"Sufficient tokens: {available:,} available (required: {required:,})"
        )

    return False, (
        f"Insufficient tokens. Required: {required:,}, available: {available:,}. "
        "Contact admin to allocate more tokens."
    )


async def add_used_tokens(user_id: str, tokens_to_add: int) -> bool:
    """
    Add tokens to user's used_token in Firestore.
    Uses Firestore increment to avoid race conditions.
    Canonical field: used_token (singular) - matches admin/v1. Do NOT write used_tokens.
    """
    if not user_id or tokens_to_add <= 0:
        return True
    try:
        from google.cloud.firestore_v1.transforms import Increment
        from v2.common.gcp import GcpManager
        gcp = GcpManager._get_instance()
        fs = gcp._firestore_service
        doc_ref = fs._client.collection(USER_COLLECTION).document(str(user_id).strip())

        # Use only used_token (singular) - avoid creating duplicate used_tokens field
        await asyncio.to_thread(
            doc_ref.update,
            {"used_token": Increment(tokens_to_add)}
        )
        add_log(f"[TokenService] Added {tokens_to_add:,} used tokens for user {user_id}")
        return True
    except Exception as e:
        add_log(f"[TokenService] Failed to update used tokens: {e}")
        return False
