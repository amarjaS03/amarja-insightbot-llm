"""
Centralized LLM client for all agents.

Strict policy: all execution-layer LLM calls must use GPT-5.4 on OpenAI
Responses API. Any other model/provider raises a clear error.
"""

import os
import logging
from typing import Any
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv(override=True)

# Module-level singletons — populated by initialize_client()
_openai_client = None
ENFORCED_MODEL = "gpt-5.4"


def vision_image_mime_subtype(suffix: str) -> str:
    """MIME subtype for data:image/<subtype>;base64 URLs (vision). Maps .jpg -> jpeg."""
    s = (suffix or "").lower().lstrip(".")
    if s == "jpg":
        return "jpeg"
    return s if s else "png"


def initialize_client() -> None:
    """
    Initialize LLM provider client(s) from env vars.
    Call once at application startup (ExecutionApi.__init__).

    Env vars:
        MODEL_NAME    — must be "gpt-5.4"
        LLM_PROVIDER  — must be "openai" (or omitted)
        OPENAI_API_KEY
    """
    global _openai_client

    model = os.getenv("MODEL_NAME", ENFORCED_MODEL)
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()

    if not provider:
        provider = "openai"
    if provider != "openai":
        raise RuntimeError(
            f"[llm_client] Strict mode requires LLM_PROVIDER='openai', got '{provider}'."
        )
    if model.strip() != ENFORCED_MODEL:
        raise RuntimeError(
            f"[llm_client] Strict mode requires MODEL_NAME='{ENFORCED_MODEL}', got '{model.strip()}'."
        )

    if provider == "openai":
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        logger.info(f"[llm_client] Initialized OpenAI client (model={model})")


def _resolve_model() -> str:
    model = os.getenv("MODEL_NAME", ENFORCED_MODEL).strip() or ENFORCED_MODEL
    if model != ENFORCED_MODEL:
        raise RuntimeError(
            f"[llm_client] Strict mode requires MODEL_NAME='{ENFORCED_MODEL}', got '{model}'."
        )
    return model


def _resolve_provider(model: str) -> str:
    explicit = os.getenv("LLM_PROVIDER", "").strip().lower()
    provider = explicit or "openai"
    if provider != "openai":
        raise RuntimeError(
            f"[llm_client] Strict mode requires LLM_PROVIDER='openai', got '{provider}'."
        )
    return provider


async def _gpt5_call(
    messages: list,
    *,
    model: str,
    max_output_tokens: int | None,
    json_response: bool,
    **kwargs: Any,
    ) -> tuple[str, dict]:
    global _openai_client
    if _openai_client is None:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    call_kwargs: dict[str, Any] = {}
    if max_output_tokens is not None:
        call_kwargs["max_output_tokens"] = max_output_tokens
    if json_response:
        # No "verbosity" — not supported on GPT-5/reasoning models
        call_kwargs["text"] = {"format": {"type": "json_object"}}
    call_kwargs.update(kwargs)

    response = await _openai_client.responses.create(
        model=model,
        input=messages,
        **call_kwargs,
    )
    return _extract_openai_response(response)


def _extract_openai_response(response) -> tuple[str, dict]:
    text = getattr(response, "output_text", None)
    if not text:
        try:
            text = response.output[0].content[0].text
        except Exception:
            text = ""
    usage = {
        "input_tokens": getattr(response.usage, "input_tokens", 0),
        "output_tokens": getattr(response.usage, "output_tokens", 0),
    }
    return text or "", usage


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------
async def llm_call(
    messages: list,
    *,
    model: str | None = None,
    max_output_tokens: int | None = None,
    json_response: bool = False,
    **kwargs: Any,
    ) -> tuple[str, dict]:
    """
    Make an LLM call routed to the correct provider and adapter.

    Args:
        messages:          OpenAI Responses API-style message list.
                           Multimodal content (input_image) is handled internally.
        model:             Override model name. Must still be "gpt-5.4".
        max_output_tokens: Token limit for the response.
        json_response:     If True, instructs the model to return JSON.
        **kwargs:          Extra provider-specific params forwarded directly
                           (e.g. temperature=0.2).

    Returns:
        (text, usage) where usage = {"input_tokens": int, "output_tokens": int}
    """
    resolved_model = (model or _resolve_model()).strip()
    if resolved_model != ENFORCED_MODEL:
        raise RuntimeError(
            f"[llm_client] Strict mode requires model '{ENFORCED_MODEL}', got '{resolved_model}'."
        )
    provider = _resolve_provider(resolved_model)

    try:
        if provider == "openai":
            return await _gpt5_call(
                messages,
                model=resolved_model,
                max_output_tokens=max_output_tokens,
                json_response=json_response,
                **kwargs,
            )
        raise RuntimeError(f"[llm_client] Unsupported provider in strict mode: {provider}")
    except Exception as e:
        logger.error(f"[llm_client] llm_call failed (model={resolved_model}): {e}")
        raise
