"""
Centralized LLM client for all agents.

Supports two providers:
  • USE_OPENROUTER (constant below) — set True to force OpenRouter for all agents; ignores
    LLM_PROVIDER for routing. Configure OPENROUTER_API_KEY and optional MODEL_NAME in .env.
  • Otherwise env LLM_PROVIDER:
      openai      → OpenAI (Responses API, e.g. gpt-5.4)
      openrouter  → OpenRouter (official openrouter Python SDK)

Usage in agents:
    from agents.llm_client import llm_call

    text, usage = await llm_call(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_input},
        ],
        max_output_tokens=1000,
        json_response=True,   # optional — enables JSON mode
        temperature=0.2,      # optional kwargs passed through
        seed=42,              # optional — forwarded to both providers (OpenAI routes through
                              #   chat.completions; OpenRouter forwards natively)
    )
    # text: str, usage: {"input_tokens": int, "output_tokens": int}

Legacy usage (agents that still hold self.client):
    from agents.llm_client import get_async_client, get_sync_client, get_model
    self.client = get_async_client()   # ProviderClient (kept for compat)
    self.model  = get_model()

Prefix caching
--------------
  • OpenAI      — automatic for identical prefixes ≥ 1024 tokens.
  • OpenRouter  — system-message cache_control markers injected automatically
                  via the openrouter SDK's native cache_control parameter.
                  User messages are intentionally NOT marked.
"""

import os
import logging
from typing import Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Constants (kept for backwards-compat — agents import ENFORCED_MODEL)
# ---------------------------------------------------------------------------
ENFORCED_MODEL = "gpt-5.4"

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_DEFAULT_MODEL = "meta-llama/llama-3.3-70b-instruct:free"
OPENROUTER_HEADERS = {
    "HTTP-Referer": "https://insightbot.ai",
    "X-Title": "InsightBot",
}

# Single switch: True = force OpenRouter (requires OPENROUTER_API_KEY in .env; optional MODEL_NAME).
USE_OPENROUTER: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def vision_image_mime_subtype(suffix: str) -> str:
    """MIME subtype for data:image/<subtype>;base64 URLs (vision). Maps .jpg -> jpeg."""
    s = (suffix or "").lower().lstrip(".")
    if s == "jpg":
        return "jpeg"
    return s if s else "png"


def _get_provider() -> str:
    if USE_OPENROUTER:
        return "openrouter"
    return (os.getenv("LLM_PROVIDER", "openai") or "openai").strip().lower()


def get_model() -> str:
    """
    Return the model name for the active provider.

    OpenAI:      reads MODEL_NAME, falls back to ENFORCED_MODEL ("gpt-5.4")
    OpenRouter:  reads MODEL_NAME, falls back to OPENROUTER_DEFAULT_MODEL
    """
    provider = _get_provider()
    model = (os.getenv("MODEL_NAME") or "").strip()
    if provider == "openrouter":
        return model or OPENROUTER_DEFAULT_MODEL
    return model or ENFORCED_MODEL


# ---------------------------------------------------------------------------
# Prefix caching — cache_control marker injection (legacy / compat path)
# ---------------------------------------------------------------------------

def _apply_cache_control(messages: list, provider: str) -> list:
    """
    Add cache_control markers to system messages for providers that support
    explicit prefix caching (currently: openrouter).

    Rules:
      • Only "system" role messages are marked — they hold static instructions
        that are identical across calls and are the correct cache boundary.
      • "user" role messages are left untouched — they carry dynamic per-request
        data (user queries, domain directories) that must never be cached.
      • For OpenAI the function is a no-op: the Responses API caches identical
        prefixes automatically with no markup required.

    Content block wrapping:
      • Plain string content  → wrapped in a single text block with cache_control.
      • Already-structured list content → cache_control added to each text block
        that doesn't already have one (image blocks are left unchanged).
    """
    if provider != "openrouter":
        # OpenAI handles prefix caching automatically — no markup needed.
        return messages

    result = []
    for msg in messages:
        if msg.get("role") == "system":
            content = msg["content"]

            if isinstance(content, str) and content.strip():
                # Promote plain string to a structured block with cache_control.
                msg = {
                    "role": "system",
                    "content": [
                        {
                            "type": "text",
                            "text": content,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                }
            elif isinstance(content, list):
                # Add cache_control to every text block that doesn't already have one.
                new_content = []
                for block in content:
                    if (
                        isinstance(block, dict)
                        and block.get("type") == "text"
                        and "cache_control" not in block
                    ):
                        block = {**block, "cache_control": {"type": "ephemeral"}}
                    new_content.append(block)
                msg = {**msg, "content": new_content}
            # If content is empty / non-string-non-list, pass through unchanged.

        result.append(msg)
    return result


# ---------------------------------------------------------------------------
# Normalized response wrapper
# ---------------------------------------------------------------------------

class _ContentItem:
    """Simulates response.output[0].content[0].text"""
    def __init__(self, text: str):
        self.text = text


class _OutputItem:
    """Simulates response.output[0]"""
    def __init__(self, text: str):
        self.content = [_ContentItem(text)]


class _NormalizedUsage:
    """
    Wraps chat.completions usage to expose the Responses API field names
    (input_tokens / output_tokens) alongside the standard ones.
    """
    def __init__(self, usage):
        self.prompt_tokens     = getattr(usage, "prompt_tokens", 0) or 0
        self.completion_tokens = getattr(usage, "completion_tokens", 0) or 0
        self.total_tokens      = getattr(usage, "total_tokens", 0) or 0
        # Responses API aliases
        self.input_tokens  = self.prompt_tokens
        self.output_tokens = self.completion_tokens
        # Prefix caching — always 0 on the compat path (OpenRouter free tier does
        # not return per-call cached token counts; real values come from the
        # dedicated OpenAI path in llm_call()).
        self.cached_tokens = 0


class _NormalizedResponse:
    """
    Wraps a chat.completions response to look like an OpenAI Responses API response.

    Agents access:
        response.output_text                   (primary text path)
        response.output[0].content[0].text     (fallback path)
        response.usage.input_tokens            (token counting)
        response.usage.output_tokens
    """
    def __init__(self, raw):
        self._raw = raw
        text = ""
        if raw.choices:
            text = raw.choices[0].message.content or ""
        self.output_text: str = text
        self.output = [_OutputItem(text)]
        self.usage = _NormalizedUsage(raw.usage) if raw.usage else _NormalizedUsage(None)


# ---------------------------------------------------------------------------
# Message format translation: Responses API → Chat Completions
# ---------------------------------------------------------------------------

def _translate_messages(input_messages: list) -> list:
    """
    Convert OpenAI Responses API message list to Chat Completions message list.

    Responses API content types translated:
        {"type": "input_text",  "text": "..."}
            → {"type": "text",      "text": "..."}
        {"type": "input_image", "image_url": "data:..."}
            → {"type": "image_url", "image_url": {"url": "data:..."}}
    """
    result = []
    for msg in input_messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        if isinstance(content, str):
            result.append({"role": role, "content": content})
        elif isinstance(content, list):
            translated_parts = []
            for part in content:
                part_type = part.get("type", "")
                if part_type == "input_text":
                    translated_parts.append({"type": "text", "text": part["text"]})
                elif part_type == "input_image":
                    translated_parts.append({
                        "type": "image_url",
                        "image_url": {"url": part["image_url"]},
                    })
                else:
                    translated_parts.append(part)
            result.append({"role": role, "content": translated_parts})
        else:
            result.append({"role": role, "content": content})
    return result


# ---------------------------------------------------------------------------
# Responses API shim for OpenRouter (legacy compat — used by ProviderClient)
# ---------------------------------------------------------------------------

class _ResponsesShim:
    """
    Provides a .create() method with the OpenAI Responses API signature,
    internally translating to chat.completions.create() for OpenRouter.

    Also applies _apply_cache_control() on the translated messages so every
    static system prompt is automatically marked for prefix caching.
    """

    def __init__(self, chat_completions):
        self._chat = chat_completions

    async def create(
        self,
        *,
        model: str,
        input: list,
        max_output_tokens: int | None = None,
        text: dict | None = None,
        **kwargs: Any,
    ) -> _NormalizedResponse:
        # 1. Translate Responses API message format → Chat Completions format.
        messages = _translate_messages(input)

        # 2. Inject cache_control on system messages for OpenRouter prefix caching.
        #    _ResponsesShim is only instantiated for the openrouter provider path,
        #    so we pass "openrouter" directly.
        messages = _apply_cache_control(messages, "openrouter")

        call_kwargs: dict[str, Any] = {}

        # 3. max_output_tokens → max_tokens
        if max_output_tokens is not None:
            call_kwargs["max_tokens"] = max_output_tokens

        # 4. text={"format": {"type": "json_object"}} → response_format={"type": "json_object"}
        if text and isinstance(text, dict):
            fmt = text.get("format", {})
            if isinstance(fmt, dict) and fmt.get("type") == "json_object":
                call_kwargs["response_format"] = {"type": "json_object"}

        call_kwargs.update(kwargs)

        raw = await self._chat.create(model=model, messages=messages, **call_kwargs)
        return _NormalizedResponse(raw)


# ---------------------------------------------------------------------------
# Chat Completions wrapper — cache_control injection for direct chat paths
# ---------------------------------------------------------------------------

class _ChatCompletionsWrapper:
    """
    Wraps chat.completions.create() to inject cache_control markers on system
    messages before forwarding the call to the underlying provider.

    Agents that call self.client.chat.completions.create(...) directly get
    prefix caching for free without any changes at the call site.
    """

    def __init__(self, completions, provider: str):
        self._completions = completions
        self._provider = provider

    async def create(self, *, messages: list, **kwargs: Any):
        messages = _apply_cache_control(messages, self._provider)
        return await self._completions.create(messages=messages, **kwargs)

    def __getattr__(self, name: str):
        # Forward any other attribute access (e.g. .with_raw_response) to the
        # underlying completions object so the wrapper is transparent.
        return getattr(self._completions, name)


class _ChatWrapper:
    """
    Wraps the 'chat' namespace to expose a cache-aware completions attribute.
    All other chat sub-resources (e.g. chat.models) are passed through unchanged.
    """

    def __init__(self, chat, provider: str):
        self._chat = chat
        self.completions = _ChatCompletionsWrapper(chat.completions, provider)

    def __getattr__(self, name: str):
        return getattr(self._chat, name)


# ---------------------------------------------------------------------------
# ProviderClient — unified async client wrapper (kept for compat)
# ---------------------------------------------------------------------------

class ProviderClient:
    """
    Provider-agnostic async client.

    Exposes:
        .responses.create(model, input, max_output_tokens, text, ...)
            → OpenAI Responses API (openai) or translated + cache-marked
              chat completions (openrouter)
        .chat.completions.create(...)
            → cache-aware wrapper for openrouter; direct pass-through for openai
    """

    def __init__(self, underlying, provider: str):
        self._underlying = underlying
        self._provider = provider

        if provider == "openrouter":
            # Wrap chat so cache_control is injected on every completions call.
            self.chat = _ChatWrapper(underlying.chat, provider)
            # _ResponsesShim translates Responses API calls + applies cache_control internally.
            self.responses = _ResponsesShim(underlying.chat.completions)
        else:
            # OpenAI: pass through unchanged — prefix caching is automatic.
            self.chat = underlying.chat
            self.responses = underlying.responses


# ---------------------------------------------------------------------------
# Public factory functions (kept for compat — agents set self.client)
# ---------------------------------------------------------------------------

def get_async_client() -> ProviderClient:
    """
    Return a ProviderClient for async agent use.

    Reads LLM_PROVIDER from env:
        openai      → AsyncOpenAI with OPENAI_API_KEY
        openrouter  → AsyncOpenAI pointed at OpenRouter with OPENROUTER_API_KEY
    """
    from openai import AsyncOpenAI

    provider = _get_provider()

    if provider == "openrouter":
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError(
                "[llm_client] LLM_PROVIDER=openrouter but OPENROUTER_API_KEY is not set."
            )
        underlying = AsyncOpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=api_key,
            default_headers=OPENROUTER_HEADERS,
        )
        logger.info(f"[llm_client] Initialized OpenRouter async client (model={get_model()})")
    else:
        underlying = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        logger.info(f"[llm_client] Initialized OpenAI async client (model={get_model()})")

    return ProviderClient(underlying, provider)


def get_sync_client():
    """
    Return a sync OpenAI-compatible client for thread-pool use.

    Reads LLM_PROVIDER from env:
        openai      → openai.OpenAI with OPENAI_API_KEY
        openrouter  → openai.OpenAI pointed at OpenRouter with OPENROUTER_API_KEY
    """
    import openai

    provider = _get_provider()

    if provider == "openrouter":
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError(
                "[llm_client] LLM_PROVIDER=openrouter but OPENROUTER_API_KEY is not set."
            )
        client = openai.OpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=api_key,
            default_headers=OPENROUTER_HEADERS,
        )
        logger.info(f"[llm_client] Initialized OpenRouter sync client (model={get_model()})")
    else:
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        logger.info(f"[llm_client] Initialized OpenAI sync client (model={get_model()})")

    return client


# ---------------------------------------------------------------------------
# Public helper — apply cache_control for manual call sites
# ---------------------------------------------------------------------------

def apply_cache_control(messages: list) -> list:
    """
    Public wrapper around _apply_cache_control using the current env provider.
    """
    return _apply_cache_control(messages, _get_provider())


# ---------------------------------------------------------------------------
# Legacy initialize_client (kept for call-sites in execution_api.py)
# ---------------------------------------------------------------------------

def initialize_client() -> None:
    """
    Initialize LLM provider client(s) from env vars.
    Call once at application startup (ExecutionApi.__init__).
    """
    provider = _get_provider()
    model = get_model()
    logger.info(
        f"[llm_client] initialize_client: provider={provider}, model={model}, "
        f"USE_OPENROUTER_switch={USE_OPENROUTER}"
    )

    if provider not in ("openai", "openrouter"):
        logger.warning(
            f"[llm_client] Unknown LLM_PROVIDER '{provider}'. "
            "Falling back to 'openai'. Supported: openai, openrouter."
        )


# ---------------------------------------------------------------------------
# OpenRouter SDK call — uses the official `openrouter` Python package
# ---------------------------------------------------------------------------

async def _openrouter_llm_call(
    messages: list,
    model: str,
    max_output_tokens: int | None,
    json_response: bool,
    **kwargs: Any,
) -> tuple[str, dict]:
    """
    Make a call via the official openrouter Python SDK.

    Messages are accepted in Responses API format (input_text / input_image)
    and translated to Chat Completions format before sending.
    System messages are marked with cache_control for prefix caching.
    """
    from openrouter import OpenRouter

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "[llm_client] LLM_PROVIDER=openrouter but OPENROUTER_API_KEY is not set."
        )

    # Translate input_text / input_image → text / image_url (Chat Completions format)
    translated = _translate_messages(messages)

    # Build call parameters
    call_params: dict[str, Any] = {
        "model": model,
        "messages": translated,
        "http_referer": OPENROUTER_HEADERS["HTTP-Referer"],
        "x_open_router_title": OPENROUTER_HEADERS["X-Title"],
    }

    if max_output_tokens is not None:
        call_params["max_tokens"] = max_output_tokens

    if json_response:
        # openrouter SDK accepts TypedDict: {"type": "json_object"}
        call_params["response_format"] = {"type": "json_object"}

    # Pass through extra kwargs (temperature, top_p, seed, etc.)
    # Only forward params the SDK explicitly supports to avoid TypeErrors.
    _SUPPORTED_KWARGS = {
        "temperature", "top_p", "top_logprobs", "seed", "presence_penalty",
        "frequency_penalty", "max_completion_tokens", "logit_bias", "logprobs",
        "stop", "user",
    }
    for key, val in kwargs.items():
        if key in _SUPPORTED_KWARGS:
            call_params[key] = val
        else:
            logger.debug(f"[llm_client] Ignoring unsupported openrouter kwarg: {key}")

    with OpenRouter(api_key=api_key) as client:
        response = await client.chat.send_async(**call_params)

    text = ""
    if response.choices:
        text = response.choices[0].message.content or ""

    usage = {
        "input_tokens":  response.usage.prompt_tokens if response.usage else 0,
        "output_tokens": response.usage.completion_tokens if response.usage else 0,
        # OpenRouter free-tier Llama does not return cached token counts.
        "cached_tokens": 0,
    }
    logger.info(
        f"[llm_client] openrouter call done — model={model} "
        f"in={usage['input_tokens']} out={usage['output_tokens']}"
    )
    return text, usage


# ---------------------------------------------------------------------------
# OpenAI Chat Completions path — used when seed= is requested on OpenAI
# ---------------------------------------------------------------------------

async def _openai_chat_llm_call(
    messages: list,
    model: str,
    max_output_tokens: int | None,
    json_response: bool,
    **kwargs: Any,
) -> tuple[str, dict]:
    """
    OpenAI Chat Completions path — used when seed= is requested.

    responses.create does not accept seed; chat.completions.create does
    (supported since OpenAI API version 2023-11-06).

    Returns the same (text, usage) tuple contract as all other paths:
        usage = {"input_tokens": int, "output_tokens": int, "cached_tokens": int}

    Logs system_fingerprint for model-drift detection — this value changes when
    OpenAI silently updates the model's serving configuration, which would
    invalidate reproducibility guarantees even with a fixed seed.
    """
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    translated = _translate_messages(messages)

    call_params: dict[str, Any] = {"model": model, "messages": translated}
    if max_output_tokens is not None:
        call_params["max_completion_tokens"] = max_output_tokens
    if json_response:
        call_params["response_format"] = {"type": "json_object"}
    call_params.update(kwargs)  # seed, temperature, top_p, etc.

    try:
        response = await client.chat.completions.create(**call_params)
    except Exception as e:
        logger.error(f"[llm_client] _openai_chat_llm_call failed (model={model}): {e}")
        raise

    text = ""
    if response.choices:
        text = response.choices[0].message.content or ""

    # system_fingerprint changes when OpenAI silently updates model serving config.
    # Log it so production log searches can detect unexpected model drift.
    fingerprint = getattr(response, "system_fingerprint", None)
    if fingerprint:
        logger.debug(
            f"[llm_client] openai chat system_fingerprint={fingerprint} model={model}"
        )

    usage_obj = response.usage
    cached = 0
    if usage_obj:
        details = getattr(usage_obj, "prompt_tokens_details", None)
        cached = getattr(details, "cached_tokens", 0) or 0

    usage = {
        "input_tokens":  getattr(usage_obj, "prompt_tokens", 0) or 0,
        "output_tokens": getattr(usage_obj, "completion_tokens", 0) or 0,
        "cached_tokens": cached,
    }
    logger.info(
        f"[llm_client] openai chat call done — model={model} "
        f"seed={kwargs.get('seed')} "
        f"in={usage['input_tokens']} out={usage['output_tokens']}"
    )
    return text, usage


# ---------------------------------------------------------------------------
# llm_call — single entry point for ALL LLM calls across every agent
# ---------------------------------------------------------------------------

async def llm_call(
    messages: list,
    *,
    model: str | None = None,
    max_output_tokens: int | None = None,
    json_response: bool = False,
    seed: int | None = None,
    **kwargs: Any,
) -> tuple[str, dict]:
    """
    Make an LLM call routed to the correct provider.

    Args:
        messages:          Message list in Responses API format:
                           [{"role": "system"/"user", "content": str | list}, ...]
                           Multimodal content uses input_text / input_image types.
        model:             Override model name (defaults to get_model()).
        max_output_tokens: Token limit for the response.
        json_response:     If True, instructs the model to return JSON.
        seed:              Optional sampling seed.
                           • OpenRouter: forwarded natively via the openrouter SDK.
                           • OpenAI: routes through ``chat.completions`` (supports seed)
                             instead of ``responses.create`` (which does not). Response
                             includes ``system_fingerprint`` in logs for model-drift
                             detection.
                           Determinism is best-effort — GPU floating-point variance may
                           still produce minor differences across datacenters.
        **kwargs:          Extra params (e.g. temperature=0.2, top_p=0.9).

    Returns:
        (text, usage) where:
            text  — the model's response as a plain string
            usage — {"input_tokens": int, "output_tokens": int}
    """
    resolved_model = (model or get_model()).strip()
    provider = _get_provider()

    forward_kwargs = dict(kwargs)
    if seed is not None:
        forward_kwargs["seed"] = seed
        logger.debug(f"[llm_client] llm_call seed={seed} model={resolved_model}")

    if provider == "openrouter":
        return await _openrouter_llm_call(
            messages=messages,
            model=resolved_model,
            max_output_tokens=max_output_tokens,
            json_response=json_response,
            **forward_kwargs,
        )

    # --- OpenAI path ---
    # responses.create does not support seed; route seeded calls through chat.completions.
    if "seed" in forward_kwargs:
        return await _openai_chat_llm_call(
            messages=messages,
            model=resolved_model,
            max_output_tokens=max_output_tokens,
            json_response=json_response,
            **forward_kwargs,  # seed, temperature, top_p, etc. all forwarded
        )

    openai_client = get_async_client()
    text_param = {"format": {"type": "json_object"}} if json_response else None
    openai_kwargs = dict(forward_kwargs)

    try:
        response = await openai_client.responses.create(
            model=resolved_model,
            input=messages,
            max_output_tokens=max_output_tokens,
            text=text_param,
            **openai_kwargs,
        )
        text = response.output_text or ""
        details = getattr(response.usage, "input_tokens_details", None)
        usage = {
            "input_tokens":  getattr(response.usage, "input_tokens", 0),
            "output_tokens": getattr(response.usage, "output_tokens", 0),
            # Prefix caching: tokens served from OpenAI's automatic cache at 50% cost.
            # Populated from response.usage.input_tokens_details.cached_tokens.
            "cached_tokens": getattr(details, "cached_tokens", 0),
        }
        return text, usage
    except Exception as e:
        logger.error(f"[llm_client] llm_call failed (model={resolved_model}): {e}")
        raise


