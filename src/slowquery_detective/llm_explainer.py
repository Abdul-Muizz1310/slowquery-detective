"""OpenRouter LLM fallback — see ``docs/specs/04-explainer.md``.

Talks directly to the OpenRouter REST API via httpx. We deliberately avoid
the ``openai`` client here so the cascade logic, cooldown, and JSON
validation live in one readable place; the ``openai`` extra stays in
pyproject for users who already have it installed but the package itself
only needs httpx.

Behavior contract:

- Called from ``explain.py`` only when the rules engine returns an empty
  list. The module does not re-check that.
- PRIMARY -> FAST -> FALLBACK cascade on HTTP 429 or 5xx / connect errors
  / timeouts. 401 is non-retriable.
- Per-fingerprint cooldown driven by an injected ``now``.
- On any non-abstention-worthy exception the function returns ``None`` and
  logs; it never propagates failures to the caller.
- Never lets the API key or original literal escape into logs.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from slowquery_detective.rules.base import Suggestion, SuggestionKind

_LOG = logging.getLogger("slowquery.llm")

SYSTEM_PROMPT: str = (
    "You are a Postgres performance expert. A query is slow. "
    "Given its canonical SQL and EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
    "plan, return a single JSON object and nothing else."
)

# Map the LLM's ``kind`` enum into our Suggestion's SuggestionKind Literal.
_VALID_KINDS: frozenset[str] = frozenset({"index", "rewrite", "denormalize", "partition"})

# Only CREATE INDEX statements are retained as executable; everything else
# is preserved as diagnostic prose but ``sql`` is nulled out.
_SAFE_DDL_PREFIX = "CREATE INDEX"


class LlmConfig(BaseSettings):
    """Configuration for the OpenRouter-backed explain fallback."""

    model_config = SettingsConfigDict(env_prefix="SLOWQUERY_LLM_", extra="forbid")

    enabled: bool = False
    api_key: SecretStr | None = None
    base_url: HttpUrl = HttpUrl("https://openrouter.ai/api/v1")
    model_primary: str = ""
    model_fast: str = ""
    model_fallback: str = ""
    temperature: float = Field(default=0.1, ge=0.0, le=0.3)
    min_confidence: float = Field(default=0.4, ge=0.0, le=1.0)
    per_fingerprint_cooldown_seconds: float = Field(default=60.0, gt=0.0)

    @field_validator("temperature")
    @classmethod
    def _temperature_cap(cls, v: float) -> float:
        if v > 0.3:
            raise ValueError("temperature must not exceed 0.3")
        return v


class _LlmResponse(BaseModel):
    """Strict schema for the JSON the model must return."""

    model_config = ConfigDict(extra="ignore", strict=True)

    diagnosis: str
    suggestion: str | None
    confidence: float = Field(ge=0.0, le=1.0)
    kind: str


# Module-level cooldown map. ``fingerprint_id -> last_successful_call_time``.
# Process-local; resets on restart. Good enough for a process-scoped rate
# limiter.
_COOLDOWN: dict[str, float] = {}


async def explain(
    canonical_sql: str,
    plan_json: dict[str, Any],
    *,
    config: LlmConfig,
    fingerprint_id: str,
    now: float | None = None,
) -> Suggestion | None:
    """Ask an OpenRouter model to diagnose a slow plan; return a Suggestion.

    Returns ``None`` when disabled, when no API key is set, when the cooldown
    is active, when the model abstains, or when the upstream call fails.
    Never raises on upstream failures — logs and returns ``None``.
    """
    if not isinstance(plan_json, dict):
        raise TypeError("plan_json must be a dict")

    if not config.enabled:
        return None
    if config.api_key is None:
        _LOG.warning("slowquery.llm.missing_key")
        return None

    current = _now_value(now)
    last = _COOLDOWN.get(fingerprint_id)
    if last is not None and current - last < config.per_fingerprint_cooldown_seconds:
        return None

    suggestion = await _cascade(canonical_sql, plan_json, config)

    if suggestion is not None:
        _COOLDOWN[fingerprint_id] = current

    return suggestion


async def _cascade(
    canonical_sql: str,
    plan_json: dict[str, Any],
    config: LlmConfig,
) -> Suggestion | None:
    """Try PRIMARY -> FAST -> FALLBACK; return a Suggestion or None."""
    models = (config.model_primary, config.model_fast, config.model_fallback)

    async with httpx.AsyncClient(timeout=15.0) as client:
        for model in models:
            outcome = await _call_model(client, model, canonical_sql, plan_json, config)
            if outcome.retry:
                continue
            return outcome.suggestion

    _LOG.warning("slowquery.llm.cascade_exhausted")
    return None


class _CallOutcome:
    __slots__ = ("retry", "suggestion")

    def __init__(self, *, retry: bool, suggestion: Suggestion | None) -> None:
        self.retry = retry
        self.suggestion = suggestion


async def _call_model(
    client: httpx.AsyncClient,
    model: str,
    canonical_sql: str,
    plan_json: dict[str, Any],
    config: LlmConfig,
) -> _CallOutcome:
    """Single-model call. Returns retry=True on retriable failures."""
    assert config.api_key is not None  # Guarded by ``explain``.
    url = f"{str(config.base_url).rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {config.api_key.get_secret_value()}",
        "Content-Type": "application/json",
    }
    user_content = f"Canonical SQL:\n{canonical_sql}\n\nPlan JSON:\n{json.dumps(plan_json)}"
    body = {
        "model": model,
        "temperature": config.temperature,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
    }

    try:
        response = await client.post(url, headers=headers, json=body)
    except (httpx.TimeoutException, httpx.ConnectError, httpx.NetworkError):
        _LOG.debug("slowquery.llm.network_failure", extra={"model": model})
        return _CallOutcome(retry=True, suggestion=None)
    except httpx.HTTPError:
        _LOG.debug("slowquery.llm.http_error", extra={"model": model})
        return _CallOutcome(retry=True, suggestion=None)

    if response.status_code == 401:
        _LOG.error("slowquery.llm.auth_failure")
        return _CallOutcome(retry=False, suggestion=None)
    if response.status_code == 429 or response.status_code >= 500:
        return _CallOutcome(retry=True, suggestion=None)
    if response.status_code != 200:
        return _CallOutcome(retry=False, suggestion=None)

    try:
        payload = response.json()
        content_raw = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, json.JSONDecodeError, TypeError):
        _LOG.warning("slowquery.llm.invalid_json")
        return _CallOutcome(retry=False, suggestion=None)

    return _CallOutcome(
        retry=False,
        suggestion=_parse_suggestion(content_raw, config),
    )


def _parse_suggestion(content_raw: str, config: LlmConfig) -> Suggestion | None:
    """Validate the model's JSON reply and project it onto ``Suggestion``."""
    try:
        parsed_json = json.loads(content_raw)
    except (json.JSONDecodeError, TypeError):
        _LOG.warning("slowquery.llm.invalid_json")
        return None

    try:
        parsed = _LlmResponse.model_validate(parsed_json)
    except Exception:
        _LOG.warning("slowquery.llm.invalid_json")
        return None

    if parsed.confidence < config.min_confidence:
        return None
    if parsed.kind not in _VALID_KINDS:
        return None
    if parsed.suggestion is None:
        return None

    # Only allow CREATE INDEX DDL through as executable; anything else
    # becomes diagnostic prose with ``sql=None``.
    sql: str | None = parsed.suggestion
    if sql is not None and not sql.strip().upper().startswith(_SAFE_DDL_PREFIX):
        sql = None

    # Narrow ``kind`` to SuggestionKind. ``_VALID_KINDS`` already filtered.
    kind: SuggestionKind = parsed.kind  # type: ignore[assignment]
    return Suggestion(
        kind=kind,
        sql=sql,
        rationale=parsed.diagnosis,
        confidence=parsed.confidence,
        source="llm",
        rule_name=None,
    )


def _now_value(now: float | None) -> float:
    import time

    return time.monotonic() if now is None else float(now)
