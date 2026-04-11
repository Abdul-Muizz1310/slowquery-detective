"""OpenRouter LLM fallback — see ``docs/specs/04-explainer.md``.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, HttpUrl, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from slowquery_detective.rules.base import Suggestion


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


async def explain(
    canonical_sql: str,
    plan_json: dict[str, Any],
    *,
    config: LlmConfig,
    fingerprint_id: str,
    now: float | None = None,
) -> Suggestion | None:
    """Ask an OpenRouter model to diagnose a slow plan; return a Suggestion."""
    raise NotImplementedError("S4: implement explain() per docs/specs/04-explainer.md")
