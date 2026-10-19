"""Response models for the read routes — spec 07 cases 24-25 (interface I-2).

``QueryRow`` carries exactly the fields interface I-2 pins; ``extra="forbid"``
keeps it that way. ``fingerprint`` is the canonical (literal-free) SQL.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, JsonValue

from slowquery_detective.rules.base import SuggestionKind, SuggestionSource

_FROZEN = ConfigDict(extra="forbid", frozen=True)


class SuggestionOut(BaseModel):
    model_config = _FROZEN

    kind: SuggestionKind
    source: SuggestionSource
    sql: str | None
    rationale: str
    applied_at: datetime | None


class QueryRow(BaseModel):
    model_config = _FROZEN

    id: str
    fingerprint: str
    first_seen: datetime
    last_seen: datetime
    call_count: int
    total_ms: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    max_ms: float
    suggestions: list[SuggestionOut]


class ExplainPlanOut(BaseModel):
    model_config = _FROZEN

    plan_json: dict[str, JsonValue]
    cost: float | None
    captured_at: datetime


class SampleOut(BaseModel):
    model_config = _FROZEN

    duration_ms: float
    request_id: str | None
    sampled_at: datetime


class QueryDetail(BaseModel):
    model_config = _FROZEN

    fingerprint: QueryRow
    canonical_sql: str
    explain_plan: ExplainPlanOut | None
    suggestions: list[SuggestionOut]
    recent_samples: list[SampleOut]
