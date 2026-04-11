"""Shared rule types + the pure ``run_rules`` dispatcher.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

SuggestionKind = Literal["index", "rewrite", "denormalize", "partition"]
SuggestionSource = Literal["rules", "llm"]


class Suggestion(BaseModel):
    """A single index / rewrite suggestion produced by a rule or the LLM."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    kind: SuggestionKind
    sql: str | None
    rationale: str
    confidence: float = Field(ge=0.0, le=1.0)
    source: SuggestionSource
    rule_name: str | None = None


class Rule(Protocol):
    name: str

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None: ...


ALL_RULES: tuple[Rule, ...] = ()  # populated in S4 via rules/__init__.py


def run_rules(
    plan: dict[str, Any],
    canonical_sql: str,
    *,
    fingerprint_id: str,
    recent_call_count: int = 0,
) -> list[Suggestion]:
    """Apply every registered rule; return matches sorted by confidence desc."""
    raise NotImplementedError("S4: implement run_rules per docs/specs/03-rules.md")
