"""Shared rule types + the pure ``run_rules`` dispatcher.

See ``docs/specs/03-rules.md``. All rules are pure functions of
``(plan, canonical_sql, fingerprint_id, recent_call_count)``. The dispatcher
applies every registered rule, returns a stable-sorted list by confidence
desc, then rule_name asc.
"""

from __future__ import annotations

import re
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

SuggestionKind = Literal["index", "rewrite", "denormalize", "partition"]
SuggestionSource = Literal["rules", "llm"]

# Identifiers that a rule is allowed to interpolate into generated DDL.
# Anything failing this regex causes the rule to abstain rather than emit
# a potentially-injectable SQL statement. See spec case 24.
IDENTIFIER_RE: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Small Postgres reserved-word set. When a rule emits DDL for a table or
# column whose name collides with one of these, it double-quotes the
# identifier so the resulting ``CREATE INDEX`` is still valid.
_POSTGRES_RESERVED: frozenset[str] = frozenset(
    {
        "all",
        "and",
        "any",
        "as",
        "asc",
        "between",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "column",
        "constraint",
        "create",
        "current_date",
        "current_time",
        "current_timestamp",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "for",
        "foreign",
        "from",
        "full",
        "grant",
        "group",
        "having",
        "in",
        "inner",
        "intersect",
        "into",
        "is",
        "join",
        "leading",
        "left",
        "like",
        "limit",
        "not",
        "null",
        "offset",
        "on",
        "or",
        "order",
        "outer",
        "primary",
        "references",
        "right",
        "select",
        "session_user",
        "some",
        "symmetric",
        "table",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "when",
        "where",
        "with",
    }
)


def quote_if_reserved(identifier: str) -> str:
    """Double-quote ``identifier`` if it collides with a Postgres keyword."""
    if identifier.lower() in _POSTGRES_RESERVED:
        return f'"{identifier}"'
    return identifier


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


def run_rules(
    plan: dict[str, Any],
    canonical_sql: str,
    *,
    fingerprint_id: str,
    recent_call_count: int = 0,
) -> list[Suggestion]:
    """Apply every registered rule; return matches sorted by confidence desc."""
    # plan access will raise if not a dict — spec case 17.
    if not isinstance(plan, dict):
        raise TypeError("plan must be a dict")

    results: list[Suggestion] = []
    for rule in ALL_RULES:
        try:
            suggestion = rule.apply(
                plan,
                canonical_sql,
                fingerprint_id=fingerprint_id,
                recent_call_count=recent_call_count,
            )
        except Exception:
            continue
        if suggestion is not None:
            results.append(suggestion)

    # Stable sort: by confidence desc, then by rule_name asc so identical
    # confidences produce deterministic ordering.
    results.sort(key=lambda s: (-s.confidence, s.rule_name or ""))
    return results


# --------------------------------------------------------------------------
# Plan-walker utilities shared by concrete rules
# --------------------------------------------------------------------------


def walk_nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Return every ``Plan`` sub-node in a depth-first order.

    Postgres ``EXPLAIN (FORMAT JSON)`` nests children under ``Plans``. A
    top-level plan lives under the key ``Plan``. If neither is present the
    returned list is empty (spec case 20).
    """
    out: list[dict[str, Any]] = []
    root = plan.get("Plan") if isinstance(plan, dict) else None
    if not isinstance(root, dict):
        return out

    stack: list[dict[str, Any]] = [root]
    while stack:
        node = stack.pop()
        out.append(node)
        children = node.get("Plans") or []
        for child in reversed(children):
            if isinstance(child, dict):
                stack.append(child)
    return out


def coerce_int(value: Any) -> int:
    """Accept ints or stringified ints per spec case 21."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


# --------------------------------------------------------------------------
# Rule registry
# --------------------------------------------------------------------------


def _registered_rules() -> tuple[Rule, ...]:
    """Assemble the concrete rule classes.

    Import is lazy so individual rule modules can import ``base`` without
    causing a circular import.
    """
    from slowquery_detective.rules.function_in_where import FunctionInWhere
    from slowquery_detective.rules.missing_fk_index import MissingFkIndex
    from slowquery_detective.rules.n_plus_one import NPlusOneSuspicion
    from slowquery_detective.rules.select_star import SelectStarWideTable
    from slowquery_detective.rules.seq_scan import SeqScanLargeTable
    from slowquery_detective.rules.sort_without_index import SortWithoutIndex

    return (
        SeqScanLargeTable(),
        MissingFkIndex(),
        SortWithoutIndex(),
        FunctionInWhere(),
        SelectStarWideTable(),
        NPlusOneSuspicion(),
    )


ALL_RULES: tuple[Rule, ...] = _registered_rules()
