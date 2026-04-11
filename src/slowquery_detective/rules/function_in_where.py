"""Rule: WHERE clause wraps a column in a function (LOWER, DATE, ...)."""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion


class FunctionInWhere:
    name = "function_in_where"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        raise NotImplementedError("S4: implement FunctionInWhere per docs/specs/03-rules.md")
