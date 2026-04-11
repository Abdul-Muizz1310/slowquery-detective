"""Rule: Sort node with an ORDER BY not served by an index."""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion


class SortWithoutIndex:
    name = "sort_without_index"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        raise NotImplementedError("S4: implement SortWithoutIndex per docs/specs/03-rules.md")
