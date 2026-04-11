"""Rule: SELECT * on a wide table."""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion


class SelectStarWideTable:
    name = "select_star_wide_table"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        raise NotImplementedError("S4: implement SelectStarWideTable per docs/specs/03-rules.md")
