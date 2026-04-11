"""Rule: N+1 suspicion based on recent call count on the same fingerprint."""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion


class NPlusOneSuspicion:
    name = "n_plus_one_suspicion"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        raise NotImplementedError("S4: implement NPlusOneSuspicion per docs/specs/03-rules.md")
