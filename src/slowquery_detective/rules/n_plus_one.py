"""Rule: N+1 suspicion based on recent call count on the same fingerprint.

Fires when ``recent_call_count >= 50``. Does not touch the plan at all —
it's a heuristic on call rate, fed by the ring buffer / middleware.
"""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion

CALL_COUNT_THRESHOLD = 50


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
        if recent_call_count < CALL_COUNT_THRESHOLD:
            return None

        return Suggestion(
            kind="rewrite",
            sql=None,
            rationale=(
                f"Fingerprint called {recent_call_count} times in the recent "
                f"window, which is strongly suggestive of an N+1 access pattern. "
                f"Consider eager-loading via selectinload / joinedload or a "
                f"single JOIN at the query layer."
            ),
            confidence=0.65,
            source="rules",
            rule_name=self.name,
        )
