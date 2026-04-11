"""Rule: SELECT * on a wide table.

Fires when canonical SQL contains ``SELECT *`` AND the plan's top node
reports more than 20 columns in ``Output``. Does not emit DDL — the
suggestion is a plain rewrite note.
"""

from __future__ import annotations

import re
from typing import Any

from slowquery_detective.rules.base import Suggestion, walk_nodes

_SELECT_STAR_RE = re.compile(r"\bselect\s+\*", re.IGNORECASE)
WIDE_TABLE_COL_THRESHOLD = 20


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
        if not _SELECT_STAR_RE.search(canonical_sql):
            return None

        nodes = walk_nodes(plan)
        if not nodes:
            return None

        top = nodes[0]
        output = top.get("Output") or []
        if not isinstance(output, list) or len(output) <= WIDE_TABLE_COL_THRESHOLD:
            return None

        return Suggestion(
            kind="rewrite",
            sql=None,
            rationale=(
                f"SELECT * pulls {len(output)} columns per row. Listing only "
                f"the columns you actually read cuts row width, planner cost, "
                f"and protocol bytes."
            ),
            confidence=0.7,
            source="rules",
            rule_name=self.name,
        )
