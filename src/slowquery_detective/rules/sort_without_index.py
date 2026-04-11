"""Rule: Sort node with an ORDER BY not served by an index.

Fires when there's a ``Sort`` node AND the canonical SQL has ``ORDER BY``
AND no ancestor ``Index Scan`` already returns pre-sorted rows.
"""

from __future__ import annotations

import re
from typing import Any

from slowquery_detective.rules.base import IDENTIFIER_RE, Suggestion, walk_nodes

_ORDER_BY_RE = re.compile(
    r"\border\s+by\s+(?:\"?(\w+)\"?\.)?\"?(\w+)\"?",
    re.IGNORECASE,
)


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
        match = _ORDER_BY_RE.search(canonical_sql)
        if not match:
            return None

        col = match.group(2)
        if not col or not IDENTIFIER_RE.match(col):
            return None

        nodes = walk_nodes(plan)
        if not nodes:
            return None

        # If the top node is an Index Scan, we're already pre-sorted and
        # the rule should abstain (spec case 8).
        top = nodes[0] if nodes else {}
        if top.get("Node Type") == "Index Scan":
            return None

        # Require at least one Sort node with meaningful cost/time.
        sort_nodes = [n for n in nodes if n.get("Node Type") == "Sort"]
        if not sort_nodes:
            return None

        has_expensive = any(
            float(n.get("Total Cost") or 0.0) > 1000.0
            or float(n.get("Actual Total Time") or 0.0) > 100.0
            for n in sort_nodes
        )
        if not has_expensive:
            return None

        # Find the most plausible target table: first Seq Scan under the Sort.
        table: str | None = None
        for n in nodes:
            if n.get("Node Type") in ("Seq Scan", "Bitmap Heap Scan"):
                candidate = n.get("Relation Name")
                if isinstance(candidate, str) and IDENTIFIER_RE.match(candidate):
                    table = candidate
                    break

        if table is None:
            return None

        sql = f"CREATE INDEX IF NOT EXISTS ix_{table}_{col} ON {table}({col});"
        return Suggestion(
            kind="index",
            sql=sql,
            rationale=(
                f"Sort node on ORDER BY {col} with significant cost; an index "
                f"ending with {col} would let the planner skip the sort step."
            ),
            confidence=0.8,
            source="rules",
            rule_name=self.name,
        )
