"""Rule: Seq Scan on a large table with a WHERE predicate.

Fires when any ``Seq Scan`` node reports ``Plan Rows > 10_000`` AND the
canonical SQL contains a WHERE clause. Picks the highest-cost seq scan in
the plan. Emits ``CREATE INDEX IF NOT EXISTS ix_<table>_<col> ON ...``.
"""

from __future__ import annotations

import re
from typing import Any

from slowquery_detective.rules.base import (
    IDENTIFIER_RE,
    Suggestion,
    coerce_int,
    quote_if_reserved,
    walk_nodes,
)

# Capture the first column in a canonical WHERE clause. Literals are
# already ``?`` by the time fingerprint.py is done, so this regex sees
# things like ``where user_id = ?`` regardless of the original literal.
_WHERE_COL_RE = re.compile(
    r"\bwhere\s+(?:\"?(\w+)\"?\.)?\"?(\w+)\"?\s*(?:=|>|<|>=|<=|!=|<>|in|like)",
    re.IGNORECASE,
)

ROW_THRESHOLD = 10_000


class SeqScanLargeTable:
    name = "seq_scan_large_table"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        # Must have a WHERE clause. Spec case 3.
        match = _WHERE_COL_RE.search(canonical_sql)
        if not match:
            return None

        col = match.group(2)
        if not col or not IDENTIFIER_RE.match(col):
            return None

        # Find the highest-cost seq scan with enough rows.
        best: dict[str, Any] | None = None
        best_cost = -1.0
        for node in walk_nodes(plan):
            if node.get("Node Type") != "Seq Scan":
                continue
            if coerce_int(node.get("Plan Rows")) <= ROW_THRESHOLD:
                continue
            cost = float(node.get("Total Cost") or 0.0)
            if cost > best_cost:
                best = node
                best_cost = cost

        if best is None:
            return None

        table = best.get("Relation Name")
        if not isinstance(table, str) or not IDENTIFIER_RE.match(table):
            return None

        rows = coerce_int(best.get("Plan Rows"))
        quoted_table = quote_if_reserved(table)
        sql = f"CREATE INDEX IF NOT EXISTS ix_{table}_{col} ON {quoted_table}({col});"
        return Suggestion(
            kind="index",
            sql=sql,
            rationale=(f"Seq Scan on {table} with WHERE {col}; estimated {rows:,} rows."),
            confidence=0.9,
            source="rules",
            rule_name=self.name,
        )
