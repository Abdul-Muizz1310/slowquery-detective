"""Rule: WHERE clause wraps a column in a function (LOWER, DATE, ...).

Fires on ``WHERE <fn>(<col>) = ?`` patterns. Does not fire on
``WHERE <col> = <fn>(?)`` (function applied to the parameter, not the
column) per spec case 10.
"""

from __future__ import annotations

import re
from typing import Any

from slowquery_detective.rules.base import IDENTIFIER_RE, Suggestion, walk_nodes

# Match ``WHERE <fn>(<col>) <op> ?`` but NOT ``WHERE <col> <op> <fn>(?)``.
# The function name is captured group 1, column is group 2.
_FUNC_IN_WHERE_RE = re.compile(
    r"""
    \bwhere\s+
    ([A-Za-z_][A-Za-z0-9_]*)      # function name
    \s*\(\s*
    (?:\"?(\w+)\"?\.)?            # optional table qualifier
    \"?(\w+)\"?                   # column name
    \s*\)\s*
    (?:=|>|<|>=|<=|!=|<>|like)
    """,
    re.IGNORECASE | re.VERBOSE,
)


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
        match = _FUNC_IN_WHERE_RE.search(canonical_sql)
        if not match:
            return None

        fn = match.group(1).upper()
        col = match.group(3)
        if not col or not IDENTIFIER_RE.match(col):
            return None

        # Table comes from the plan's top node (first Seq Scan / Index Scan).
        table: str | None = None
        for node in walk_nodes(plan):
            candidate = node.get("Relation Name")
            if isinstance(candidate, str) and IDENTIFIER_RE.match(candidate):
                table = candidate
                break

        if table is None:
            return None

        sql = f"CREATE INDEX IF NOT EXISTS ix_{table}_{col}_{fn.lower()} ON {table}({fn}({col}));"
        return Suggestion(
            kind="index",
            sql=sql,
            rationale=(
                f"WHERE {fn}({col}) prevents the planner from using any plain "
                f"index on {col}. A functional index on {fn}({col}) restores "
                f"lookup performance."
            ),
            confidence=0.85,
            source="rules",
            rule_name=self.name,
        )
