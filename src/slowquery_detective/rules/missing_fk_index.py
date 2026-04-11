"""Rule: FK column used in join without an index.

Fires when a Nested Loop / Hash Join has an inner ``Seq Scan`` on a column
whose name ends in ``_id``. Skips when the inner side is already an
``Index Scan``.
"""

from __future__ import annotations

import re
from typing import Any

from slowquery_detective.rules.base import IDENTIFIER_RE, Suggestion, walk_nodes

# Extract ``<table>.<col>`` or just ``<col>`` from a Postgres ``Filter`` /
# ``Join Filter`` clause string. We only care about the bare column on the
# left side of an equality.
_FILTER_FK_RE = re.compile(
    r"\(?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*",
)


class MissingFkIndex:
    name = "missing_fk_index"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        nodes = walk_nodes(plan)
        if not nodes:
            return None

        # Walk each join node and inspect its immediate children.
        for node in nodes:
            if node.get("Node Type") not in ("Nested Loop", "Hash Join", "Merge Join"):
                continue

            children = node.get("Plans") or []
            if not isinstance(children, list) or len(children) < 2:
                continue

            inner = children[1]
            if not isinstance(inner, dict):
                continue
            if inner.get("Node Type") != "Seq Scan":
                continue

            table = inner.get("Relation Name")
            if not isinstance(table, str) or not IDENTIFIER_RE.match(table):
                continue

            # Find an FK column: prefer the inner node's ``Filter`` clause;
            # fall back to scanning the canonical SQL for a ``_id`` mention.
            filter_text = str(inner.get("Filter") or "")
            col: str | None = None
            match = _FILTER_FK_RE.search(filter_text)
            if match:
                candidate = match.group(1)
                if candidate.endswith("_id") and IDENTIFIER_RE.match(candidate):
                    col = candidate

            if col is None:
                # Canonical SQL scan: first ``<name>_id`` token.
                for word in re.findall(r"\b([a-z_][a-z0-9_]*_id)\b", canonical_sql.lower()):
                    if IDENTIFIER_RE.match(word):
                        col = word
                        break

            if col is None or not IDENTIFIER_RE.match(col):
                continue

            sql = f"CREATE INDEX IF NOT EXISTS ix_{table}_{col} ON {table}({col});"
            return Suggestion(
                kind="index",
                sql=sql,
                rationale=(
                    f"Join between outer and inner {table} performed a Seq Scan "
                    f"on the join key {col}; an index on ({col}) would let the "
                    f"planner use an Index Scan."
                ),
                confidence=0.92,
                source="rules",
                rule_name=self.name,
            )

        return None
