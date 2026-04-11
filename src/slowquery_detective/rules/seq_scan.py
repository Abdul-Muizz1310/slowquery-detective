"""Rule: Seq Scan on a large table with a WHERE predicate."""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion


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
        raise NotImplementedError("S4: implement SeqScanLargeTable per docs/specs/03-rules.md")
