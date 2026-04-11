"""Async store writer — see ``docs/specs/05-middleware.md`` + data model.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Suggestion


class StoreWriter:
    """Persists fingerprints, plans, and suggestions to the configured store."""

    def __init__(self, store_url: str) -> None:
        self._store_url = store_url

    async def upsert_fingerprint(
        self,
        fingerprint_id: str,
        canonical_sql: str,
    ) -> None:
        raise NotImplementedError("S4: implement StoreWriter.upsert_fingerprint")

    async def record_sample(
        self,
        fingerprint_id: str,
        duration_ms: float,
        rows: int | None = None,
    ) -> None:
        raise NotImplementedError("S4: implement StoreWriter.record_sample")

    async def upsert_plan(
        self,
        fingerprint_id: str,
        plan_json: dict[str, Any],
        plan_text: str,
        cost: float,
    ) -> None:
        raise NotImplementedError("S4: implement StoreWriter.upsert_plan")

    async def insert_suggestions(
        self,
        fingerprint_id: str,
        suggestions: list[Suggestion],
    ) -> None:
        raise NotImplementedError("S4: implement StoreWriter.insert_suggestions")

    async def close(self) -> None:
        raise NotImplementedError("S4: implement StoreWriter.close")
