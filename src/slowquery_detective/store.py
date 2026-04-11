"""Async store writer — typed interface with no concrete implementation.

The package ships with ``StoreWriter`` as a thin async interface that raises
``NotImplementedError`` from every method. Two paths to a working store:

1. Use the concrete implementation in ``slowquery-demo-backend`` (Phase 4b),
   which ships Alembic migrations for the four tables defined in
   ``docs/projects/50-slowquery-detective.md`` and wires an asyncpg-backed
   subclass into ``install(app, engine, store_url=...)``.
2. Subclass ``StoreWriter`` against your own database. Every method is
   async and typed — override the four public hooks and pass your instance
   into ``install()``.

The base class is intentionally abstract so ``slowquery-detective`` as a
library has no hard dependency on a particular schema.
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
