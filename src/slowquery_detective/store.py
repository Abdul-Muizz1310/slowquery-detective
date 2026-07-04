"""Async store writer — typed interface plus an in-process default.

``StoreWriter`` is the abstract async interface every method of which raises
``NotImplementedError``; it exists so callers can subclass it against a real
database. ``InMemoryStoreWriter`` is the concrete, process-local default that
``install()`` wires in when no custom store is supplied.

**Persistence scope (important).** The in-memory store — like the ring
buffer, the worker's plan cache, and the LLM cooldown map — lives entirely in
one process. In a multi-worker / multi-replica deployment (the common
production shape for FastAPI behind gunicorn/uvicorn or a load balancer) each
process sees only its own slice of traffic and computes its own percentiles.
For cross-process aggregation, subclass ``StoreWriter`` against a shared
database (e.g. the asyncpg-backed implementation in ``slowquery-demo-backend``
Phase 4b) and pass it via ``install(app, engine, store=...)``. This limitation
is documented prominently in the README so single-process demos aren't
mistaken for a horizontally-scalable service.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from slowquery_detective.rules.base import Suggestion


class StoreWriter:
    """Persists fingerprints, plans, and suggestions to the configured store.

    Abstract base: every method raises ``NotImplementedError``. Subclass it
    for a real backing store, or use :class:`InMemoryStoreWriter` for a
    single-process default.
    """

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


@dataclass
class _StoredPlan:
    plan_json: dict[str, Any]
    plan_text: str
    cost: float


class InMemoryStoreWriter(StoreWriter):
    """Process-local store used as the default when none is injected.

    Concrete so the default ``install()`` path actually persists (rather than
    raising ``NotImplementedError`` from the worker on every job). State is
    per-process only — see the module docstring for the multi-worker caveat.
    """

    def __init__(self, store_url: str = "") -> None:
        super().__init__(store_url)
        self.fingerprints: dict[str, str] = {}
        self.plans: dict[str, _StoredPlan] = {}
        self.suggestions: dict[str, list[Suggestion]] = {}

    async def upsert_fingerprint(self, fingerprint_id: str, canonical_sql: str) -> None:
        self.fingerprints[fingerprint_id] = canonical_sql

    async def record_sample(
        self,
        fingerprint_id: str,
        duration_ms: float,
        rows: int | None = None,
    ) -> None:
        # Sample-level history isn't retained in the in-memory store; the ring
        # buffer owns live percentiles. Kept as a no-op for interface parity.
        return None

    async def upsert_plan(
        self,
        fingerprint_id: str,
        plan_json: dict[str, Any],
        plan_text: str,
        cost: float,
    ) -> None:
        self.plans[fingerprint_id] = _StoredPlan(plan_json, plan_text, cost)

    async def insert_suggestions(
        self,
        fingerprint_id: str,
        suggestions: list[Suggestion],
    ) -> None:
        self.suggestions.setdefault(fingerprint_id, []).extend(suggestions)

    async def close(self) -> None:
        return None
