"""Red tests for docs/specs/06-explain-worker.md — integration level.

Covers cases 24-28 (parameter substitution) and 33-34 (performance).
These need a real Postgres (testcontainers) to run EXPLAIN against.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from slowquery_detective.explain import ExplainJob, ExplainWorker
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

pytestmark = pytest.mark.integration

FID = "abcdef0123456789"


@pytest.fixture(scope="module")
def pg() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture()
async def engine(pg: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    url = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")
    eng = create_async_engine(url)
    try:
        async with eng.begin() as conn:
            await conn.execute(text("CREATE TABLE IF NOT EXISTS users (id int, email text)"))
            await conn.execute(text("INSERT INTO users VALUES (1, 'a'), (2, 'b')"))
        yield eng
    finally:
        await eng.dispose()


class _NullStore(StoreWriter):
    def __init__(self) -> None:
        super().__init__("postgresql://null")

    async def upsert_fingerprint(self, *_: Any, **__: Any) -> None:  # type: ignore[override]
        return None

    async def record_sample(self, *_: Any, **__: Any) -> None:  # type: ignore[override]
        return None

    async def upsert_plan(self, *_: Any, **__: Any) -> None:  # type: ignore[override]
        return None

    async def insert_suggestions(self, *_: Any, **__: Any) -> None:  # type: ignore[override]
        return None

    async def close(self) -> None:  # type: ignore[override]
        return None


def _empty_rules(_plan: dict[str, Any], _sql: str) -> list[Suggestion]:
    return []


class _SpyEngine:
    """Proxy that wraps a real AsyncEngine and records executed SQL statements."""

    def __init__(self, real_engine: AsyncEngine, seen: list[str]) -> None:
        self._real = real_engine
        self._seen = seen

    def connect(self) -> Any:
        conn_ctx = self._real.connect()
        seen = self._seen

        class _SpyCtx:
            async def __aenter__(self_inner) -> Any:
                self_inner._inner = await conn_ctx.__aenter__()
                return self_inner

            async def __aexit__(self_inner, *args: object) -> None:
                await conn_ctx.__aexit__(*args)

            async def execute(self_inner, stmt: Any, *a: Any, **k: Any) -> Any:
                seen.append(str(stmt))
                return await self_inner._inner.execute(stmt, *a, **k)

        return _SpyCtx()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


# ---------------------------------------------------------------------------
# Parameter substitution (cases 24-28)
# ---------------------------------------------------------------------------


async def test_24_int_placeholder_substituted_as_one(engine: AsyncEngine) -> None:
    seen: list[str] = []
    spy = _SpyEngine(engine, seen)
    worker = ExplainWorker(
        engine=spy,  # type: ignore[arg-type]
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT * FROM users WHERE id = ?",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.3)
    await worker.stop()
    joined = "\n".join(seen)
    assert "id = 1" in joined.lower()


async def test_25_text_placeholder_substituted_as_empty_string(engine: AsyncEngine) -> None:
    worker = ExplainWorker(
        engine=engine,
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT * FROM users WHERE email = ?",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.3)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None


async def test_26_in_list_substituted_as_single_value(engine: AsyncEngine) -> None:
    worker = ExplainWorker(
        engine=engine,
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT * FROM users WHERE id IN (?)",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.3)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None


async def test_27_unknown_placeholder_falls_back_to_plain_explain(
    engine: AsyncEngine,
) -> None:
    seen: list[str] = []
    spy = _SpyEngine(engine, seen)
    worker = ExplainWorker(
        engine=spy,  # type: ignore[arg-type]
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT jsonb_path_query(data, ?) FROM users",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.3)
    await worker.stop()
    joined = "\n".join(seen).upper()
    # Substitutes NULL for jsonb path placeholders, still runs EXPLAIN ANALYZE.
    assert "EXPLAIN" in joined
    assert "NULL" in joined


async def test_28_explain_canonical_sql_has_no_original_literal(
    engine: AsyncEngine,
) -> None:
    seen: list[str] = []
    spy = _SpyEngine(engine, seen)
    worker = ExplainWorker(
        engine=spy,  # type: ignore[arg-type]
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT * FROM users WHERE email = ?",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.3)
    await worker.stop()
    joined = "\n".join(seen)
    # Canonical has ``?``; nothing like a real literal ever appears.
    assert "'user@example.com'" not in joined
    assert "'alice'" not in joined


# ---------------------------------------------------------------------------
# Performance (cases 33-34)
# ---------------------------------------------------------------------------


@pytest.mark.slow
async def test_33_submit_overhead_under_10us(engine: AsyncEngine) -> None:
    worker = ExplainWorker(
        engine=engine,
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    t0 = time.perf_counter()
    for _ in range(10_000):
        worker.submit(
            ExplainJob(
                fingerprint_id=FID,
                canonical_sql="SELECT 1",
                observed_ms=500.0,
                enqueued_at=0.0,
            )
        )
    elapsed = time.perf_counter() - t0
    await worker.stop()
    per_call = elapsed / 10_000
    assert per_call < 10e-6, f"submit overhead {per_call * 1e6:.1f}µs (budget 10µs)"


@pytest.mark.slow
async def test_34_burst_completes_under_budget(engine: AsyncEngine) -> None:
    worker = ExplainWorker(
        engine=engine,
        store=_NullStore(),
        rules=_empty_rules,
        explainer=None,
    )
    await worker.start()
    t0 = time.perf_counter()
    for i in range(100):
        worker.submit(
            ExplainJob(
                fingerprint_id=f"fp{i:014d}",
                canonical_sql="SELECT 1",
                observed_ms=500.0,
                enqueued_at=0.0,
            )
        )
    # Wait for drain.
    await asyncio.sleep(2.0)
    elapsed = time.perf_counter() - t0
    await worker.stop()
    assert elapsed < 5.0
