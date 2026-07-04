"""Regression tests for the EXPLAIN read-only guard.

Audit CRITICAL: "EXPLAIN ANALYZE runs against captured INSERT/UPDATE/DELETE
statements with fabricated parameter values." EXPLAIN (ANALYZE) *executes*
its target, so the worker must refuse to run EXPLAIN on anything that is not
a provably read-only statement. These tests exercise both the pure guard and
the composed worker path with a spying engine so no statement reaches the DB.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from slowquery_detective.explain import ExplainJob, ExplainWorker, is_read_only_sql
from slowquery_detective.store import StoreWriter

FID = "abcdef0123456789"


# ---------------------------------------------------------------------------
# Pure guard — parsed, not regexed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "select * from orders where user_id = ?",
        "select 1",
        "with x as (select 1) select * from x",
        "values (1), (2)",
        "select a from t union select b from u",
        "select count(*) from t where created_at > ?",
    ],
)
def test_guard_allows_read_only(sql: str) -> None:
    assert is_read_only_sql(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "insert into users (name) values (?)",
        "update users set active = ? where id = ?",
        "delete from users where id = ?",
        "with x as (insert into t values (?) returning id) select * from x",
        "with x as (update t set a = ? returning id) select * from x",
        "with x as (delete from t returning id) select * from x",
        "merge into t using s on t.id = s.id when matched then update set a = 1",
        "do $$ begin perform 1; end $$",
        "call my_proc(?)",
        "create index if not exists ix_a on b(c)",
        "drop table users",
        "truncate users",
        "vacuum analyze",
        "not even sql ;;;",
    ],
)
def test_guard_rejects_non_read_only(sql: str) -> None:
    assert is_read_only_sql(sql) is False


# ---------------------------------------------------------------------------
# Composed worker path — nothing non-read-only ever reaches the engine.
# ---------------------------------------------------------------------------


def _mock_store() -> MagicMock:
    store = MagicMock(spec=StoreWriter)
    store.upsert_plan = AsyncMock(return_value=None)
    store.insert_suggestions = AsyncMock(return_value=None)
    store.upsert_fingerprint = AsyncMock(return_value=None)
    store.record_sample = AsyncMock(return_value=None)
    return store


class _SpyEngine:
    """Records every statement the worker tries to EXPLAIN."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    def connect(self) -> Any:
        seen = self.seen

        class _Conn:
            async def __aenter__(self) -> _Conn:
                return self

            async def __aexit__(self, *_: object) -> None:
                return None

            async def execute(self, stmt: Any) -> Any:
                seen.append(str(stmt))

                class _R:
                    def scalar_one(self) -> list[dict[str, Any]]:
                        return [{"Plan": {"Node Type": "Seq Scan"}}]

                return _R()

        return _Conn()


async def test_worker_never_explains_writes() -> None:
    engine = _SpyEngine()
    worker = ExplainWorker(
        engine=engine,
        store=_mock_store(),
        rules=lambda _p, _s: [],
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    writes = [
        "UPDATE users SET active = ? WHERE id = ?",
        "INSERT INTO users (name) VALUES (?)",
        "DELETE FROM users WHERE id = ?",
        "WITH x AS (INSERT INTO t VALUES (?) RETURNING id) SELECT * FROM x",
        "DO $$ BEGIN PERFORM 1; END $$",
        "CALL do_thing(?)",
    ]
    for i, sql in enumerate(writes):
        worker.submit(
            ExplainJob(
                fingerprint_id=f"fp{i:014d}",
                canonical_sql=sql,
                observed_ms=500.0,
                enqueued_at=0.0,
            )
        )
    await asyncio.sleep(0.2)
    await worker.stop()

    # The engine must never have been asked to execute anything.
    assert engine.seen == []
    for i in range(len(writes)):
        assert worker.plan_cache_get(f"fp{i:014d}") is None


async def test_worker_still_explains_selects() -> None:
    engine = _SpyEngine()
    worker = ExplainWorker(
        engine=engine,
        store=_mock_store(),
        rules=lambda _p, _s: [],
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT * FROM orders WHERE user_id = ?",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.2)
    await worker.stop()
    assert any("EXPLAIN" in s.upper() for s in engine.seen)
    assert worker.plan_cache_get(FID) is not None
