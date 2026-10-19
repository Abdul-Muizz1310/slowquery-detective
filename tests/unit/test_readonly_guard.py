"""Regression tests for the EXPLAIN read-only guard.

Audit CRITICAL: "EXPLAIN ANALYZE runs against captured INSERT/UPDATE/DELETE
statements with fabricated parameter values." EXPLAIN (ANALYZE) *executes*
its target, so the worker must refuse to run EXPLAIN on anything that is not
a provably read-only statement. These tests exercise both the pure guard and
the composed worker path with a spying engine so no statement reaches the DB.
"""

from __future__ import annotations

from typing import Any

import pytest

from slowquery_detective.explain import ExplainJob, ExplainWorker, is_read_only_sql
from slowquery_detective.store import InMemoryStoreWriter

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
# Composed worker path — a spying engine proves nothing reaches the database.
# ---------------------------------------------------------------------------


class _SpyEngine:
    """Records every statement the worker sends to the database."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    def connect(self) -> Any:
        seen = self.seen

        class _Conn:
            async def __aenter__(self) -> _Conn:
                return self

            async def __aexit__(self, *_: object) -> None:
                return None

            async def exec_driver_sql(self, stmt: str, _params: Any = None) -> Any:
                seen.append(stmt)

                class _R:
                    def scalar_one(self) -> list[dict[str, Any]]:
                        return [{"Plan": {"Node Type": "Seq Scan"}}]

                return _R()

        return _Conn()


def _no_rules(*_a: Any, **_k: Any) -> list[Any]:
    return []


def _job(fid: str, canonical: str) -> ExplainJob:
    return ExplainJob(
        fingerprint_id=fid,
        canonical_sql=canonical,
        statement=canonical.replace("?", "$1"),
        parameters=(1,),
        observed_ms=500.0,
        recent_call_count=1,
        enqueued_at=0.0,
    )


async def test_worker_never_explains_writes() -> None:
    engine = _SpyEngine()
    worker = ExplainWorker(
        engine=engine,
        store=InMemoryStoreWriter(),
        rules=_no_rules,
        explainer=None,
        now=lambda: 0.0,
    )
    writes = [
        "UPDATE users SET active = ? WHERE id = ?",
        "INSERT INTO users (name) VALUES (?)",
        "DELETE FROM users WHERE id = ?",
        "WITH x AS (INSERT INTO t VALUES (?) RETURNING id) SELECT * FROM x",
        "DO $$ BEGIN PERFORM 1; END $$",
        "CALL do_thing(?)",
    ]
    for i, sql in enumerate(writes):
        assert worker.submit(_job(f"{i:016x}", sql)) is True
    assert await worker.process_pending() == len(writes)

    # The engine must never have been asked to run anything.
    assert engine.seen == []
    # A refused statement cools down instead of being re-parsed in a loop.
    assert all(not worker.accepts(f"{i:016x}") for i in range(len(writes)))


async def test_worker_still_explains_selects() -> None:
    engine = _SpyEngine()
    worker = ExplainWorker(
        engine=engine,
        store=InMemoryStoreWriter(),
        rules=_no_rules,
        explainer=None,
        now=lambda: 0.0,
    )
    worker.submit(_job(FID, "SELECT * FROM orders WHERE user_id = ?"))
    await worker.process_pending()
    assert engine.seen == ["EXPLAIN (FORMAT JSON) SELECT * FROM orders WHERE user_id = $1"]
