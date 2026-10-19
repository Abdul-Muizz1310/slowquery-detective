"""Specs 06 + 07 — the EXPLAIN worker against a real Postgres.

Spec 07 cases 28-30 (real parameters, never executed, never stored) and
spec 06 cases 33-34 (performance, ``@slow``).
"""

from __future__ import annotations

import asyncio
import json
import statistics
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from slowquery_detective.explain import ExplainJob, ExplainWorker
from slowquery_detective.rules import run_rules
from slowquery_detective.store import InMemoryStoreWriter

pytestmark = pytest.mark.integration

SECRET = "sk-live-integration-7e3f"
ACCOUNT = uuid.UUID("0b8f6d3a-2c4e-4f5a-9b7c-1d2e3f4a5b6c")


@pytest.fixture()
async def engine(pg: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    url = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")
    eng = create_async_engine(url)
    async with eng.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS w_users CASCADE"))
        await conn.execute(text("DROP SEQUENCE IF EXISTS w_seq"))
        await conn.execute(text("CREATE TABLE w_users (id uuid primary key, email text, n int)"))
        await conn.execute(
            text("INSERT INTO w_users VALUES (:id, :email, 1)"), {"id": ACCOUNT, "email": SECRET}
        )
        # nextval() survives a rollback, so an executed EXPLAIN target would show.
        await conn.execute(text("CREATE SEQUENCE w_seq"))
        await conn.execute(
            text(
                "CREATE OR REPLACE FUNCTION w_bump() RETURNS bigint LANGUAGE sql VOLATILE AS "
                "$$ SELECT nextval('w_seq') $$"
            )
        )
    yield eng
    await eng.dispose()


class PlanStore(InMemoryStoreWriter):
    def __init__(self) -> None:
        super().__init__()
        self.plans: dict[str, Any] = {}

    async def upsert_plan(
        self, fingerprint_id: str, plan_json: Any, cost: float | None, captured_at: datetime
    ) -> None:
        self.plans[fingerprint_id] = plan_json
        await super().upsert_plan(fingerprint_id, plan_json, cost, captured_at)


def job(fid: str, canonical: str, statement: str, parameters: Any) -> ExplainJob:
    return ExplainJob(
        fingerprint_id=fid,
        canonical_sql=canonical,
        statement=statement,
        parameters=parameters,
        observed_ms=500.0,
        recent_call_count=1,
        enqueued_at=0.0,
    )


def worker(engine: AsyncEngine, store: InMemoryStoreWriter, **kw: Any) -> ExplainWorker:
    return ExplainWorker(engine=engine, store=store, rules=run_rules, explainer=None, **kw)


async def test_07_28_explicitly_cast_uuid_parameter_is_explained(engine: AsyncEngine) -> None:
    store = PlanStore()
    w = worker(engine, store)
    w.submit(
        job(
            "1" * 16,
            "select email from w_users where id = cast(? as uuid)",
            "SELECT email FROM w_users WHERE id = $1::UUID",
            (ACCOUNT,),
        )
    )
    await w.process_pending()
    plan = store.plans["1" * 16]
    assert plan["Plan"]["Node Type"] in {"Index Scan", "Seq Scan", "Bitmap Heap Scan"}


async def test_07_28_inferred_parameter_types_are_explained(engine: AsyncEngine) -> None:
    store = PlanStore()
    w = worker(engine, store)
    w.submit(
        job(
            "2" * 16,
            "select id from w_users where email = ? and n > ?",
            "SELECT id FROM w_users WHERE email = $1 AND n > $2",
            (SECRET, 0),
        )
    )
    await w.process_pending()
    plan_text = json.dumps(store.plans["2" * 16])
    assert "w_users" in plan_text
    # 07-30: the parameter shaped the plan but was scrubbed before storage.
    assert SECRET not in plan_text
    assert "?::text" in plan_text


async def test_07_29_explain_does_not_execute_the_statement(engine: AsyncEngine) -> None:
    store = PlanStore()
    w = worker(engine, store)
    w.submit(job("3" * 16, "select w_bump()", "SELECT w_bump()", ()))
    await w.process_pending()
    assert "3" * 16 in store.plans
    async with engine.connect() as conn:
        called = (await conn.execute(text("SELECT is_called FROM w_seq"))).scalar_one()
    assert called is False


async def test_07_29_writes_never_reach_explain(engine: AsyncEngine) -> None:
    store = PlanStore()
    w = worker(engine, store)
    w.submit(
        job(
            "4" * 16,
            "update w_users set n = ? where id = ?",
            "UPDATE w_users SET n = $1 WHERE id = $2",
            (99, ACCOUNT),
        )
    )
    await w.process_pending()
    assert store.plans == {}
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT n FROM w_users"))).scalar_one() == 1


async def test_bad_parameters_store_nothing_and_cool_down(engine: AsyncEngine) -> None:
    store = PlanStore()
    w = worker(engine, store)
    w.submit(
        job(
            "5" * 16,
            "select email from w_users where id = cast(? as uuid)",
            "SELECT email FROM w_users WHERE id = $1::UUID",
            ("not-a-uuid",),
        )
    )
    await w.process_pending()
    assert store.plans == {}
    assert not w.accepts("5" * 16)


# ---------------------------------------------------------------------------
# Performance (06: 33-34)
# ---------------------------------------------------------------------------


@pytest.mark.slow
async def test_33_submit_overhead_under_10us(engine: AsyncEngine) -> None:
    """Spec 06 case 33: with the worker running and the queue not full,
    ``submit`` costs at most 10 µs. Jobs are built before the timed loop and
    every job has its own fingerprint, so each call takes the enqueue path
    (a repeated fingerprint would take the cheaper already-queued path)."""
    rounds, batch = 10, 1_000
    w = worker(engine, InMemoryStoreWriter(), max_queue_size=rounds * batch)
    jobs = [job(f"{i:016x}", "select 1", "SELECT 1", ()) for i in range(rounds * batch)]
    per_call: list[float] = []
    for r in range(rounds):
        chunk = jobs[r * batch : (r + 1) * batch]
        t0 = time.perf_counter()
        for j in chunk:
            w.submit(j)
        per_call.append((time.perf_counter() - t0) / batch)
    assert w.dropped_jobs == 0
    assert w.queued == rounds * batch
    median = statistics.median(per_call)
    assert median < 10e-6, f"submit overhead {median * 1e6:.1f}µs (budget 10µs)"


@pytest.mark.slow
async def test_34_burst_completes_under_budget(engine: AsyncEngine) -> None:
    w = worker(engine, InMemoryStoreWriter())
    await w.start()
    t0 = time.perf_counter()
    for i in range(100):
        w.submit(job(f"{i:016x}", "select 1", "SELECT 1", ()))
    while w.queued and time.perf_counter() - t0 < 10:
        await asyncio.sleep(0.05)
    elapsed = time.perf_counter() - t0
    await w.stop()
    assert w.queued == 0
    assert elapsed < 5.0
