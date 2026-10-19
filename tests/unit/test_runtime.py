"""Spec 07 cases 5-9, 13, 21, 22 — the runtime between hooks, worker and store."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from datetime import datetime

import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import slowquery_detective.hooks as hooks_mod
from slowquery_detective import InMemoryStoreWriter, install
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.explain import ExplainJob, ExplainWorker
from slowquery_detective.hooks import Observation
from slowquery_detective.recorder import Batch, Recorder
from slowquery_detective.rules import run_rules
from slowquery_detective.runtime import Runtime

UNREACHABLE = "postgresql+asyncpg://u:p@127.0.0.1:1/db"
FP = "0123456789abcdef"


@pytest.fixture()
def engine() -> Iterator[AsyncEngine]:
    eng = create_async_engine(UNREACHABLE)
    yield eng
    hooks_mod.detach(eng)


class CountingStore(InMemoryStoreWriter):
    def __init__(self) -> None:
        super().__init__()
        self.sweeps: list[datetime] = []
        self.writes = 0

    async def sweep(self, service: str, cutoff: datetime) -> None:
        self.sweeps.append(cutoff)
        await super().sweep(service, cutoff)

    async def write_batch(self, service: str, batch: Batch) -> None:
        self.writes += 1
        await super().write_batch(service, batch)


def _runtime(engine: AsyncEngine, store: InMemoryStoreWriter | None = None, **kw: int) -> Runtime:
    app = FastAPI()
    install(app, engine, service="magpie", store=store or InMemoryStoreWriter(), **kw)
    runtime: Runtime = app.state.slowquery_runtime
    return runtime


def _obs(ms: float, *, rid: str | None = None, executemany: bool = False) -> Observation:
    return Observation(
        fingerprint_id=FP,
        canonical_sql="select * from t where id = ?",
        duration_ms=ms,
        statement="SELECT * FROM t WHERE id = $1",
        parameters=(5,),
        executemany=executemany,
        request_id=rid,
    )


# ---------------------------------------------------------------------------
# Threshold gating of EXPLAIN jobs (case 8)
# ---------------------------------------------------------------------------


def test_08_below_threshold_records_but_submits_nothing(engine: AsyncEngine) -> None:
    rt = _runtime(engine, threshold_ms=100)
    rt.observe(_obs(99.9, rid="R"))
    assert rt.worker.queued == 0
    batch = rt.recorder.drain()
    assert batch.fingerprints[FP].calls == 1
    assert batch.requests[("R", FP)].calls == 1
    assert batch.samples == ()


def test_08_at_threshold_submits_a_job_with_the_real_statement(engine: AsyncEngine) -> None:
    rt = _runtime(engine, threshold_ms=100)
    submitted: list[ExplainJob] = []
    real_submit = rt.worker.submit

    def _spy(job: ExplainJob) -> bool:
        submitted.append(job)
        return real_submit(job)

    rt.worker.submit = _spy  # type: ignore[method-assign]
    for _ in range(3):
        rt.buffer.record(FP, 1.0)
    rt.observe(_obs(100.0, rid="R"))
    assert rt.worker.queued == 1
    (job,) = submitted
    assert job.statement == "SELECT * FROM t WHERE id = $1"
    assert job.parameters == (5,)
    assert job.recent_call_count == 3
    assert len(rt.recorder.drain().samples) == 1


def test_08_executemany_is_sampled_but_never_explained(engine: AsyncEngine) -> None:
    rt = _runtime(engine, threshold_ms=10)
    rt.observe(_obs(500.0, executemany=True))
    assert rt.worker.queued == 0
    assert len(rt.recorder.drain().samples) == 1


def test_13_cooling_down_fingerprint_is_not_resubmitted(engine: AsyncEngine) -> None:
    rt = _runtime(engine, threshold_ms=10)
    rt.observe(_obs(500.0))
    rt.observe(_obs(500.0))
    assert rt.worker.queued == 1


def test_23_observe_never_raises(engine: AsyncEngine, caplog: pytest.LogCaptureFixture) -> None:
    rt = _runtime(engine)

    def _boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("recorder broke")

    rt.recorder.observe = _boom  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        rt.observe(_obs(1.0))
    assert any("observe_failed" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Flush: percentiles, drops (cases 6, 9)
# ---------------------------------------------------------------------------


async def test_06_flush_attaches_rolling_percentiles(engine: AsyncEngine) -> None:
    store = InMemoryStoreWriter()
    rt = _runtime(engine, store)
    await store.start()
    for ms in (10.0, 20.0, 30.0):
        rt.buffer.record(FP, ms)
        rt.observe(_obs(ms))
    await rt.flush()
    (row,) = await store.list_queries("magpie", since=None, limit=5)
    assert row.call_count == 3
    assert row.p50_ms == 20.0
    assert row.max_ms == 30.0


async def test_09_drops_are_logged_at_warning(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    rt = _runtime(engine)
    for i in range(10_001):
        rt.recorder.observe(f"{i:016x}", "select 1", 1.0, None)
    with caplog.at_level(logging.WARNING, logger="slowquery.runtime"):
        await rt.flush()
    assert any("dropped" in r.getMessage() for r in caplog.records)


async def test_flush_with_nothing_pending_does_not_touch_the_store(engine: AsyncEngine) -> None:
    store = CountingStore()
    rt = _runtime(engine, store)
    await rt.start()
    await rt.flush()
    await rt.flush()
    assert store.writes == 0
    await rt.stop()


# ---------------------------------------------------------------------------
# Sweep scheduling (case 21)
# ---------------------------------------------------------------------------


async def test_21_sweep_at_start_then_only_after_activity_and_at_most_hourly(
    engine: AsyncEngine,
) -> None:
    clock = [1_790_000_000.0]
    store = CountingStore()
    rt = Runtime(
        service="magpie",
        store=store,
        recorder=Recorder(threshold_ms=100.0, clock=lambda: clock[0]),
        buffer=RingBuffer(),
        worker=ExplainWorker(engine=engine, store=store, rules=run_rules, explainer=None),
        retention_s=3_600,
        clock=lambda: clock[0],
    )
    await rt.start()
    assert len(store.sweeps) == 1
    assert store.sweeps[0].timestamp() == clock[0] - 3_600

    clock[0] += 7_200
    await rt.flush()  # idle: no write, no sweep
    assert len(store.sweeps) == 1

    rt.observe(_obs(1.0))
    await rt.flush()  # activity after more than an hour: sweep
    assert len(store.sweeps) == 2

    clock[0] += 60
    rt.observe(_obs(1.0))
    await rt.flush()  # activity, but within the hour: no sweep
    assert len(store.sweeps) == 2
    await rt.stop()


async def test_21_sweep_failure_is_logged_not_fatal(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    class BadSweep(InMemoryStoreWriter):
        async def sweep(self, service: str, cutoff: datetime) -> None:
            raise RuntimeError("sweep failed")

    rt = _runtime(engine, BadSweep())
    with caplog.at_level(logging.ERROR, logger="slowquery.runtime"):
        await rt.start()
    assert any("sweep_failed" in r.getMessage() for r in caplog.records)
    assert rt.ready
    await rt.stop()


# ---------------------------------------------------------------------------
# The periodic flush loop (case 27)
# ---------------------------------------------------------------------------


def _direct_runtime(engine: AsyncEngine, store: InMemoryStoreWriter, **kw: float) -> Runtime:
    return Runtime(
        service="magpie",
        store=store,
        recorder=Recorder(threshold_ms=100.0),
        buffer=RingBuffer(),
        worker=ExplainWorker(engine=engine, store=store, rules=run_rules, explainer=None),
        retention_s=3_600,
        **kw,
    )


async def test_27_flush_loop_writes_without_a_read(engine: AsyncEngine) -> None:
    store = InMemoryStoreWriter()
    rt = _direct_runtime(engine, store, flush_interval_s=0.01)
    await rt.start()
    try:
        rt.observe(_obs(1.0, rid="R"))
        for _ in range(100):
            if await store.list_request_queries("magpie", "R", since=None, limit=5):
                break
            await asyncio.sleep(0.01)
        rows = await store.list_request_queries("magpie", "R", since=None, limit=5)
    finally:
        await rt.stop()
    assert [r.call_count for r in rows] == [1]


async def test_27_flush_loop_survives_an_error(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    store = InMemoryStoreWriter()
    rt = _direct_runtime(engine, store, flush_interval_s=0.01)
    real_flush = rt.flush
    calls = {"n": 0}

    async def _flaky() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        await real_flush()

    rt.flush = _flaky  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="slowquery.runtime"):
        await rt.start()
        for _ in range(100):
            if calls["n"] >= 3:
                break
            await asyncio.sleep(0.01)
        await rt.stop()
    assert calls["n"] >= 3
    assert any("flush_loop_error" in r.getMessage() for r in caplog.records)


async def test_27_stop_survives_failing_final_flush_and_on_stop(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    async def _bad_on_stop() -> None:
        raise RuntimeError("detach failed")

    rt = _direct_runtime(engine, InMemoryStoreWriter(), on_stop=_bad_on_stop)  # type: ignore[arg-type]

    async def _bad_flush() -> None:
        raise RuntimeError("final flush failed")

    await rt.start()
    rt.flush = _bad_flush  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="slowquery.runtime"):
        await rt.stop()
    messages = {r.getMessage() for r in caplog.records}
    assert {"slowquery.runtime.final_flush_failed", "slowquery.runtime.on_stop_failed"} <= messages
