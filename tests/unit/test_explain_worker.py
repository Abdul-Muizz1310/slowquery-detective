"""Specs 06 + 07 — the EXPLAIN worker, unit level.

Spec 06 numbering is kept where a case survived 0.2.0; spec 07 cases 28-33
(real parameters, no re-execution, scrubbing, submit gating, threads) are
marked ``07_``. The real-database versions live in
``tests/integration/test_explain_worker.py`` and ``test_fleet_flow.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime
from types import MappingProxyType
from typing import Any

import pytest

from slowquery_detective.explain import ExplainJob, ExplainWorker
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import InMemoryStoreWriter

FID = "abcdef0123456789"
CANONICAL = "select * from orders where user_id = ?"
STATEMENT = "SELECT * FROM orders WHERE user_id = $1"
SECRET = "sk-live-0xdeadbeef"

SEQ_PLAN: dict[str, Any] = {
    "Plan": {
        "Node Type": "Seq Scan",
        "Relation Name": "orders",
        "Plan Rows": 50_000,
        "Total Cost": 1234.5,
        "Filter": f"(note = '{SECRET}'::text)",
    }
}


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeEngine:
    """Records every ``exec_driver_sql`` call; answers with a canned result."""

    def __init__(
        self,
        result: Any = None,
        *,
        delay: float = 0.0,
        error: Exception | None = None,
    ) -> None:
        self.result = [SEQ_PLAN] if result is None else result
        self.delay = delay
        self.error = error
        self.calls: list[tuple[str, Any]] = []

    def connect(self) -> FakeEngine._Conn:
        return FakeEngine._Conn(self)

    class _Conn:
        def __init__(self, engine: FakeEngine) -> None:
            self.engine = engine

        async def __aenter__(self) -> FakeEngine._Conn:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def exec_driver_sql(self, statement: str, parameters: Any = None) -> Any:
            self.engine.calls.append((statement, parameters))
            if self.engine.delay:
                await asyncio.sleep(self.engine.delay)
            if self.engine.error is not None:
                raise self.engine.error
            result = self.engine.result

            class _Result:
                def scalar_one(self) -> Any:
                    return result

            return _Result()


class SpyStore(InMemoryStoreWriter):
    def __init__(self, *, fail_plan: bool = False, fail_suggestions: bool = False) -> None:
        super().__init__()
        self.plans: list[tuple[str, dict[str, Any], float | None]] = []
        self.suggestion_calls: list[tuple[str, list[Suggestion]]] = []
        self.fail_plan = fail_plan
        self.fail_suggestions = fail_suggestions

    async def upsert_plan(
        self, fingerprint_id: str, plan_json: Any, cost: float | None, captured_at: datetime
    ) -> None:
        if self.fail_plan:
            raise RuntimeError("store down")
        self.plans.append((fingerprint_id, plan_json, cost))
        await super().upsert_plan(fingerprint_id, plan_json, cost, captured_at)

    async def insert_suggestions(self, fingerprint_id: str, suggestions: Any) -> None:
        if self.fail_suggestions:
            raise RuntimeError("store down")
        self.suggestion_calls.append((fingerprint_id, list(suggestions)))
        await super().insert_suggestions(fingerprint_id, suggestions)


def suggestion() -> Suggestion:
    return Suggestion(
        kind="index",
        sql="CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
        rationale="seq scan on 50k rows",
        confidence=0.9,
        source="rules",
        rule_name="seq_scan_large_table",
    )


class RulesSpy:
    def __init__(self, result: list[Suggestion] | None = None, error: bool = False) -> None:
        self.result = result if result is not None else [suggestion()]
        self.error = error
        self.calls: list[tuple[dict[str, Any], str, str, int]] = []

    def __call__(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> list[Suggestion]:
        self.calls.append((plan, canonical_sql, fingerprint_id, recent_call_count))
        if self.error:
            raise RuntimeError("rule crashed")
        return list(self.result)


class ExplainerSpy:
    def __init__(self, result: Suggestion | None = None, error: bool = False) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[str, dict[str, Any], str]] = []

    async def __call__(
        self, canonical_sql: str, plan_json: dict[str, Any], *, fingerprint_id: str
    ) -> Suggestion | None:
        self.calls.append((canonical_sql, plan_json, fingerprint_id))
        if self.error:
            raise RuntimeError("llm crashed")
        return self.result


def job(
    fid: str = FID,
    *,
    statement: str = STATEMENT,
    parameters: Any = (7,),
    canonical: str = CANONICAL,
    recent: int = 3,
) -> ExplainJob:
    return ExplainJob(
        fingerprint_id=fid,
        canonical_sql=canonical,
        statement=statement,
        parameters=parameters,
        observed_ms=500.0,
        recent_call_count=recent,
        enqueued_at=0.0,
    )


def worker(
    engine: FakeEngine | None = None,
    store: InMemoryStoreWriter | None = None,
    rules: Callable[..., list[Suggestion]] | None = None,
    explainer: ExplainerSpy | None = None,
    **kw: Any,
) -> ExplainWorker:
    return ExplainWorker(
        engine=engine or FakeEngine(),
        store=store or SpyStore(),
        rules=rules or RulesSpy(),
        explainer=explainer,
        **kw,
    )


# ---------------------------------------------------------------------------
# Happy path (06: 1-6; 07: 28, 30, 33)
# ---------------------------------------------------------------------------


async def test_01_job_is_explained_with_its_real_parameters_and_persisted() -> None:
    engine, store = FakeEngine(), SpyStore()
    w = worker(engine, store)
    w.submit(job())
    assert await w.process_pending() == 1
    assert engine.calls == [(f"EXPLAIN (FORMAT JSON) {STATEMENT}", (7,))]
    assert [fp for fp, _plan, _cost in store.plans] == [FID]
    assert store.plans[0][2] == 1234.5
    assert store.suggestion_calls == [(FID, [suggestion()])]


@pytest.mark.parametrize(
    ("given", "sent"),
    [
        ((1, "a"), (1, "a")),
        ([1, "a"], (1, "a")),
        ({"u": 1}, {"u": 1}),
        (MappingProxyType({"u": 1}), {"u": 1}),
        (None, None),
        ((), ()),
        ("not-a-parameter-set", None),
        (42, None),
    ],
)
async def test_07_28_parameter_shapes_are_forwarded_as_single_execution(
    given: Any, sent: Any
) -> None:
    engine = FakeEngine()
    w = worker(engine)
    w.submit(job(parameters=given))
    await w.process_pending()
    assert engine.calls[0][1] == sent
    assert "ANALYZE" not in engine.calls[0][0]


async def test_03_07_30_stored_plan_is_scrubbed() -> None:
    store = SpyStore()
    w = worker(store=store)
    w.submit(job(parameters=(SECRET,)))
    await w.process_pending()
    stored = store.plans[0][1]
    assert SECRET not in json.dumps(stored)
    assert stored["Plan"]["Filter"] == "(note = ?::text)"
    assert stored["Plan"]["Relation Name"] == "orders"


async def test_07_33_rules_see_the_unscrubbed_plan_with_fingerprint_and_call_count() -> None:
    rules = RulesSpy()
    w = worker(rules=rules)
    w.submit(job(recent=60))
    await w.process_pending()
    ((plan, sql, fid, recent),) = rules.calls
    assert plan["Plan"]["Filter"] == f"(note = '{SECRET}'::text)"
    assert (sql, fid, recent) == (CANONICAL, FID, 60)


async def test_04_07_30_llm_gets_the_scrubbed_plan_when_rules_are_empty() -> None:
    llm_suggestion = suggestion().model_copy(update={"source": "llm", "rule_name": None})
    explainer = ExplainerSpy(result=llm_suggestion)
    store = SpyStore()
    w = worker(store=store, rules=RulesSpy([]), explainer=explainer)
    w.submit(job())
    await w.process_pending()
    ((sql, plan, fid),) = explainer.calls
    assert SECRET not in json.dumps(plan)
    assert (sql, fid) == (CANONICAL, FID)
    assert store.suggestion_calls == [(FID, [llm_suggestion])]


async def test_05_rules_hit_skips_llm() -> None:
    explainer = ExplainerSpy(result=suggestion())
    w = worker(explainer=explainer)
    w.submit(job())
    await w.process_pending()
    assert explainer.calls == []


async def test_06_no_explainer_and_no_rules_still_stores_the_plan() -> None:
    store = SpyStore()
    w = worker(store=store, rules=RulesSpy([]))
    w.submit(job())
    await w.process_pending()
    assert len(store.plans) == 1
    assert store.suggestion_calls == []


@pytest.mark.parametrize(
    "result",
    [SEQ_PLAN, json.dumps([SEQ_PLAN]), [SEQ_PLAN]],
)
async def test_result_shapes_are_accepted(result: Any) -> None:
    store = SpyStore()
    w = worker(FakeEngine(result=result), store)
    w.submit(job())
    await w.process_pending()
    assert len(store.plans) == 1


@pytest.mark.parametrize("result", [[], "not json", 42, ["x"], "[]"])
async def test_unusable_results_store_nothing_and_cool_down(result: Any) -> None:
    store = SpyStore()
    w = worker(FakeEngine(result=result), store, now=lambda: 0.0)
    w.submit(job())
    await w.process_pending()
    assert store.plans == []
    assert w.submit(job()) is False  # cooling down


# ---------------------------------------------------------------------------
# Cooldown and submit gating (06: 7-14; 07: 31)
# ---------------------------------------------------------------------------


async def test_07_31_cooling_down_fingerprint_is_not_queued() -> None:
    clock = [0.0]
    w = worker(now=lambda: clock[0], per_fingerprint_cooldown_seconds=60.0)
    assert w.submit(job()) is True
    await w.process_pending()
    assert w.submit(job()) is False
    assert w.accepts(FID) is False
    clock[0] = 61.0
    assert w.accepts(FID) is True
    assert w.submit(job()) is True


async def test_07_31_already_queued_fingerprint_is_not_queued_twice() -> None:
    engine = FakeEngine()
    w = worker(engine)
    assert w.submit(job()) is True
    assert w.submit(job()) is False
    assert w.submit(job(fid="1" * 16)) is True
    assert await w.process_pending() == 2
    assert len(engine.calls) == 2


async def test_09_different_fingerprints_do_not_share_cooldown() -> None:
    w = worker(now=lambda: 0.0)
    w.submit(job())
    await w.process_pending()
    assert w.submit(job(fid="2" * 16)) is True


async def test_10_cooldown_starts_when_explain_finishes() -> None:
    clock = [0.0]

    class SlowEngine(FakeEngine):
        class _Conn(FakeEngine._Conn):
            async def exec_driver_sql(self, statement: str, parameters: Any = None) -> Any:
                clock[0] = 30.0  # EXPLAIN took 30 s
                return await super().exec_driver_sql(statement, parameters)

        def connect(self) -> FakeEngine._Conn:
            return SlowEngine._Conn(self)

    w = worker(SlowEngine(), now=lambda: clock[0], per_fingerprint_cooldown_seconds=60.0)
    w.submit(job())
    await w.process_pending()
    clock[0] = 85.0  # 85 s after enqueue, 55 s after completion
    assert w.submit(job()) is False
    clock[0] = 91.0
    assert w.submit(job()) is True


async def test_processed_jobs_counts_every_dequeued_job() -> None:
    w = worker(now=lambda: 0.0)
    assert w.processed_jobs == 0
    w.submit(job())
    w.submit(job(fid="9" * 16, canonical="update t set a = ?"))  # refused by the guard
    await w.process_pending()
    assert w.processed_jobs == 2


async def test_11_full_queue_returns_false_and_counts() -> None:
    w = worker(max_queue_size=2)
    assert w.submit(job(fid="1" * 16)) is True
    assert w.submit(job(fid="2" * 16)) is True
    assert w.submit(job(fid="3" * 16)) is False
    assert w.dropped_jobs == 1


async def test_13_submit_is_fast() -> None:
    w = worker(max_queue_size=10_000)
    t0 = time.perf_counter()
    for i in range(1_000):
        w.submit(job(fid=f"{i:016x}"))
    assert (time.perf_counter() - t0) / 1_000 < 1e-3


async def test_14_fifo_order() -> None:
    engine = FakeEngine()
    w = worker(engine)
    for i in range(3):
        w.submit(job(fid=f"{i:016x}", statement=f"SELECT {i}"))
    await w.process_pending()
    assert [c[0] for c in engine.calls] == [f"EXPLAIN (FORMAT JSON) SELECT {i}" for i in range(3)]


async def test_07_32_submit_from_another_thread_reaches_the_worker_loop() -> None:
    engine = FakeEngine()
    w = worker(engine)
    await w.start()
    try:
        t = threading.Thread(target=lambda: w.submit(job()))
        t.start()
        t.join()
        for _ in range(50):
            if engine.calls:
                break
            await asyncio.sleep(0.02)
        assert len(engine.calls) == 1
    finally:
        await w.stop()


# ---------------------------------------------------------------------------
# Errors (06: 15-19)
# ---------------------------------------------------------------------------


async def test_15_timeout_stores_nothing_and_continues() -> None:
    store = SpyStore()
    w = worker(FakeEngine(delay=0.5), store, explain_timeout_seconds=0.05, now=lambda: 0.0)
    w.submit(job())
    w.submit(job(fid="2" * 16))
    assert await w.process_pending() == 2
    assert store.plans == []
    assert w.accepts(FID) is False


async def test_16_db_error_stores_nothing_without_retrying_analyze(
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine = FakeEngine(error=RuntimeError(f"bad value {SECRET}"))
    store = SpyStore()
    w = worker(engine, store, now=lambda: 0.0)
    with caplog.at_level(logging.DEBUG):
        w.submit(job(parameters=(SECRET,)))
        await w.process_pending()
    assert len(engine.calls) == 1
    assert store.plans == []
    assert w.accepts(FID) is False
    # The driver's message may echo a parameter: it must not reach the logs.
    assert not any(SECRET in r.getMessage() for r in caplog.records)
    assert not any(r.exc_info for r in caplog.records)


async def test_17_store_errors_are_logged_not_fatal(caplog: pytest.LogCaptureFixture) -> None:
    store = SpyStore(fail_plan=True, fail_suggestions=True)
    w = worker(store=store)
    with caplog.at_level(logging.ERROR):
        w.submit(job())
        w.submit(job(fid="2" * 16))
        assert await w.process_pending() == 2
    messages = [r.getMessage() for r in caplog.records]
    assert any("store_upsert_plan_error" in m for m in messages)
    assert any("store_insert_suggestions_error" in m for m in messages)


async def test_18_rules_crash_is_treated_as_no_suggestion() -> None:
    explainer = ExplainerSpy(result=None)
    store = SpyStore()
    w = worker(store=store, rules=RulesSpy(error=True), explainer=explainer)
    w.submit(job())
    await w.process_pending()
    assert len(explainer.calls) == 1
    assert len(store.plans) == 1


async def test_19_explainer_crash_keeps_the_plan() -> None:
    store = SpyStore()
    w = worker(store=store, rules=RulesSpy([]), explainer=ExplainerSpy(error=True))
    w.submit(job())
    await w.process_pending()
    assert len(store.plans) == 1
    assert store.suggestion_calls == []


# ---------------------------------------------------------------------------
# Lifecycle (06: 20-23)
# ---------------------------------------------------------------------------


async def test_20_stop_leaves_no_task() -> None:
    w = worker(FakeEngine(delay=0.2))
    await w.start()
    w.submit(job())
    await asyncio.sleep(0.01)
    await w.stop()
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "ExplainWorker"]


async def test_21_pending_jobs_are_not_drained_on_stop() -> None:
    engine = FakeEngine()
    w = worker(engine)
    for i in range(5):
        w.submit(job(fid=f"{i:016x}"))
    await w.stop()  # never started
    assert engine.calls == []


async def test_22_23_stop_twice_and_restart() -> None:
    engine = FakeEngine()
    w = worker(engine)
    await w.start()
    await w.start()  # idempotent
    await w.stop()
    await w.stop()
    await w.start()
    w.submit(job())
    for _ in range(50):
        if engine.calls:
            break
        await asyncio.sleep(0.02)
    await w.stop()
    assert len(engine.calls) == 1


async def test_07_10_worker_task_runs_in_an_empty_context() -> None:
    from slowquery_detective.tagging import _REQUEST_ID, current_request_id

    seen: list[str | None] = []

    class Peek(FakeEngine):
        class _Conn(FakeEngine._Conn):
            async def exec_driver_sql(self, statement: str, parameters: Any = None) -> Any:
                seen.append(current_request_id())
                return await super().exec_driver_sql(statement, parameters)

        def connect(self) -> FakeEngine._Conn:
            return Peek._Conn(self)

    w = worker(Peek())
    token = _REQUEST_ID.set("R-started-inside-a-request")
    try:
        await w.start()
    finally:
        _REQUEST_ID.reset(token)
    w.submit(job())
    for _ in range(50):
        if seen:
            break
        await asyncio.sleep(0.02)
    await w.stop()
    assert seen == [None]


# ---------------------------------------------------------------------------
# Security (06: 29, 31, 32)
# ---------------------------------------------------------------------------


async def test_29_no_parameter_value_in_logs_or_store(caplog: pytest.LogCaptureFixture) -> None:
    store = SpyStore()
    w = worker(store=store, rules=RulesSpy([]), explainer=ExplainerSpy(result=None))
    with caplog.at_level(logging.DEBUG):
        w.submit(job(parameters=(SECRET,)))
        await w.process_pending()
    assert not any(SECRET in r.getMessage() for r in caplog.records)
    assert len(store.plans) == 1
    assert SECRET not in json.dumps([p for _f, p, _c in store.plans])


async def test_31_explain_runs_on_the_engine_given_to_the_worker() -> None:
    a, b = FakeEngine(), FakeEngine()
    w = worker(b)
    w.submit(job())
    await w.process_pending()
    assert a.calls == []
    assert len(b.calls) == 1
    assert w.engine is b


def test_32_no_fstring_sql_besides_the_explain_prefix() -> None:
    import inspect

    import slowquery_detective.explain as mod

    source = inspect.getsource(mod)
    assert "text(f" not in source
    assert "EXPLAIN (ANALYZE" not in source


async def test_cooldown_map_is_pruned_of_expired_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    import slowquery_detective.explain as mod

    monkeypatch.setattr(mod, "COOLDOWN_PRUNE_AT", 3)
    clock = [0.0]
    w = worker(now=lambda: clock[0], per_fingerprint_cooldown_seconds=10.0)
    for i in range(3):
        w.submit(job(fid=f"{i:016x}"))
    await w.process_pending()
    clock[0] = 100.0  # all three expired
    w.submit(job(fid="f" * 16))
    await w.process_pending()
    assert w.cooling_down == 1
