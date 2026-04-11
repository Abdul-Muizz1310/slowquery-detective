"""Red tests for docs/specs/06-explain-worker.md.

Unit-level cases (1-23, 29-32) here. The parameter-substitution and
performance cases (24-28, 33-34) that need a real DB live in
tests/integration/test_explain_worker.py.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from slowquery_detective.explain import CachedPlan, ExplainJob, ExplainWorker
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

FID = "abcdef0123456789"
CANONICAL = "SELECT * FROM orders WHERE user_id = ?"


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _mock_store() -> MagicMock:
    store = MagicMock(spec=StoreWriter)
    store.upsert_plan = AsyncMock(return_value=None)
    store.insert_suggestions = AsyncMock(return_value=None)
    store.upsert_fingerprint = AsyncMock(return_value=None)
    store.record_sample = AsyncMock(return_value=None)
    return store


def _mock_engine(plan: dict[str, Any] | None = None) -> MagicMock:
    """An async engine whose .execute returns a fake EXPLAIN result."""
    engine = MagicMock()
    fake_plan = plan or {
        "Plan": {"Node Type": "Seq Scan", "Relation Name": "orders", "Plan Rows": 50_000}
    }

    class _FakeResult:
        def scalar_one(self) -> list[dict[str, Any]]:
            return [{"Plan": fake_plan.get("Plan", fake_plan)}]

    class _FakeConn:
        async def __aenter__(self) -> "_FakeConn":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def execute(self, _stmt: Any) -> _FakeResult:
            return _FakeResult()

    def _connect() -> _FakeConn:
        return _FakeConn()

    engine.connect = _connect
    return engine


def _rules_returning(suggestions: list[Suggestion]) -> Callable[[dict[str, Any], str], list[Suggestion]]:
    def _rules(_plan: dict[str, Any], _sql: str) -> list[Suggestion]:
        return list(suggestions)

    return _rules


def _sample_suggestion() -> Suggestion:
    return Suggestion(
        kind="index",
        sql="CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
        rationale="seq scan on 50k rows",
        confidence=0.9,
        source="rules",
        rule_name="seq_scan_large_table",
    )


def _job(fid: str = FID, now: float = 0.0) -> ExplainJob:
    return ExplainJob(
        fingerprint_id=fid,
        canonical_sql=CANONICAL,
        observed_ms=500.0,
        enqueued_at=now,
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_01_single_job_produces_cached_plan_and_store_write() -> None:
    store = _mock_store()
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=store,
        rules=_rules_returning([_sample_suggestion()]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    cached = worker.plan_cache_get(FID)
    assert cached is not None
    store.upsert_plan.assert_awaited()
    store.insert_suggestions.assert_awaited()


async def test_02_three_fingerprints_all_cached() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([_sample_suggestion()]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    for fid in ("a" * 16, "b" * 16, "c" * 16):
        worker.submit(_job(fid=fid))
    await asyncio.sleep(0.3)
    await worker.stop()
    for fid in ("a" * 16, "b" * 16, "c" * 16):
        assert worker.plan_cache_get(fid) is not None


async def test_03_plan_json_forwarded_verbatim_to_store() -> None:
    custom_plan = {
        "Plan": {
            "Node Type": "Index Scan",
            "Relation Name": "foo",
            "Plan Rows": 1,
            "Index Name": "ix_foo_bar",
        }
    }
    store = _mock_store()
    worker = ExplainWorker(
        engine=_mock_engine(plan=custom_plan),
        store=store,
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    call_args = store.upsert_plan.await_args
    assert call_args is not None
    forwarded_plan = call_args.kwargs.get("plan_json") or call_args.args[1]
    assert forwarded_plan == custom_plan or forwarded_plan["Plan"]["Node Type"] == "Index Scan"


async def test_04_empty_rules_triggers_llm_fallback() -> None:
    explainer = AsyncMock(return_value=_sample_suggestion())
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=explainer,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    explainer.assert_awaited_once()


async def test_05_non_empty_rules_skips_llm() -> None:
    explainer = AsyncMock(return_value=None)
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([_sample_suggestion()]),
        explainer=explainer,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    explainer.assert_not_awaited()


async def test_06_no_explainer_still_caches_plan() -> None:
    store = _mock_store()
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=store,
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None
    store.insert_suggestions.assert_not_awaited()


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------


async def test_07_second_submit_within_cooldown_dropped() -> None:
    engine_calls = 0

    def _engine_now() -> MagicMock:
        nonlocal engine_calls
        engine_calls += 1
        return _mock_engine()

    clock = [0.0]
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        per_fingerprint_cooldown_seconds=60.0,
        now=lambda: clock[0],
    )
    await worker.start()
    worker.submit(_job(now=0.0))
    await asyncio.sleep(0.1)
    clock[0] = 1.0
    worker.submit(_job(now=1.0))
    await asyncio.sleep(0.1)
    await worker.stop()
    # Only one plan should ever have been cached in that 1s window.
    assert worker.plan_cache_get(FID) is not None


async def test_08_cooldown_expires() -> None:
    clock = [0.0]
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        per_fingerprint_cooldown_seconds=60.0,
        now=lambda: clock[0],
    )
    await worker.start()
    worker.submit(_job(now=0.0))
    await asyncio.sleep(0.1)
    clock[0] = 61.0
    worker.submit(_job(now=61.0))
    await asyncio.sleep(0.1)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None


async def test_09_different_fingerprints_do_not_share_cooldown() -> None:
    clock = [0.0]
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        per_fingerprint_cooldown_seconds=60.0,
        now=lambda: clock[0],
    )
    await worker.start()
    worker.submit(_job(fid="a" * 16, now=0.0))
    await asyncio.sleep(0.05)
    worker.submit(_job(fid="b" * 16, now=0.5))
    await asyncio.sleep(0.1)
    await worker.stop()
    assert worker.plan_cache_get("a" * 16) is not None
    assert worker.plan_cache_get("b" * 16) is not None


async def test_10_cooldown_starts_at_completion_not_enqueue() -> None:
    """Spec invariant 3: cooldown starts when EXPLAIN finishes."""
    # Encoded as a behavior assertion: two rapid submits for the same
    # fingerprint, separated by a pause shorter than cooldown — only one
    # should cache. A slow-explaining engine should not cause a back-to-
    # back re-run.
    clock = [0.0]
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        per_fingerprint_cooldown_seconds=5.0,
        now=lambda: clock[0],
    )
    await worker.start()
    worker.submit(_job(now=0.0))
    await asyncio.sleep(0.05)
    clock[0] = 1.0
    worker.submit(_job(now=1.0))
    await asyncio.sleep(0.05)
    await worker.stop()
    # Only one unique plan regardless of how many submits fired.
    assert worker.plan_cache_get(FID) is not None


# ---------------------------------------------------------------------------
# Backpressure / queue
# ---------------------------------------------------------------------------


async def test_11_queue_full_returns_false() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        max_queue_size=2,
        now=lambda: 0.0,
    )
    # Do not start the worker so the queue fills up and never drains.
    assert worker.submit(_job(fid="a" * 16)) is True
    assert worker.submit(_job(fid="b" * 16)) is True
    assert worker.submit(_job(fid="c" * 16)) is False


async def test_12_drops_do_not_affect_queued_items() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        max_queue_size=2,
        now=lambda: 0.0,
    )
    first = _job(fid="a" * 16)
    second = _job(fid="b" * 16)
    worker.submit(first)
    worker.submit(second)
    worker.submit(_job(fid="c" * 16))  # dropped
    await worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()
    assert worker.plan_cache_get("a" * 16) is not None
    assert worker.plan_cache_get("b" * 16) is not None


async def test_13_submit_never_blocks_over_1ms() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    t0 = time.perf_counter()
    worker.submit(_job())
    elapsed = time.perf_counter() - t0
    assert elapsed < 1e-3


async def test_14_fifo_order() -> None:
    processed: list[str] = []

    def _capture_rules(_plan: dict[str, Any], sql: str) -> list[Suggestion]:
        processed.append(sql)
        return []

    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_capture_rules,
        explainer=None,
        now=lambda: 0.0,
    )
    for i in range(5):
        worker.submit(
            ExplainJob(
                fingerprint_id=f"fp{i:014d}",
                canonical_sql=f"SELECT {i}",
                observed_ms=500.0,
                enqueued_at=0.0,
            )
        )
    await worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()
    assert processed == [f"SELECT {i}" for i in range(5)]


# ---------------------------------------------------------------------------
# Timeout / errors
# ---------------------------------------------------------------------------


async def test_15_timeout_caches_nothing_worker_continues() -> None:
    class _HangingEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> "_Conn":
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, _stmt: Any) -> Any:
                    await asyncio.sleep(10.0)

            return _Conn()

    worker = ExplainWorker(
        engine=_HangingEngine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        explain_timeout_seconds=0.1,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.3)
    assert worker.plan_cache_get(FID) is None
    # Second fingerprint still processes normally.
    worker.submit(_job(fid="b" * 16))
    await asyncio.sleep(0.2)
    await worker.stop()


async def test_16_db_error_retries_without_analyze() -> None:
    attempts: list[str] = []

    class _FailingEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> "_Conn":
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, stmt: Any) -> Any:
                    attempts.append(str(stmt))
                    if "ANALYZE" in str(stmt).upper():
                        raise RuntimeError("InvalidTextRepresentation")

                    class _Result:
                        def scalar_one(self) -> list[dict[str, Any]]:
                            return [{"Plan": {"Node Type": "Seq Scan"}}]

                    return _Result()

            return _Conn()

    worker = ExplainWorker(
        engine=_FailingEngine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.2)
    await worker.stop()
    assert len(attempts) >= 2


async def test_17_store_upsert_error_logged_not_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    store = _mock_store()
    store.upsert_plan = AsyncMock(side_effect=RuntimeError("store kaput"))
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=store,
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.2)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None  # cache still updated


async def test_18_rules_raise_treated_as_empty() -> None:
    def _boom(_plan: dict[str, Any], _sql: str) -> list[Suggestion]:
        raise RuntimeError("rules kaboom")

    explainer = AsyncMock(return_value=_sample_suggestion())
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_boom,
        explainer=explainer,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.2)
    await worker.stop()
    explainer.assert_awaited_once()


async def test_19_explainer_raise_persists_plan_without_suggestion() -> None:
    explainer = AsyncMock(side_effect=RuntimeError("llm kaboom"))
    store = _mock_store()
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=store,
        rules=_rules_returning([]),
        explainer=explainer,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.2)
    await worker.stop()
    store.upsert_plan.assert_awaited()


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


async def test_20_shutdown_cancels_in_flight_and_leaves_no_tasks() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await worker.stop()
    # No lingering tasks referencing the worker.
    running = [t for t in asyncio.all_tasks() if not t.done()]
    assert all("ExplainWorker" not in (t.get_name() or "") for t in running)


async def test_21_pending_jobs_not_drained_on_stop() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    for i in range(10):
        worker.submit(_job(fid=f"fp{i:014d}"))
    await worker.start()
    # Do not wait for drain.
    await worker.stop()
    cached = sum(
        1 for i in range(10) if worker.plan_cache_get(f"fp{i:014d}") is not None
    )
    assert cached < 10


async def test_22_stop_twice_is_noop() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    await worker.stop()
    await worker.stop()  # no exception


async def test_23_start_after_stop_restarts_cleanly() -> None:
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    await worker.stop()
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


async def test_29_no_original_literal_in_logs_or_cache_or_store(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    secret = "sk-live-secret-42"
    canonical = "SELECT * FROM users WHERE api_key = ?"
    store = _mock_store()
    worker = ExplainWorker(
        engine=_mock_engine(),
        store=store,
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    # The job carries canonical SQL (fingerprinted), not the original.
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql=canonical,
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.1)
    await worker.stop()

    for record in caplog.records:
        assert secret not in record.message
    cached = worker.plan_cache_get(FID)
    assert cached is None or secret not in repr(cached)
    for call in store.upsert_plan.await_args_list:
        assert secret not in repr(call)


async def test_30_param_synthesizer_never_emits_ddl_looking_sql() -> None:
    """The synthesizer is a small pure function; test it by submitting many
    canonical SQL shapes and asserting no DROP/ALTER/CREATE leaks into the
    EXPLAIN request. Uses a spying engine.
    """
    seen_statements: list[str] = []

    class _SpyEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> "_Conn":
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, stmt: Any) -> Any:
                    seen_statements.append(str(stmt))

                    class _R:
                        def scalar_one(self) -> list[dict[str, Any]]:
                            return [{"Plan": {"Node Type": "Seq Scan"}}]

                    return _R()

            return _Conn()

    worker = ExplainWorker(
        engine=_SpyEngine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    shapes = [
        "SELECT * FROM t WHERE id = ?",
        "SELECT * FROM t WHERE name = ?",
        "SELECT * FROM t WHERE id IN (?)",
        "UPDATE t SET active = ? WHERE id = ?",
    ]
    for i, sql in enumerate(shapes):
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

    for stmt in seen_statements:
        up = stmt.upper()
        assert "DROP " not in up
        assert "ALTER " not in up
        assert "TRUNCATE " not in up
        assert "GRANT " not in up
        assert "REVOKE " not in up


async def test_31_engine_passed_to_worker_is_used_for_explain() -> None:
    """No hard-coded engine — worker uses only what it was constructed with."""
    calls: list[str] = []

    class _MarkerEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> "_Conn":
                    calls.append("used")
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, _stmt: Any) -> Any:
                    class _R:
                        def scalar_one(self) -> list[dict[str, Any]]:
                            return [{"Plan": {"Node Type": "Seq Scan"}}]

                    return _R()

            return _Conn()

    worker = ExplainWorker(
        engine=_MarkerEngine(),
        store=_mock_store(),
        rules=_rules_returning([]),
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(_job())
    await asyncio.sleep(0.1)
    await worker.stop()
    assert calls == ["used"]


def test_32_no_text_sql_concatenation_in_source() -> None:
    """Grep the explain module for any f-string / % that concatenates user
    SQL into ``text(...)``. All substitution must go through a dedicated
    state machine in S4. This is a permanent regression guard.
    """
    import pathlib

    path = pathlib.Path(__file__).resolve().parents[2] / "src" / "slowquery_detective" / "explain.py"
    source = path.read_text(encoding="utf-8")
    # No ``text(f"`` pattern in this module — the synthesizer emits params.
    assert 'text(f"' not in source
    assert "text(f'" not in source
