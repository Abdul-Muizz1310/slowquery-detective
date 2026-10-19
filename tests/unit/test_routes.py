"""Spec 07 cases 44-48 and 53 — routes, authorization, readiness, lifecycle.

Driven through ASGI against the in-memory store. Observations are fed to the
runtime's recorder directly (no database here); the composed flow with real
SQL lives in ``tests/integration/test_fleet_flow.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import slowquery_detective.hooks as hooks_mod
from slowquery_detective import InMemoryStoreWriter, install
from slowquery_detective.recorder import Batch
from slowquery_detective.rules.base import Suggestion

UNREACHABLE = "postgresql+asyncpg://u:p@127.0.0.1:1/db"
FP = "0123456789abcdef"
SQL = "select * from orders where user_id = ?"


@pytest.fixture()
def engine() -> Iterator[AsyncEngine]:
    eng = create_async_engine(UNREACHABLE)
    yield eng
    hooks_mod.detach(eng)


async def _allow(_request: Request) -> bool:
    return True


def _app(engine: AsyncEngine, **kw: Any) -> FastAPI:
    app = FastAPI()
    kw.setdefault("authorize", _allow)
    install(app, engine, service="paper-trail", **kw)
    return app


@asynccontextmanager
async def _client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            yield client


def _observe(app: FastAPI, ms: float, rid: str | None, fp: str = FP, sql: str = SQL) -> None:
    app.state.slowquery_runtime.recorder.observe(fp, sql, ms, rid)


# ---------------------------------------------------------------------------
# Case 45 / 46 — authorization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/_slowquery/queries", f"/_slowquery/queries/{FP}"])
async def test_45_no_authorize_hook_means_403(engine: AsyncEngine, path: str) -> None:
    app = _app(engine, authorize=None)
    async with _client(app) as c:
        resp = await c.get(path)
    assert resp.status_code == 403


async def _deny(_r: Request) -> bool:
    return False


async def _truthy(_r: Request) -> Any:
    return "yes"


async def _raise_401(_r: Request) -> bool:
    raise HTTPException(status_code=401, detail="token expired")


async def _boom(_r: Request) -> bool:
    raise RuntimeError("verifier crashed")


@pytest.mark.parametrize(
    ("hook", "status"),
    [(_deny, 403), (_truthy, 403), (_raise_401, 401), (_boom, 403), (_allow, 200)],
)
async def test_45_authorize_outcomes(engine: AsyncEngine, hook: Any, status: int) -> None:
    app = _app(engine, authorize=hook)
    async with _client(app) as c:
        resp = await c.get("/_slowquery/queries")
    assert resp.status_code == status


async def test_45_sync_hook_returning_true_is_accepted(engine: AsyncEngine) -> None:
    app = _app(engine, authorize=lambda _r: True)
    async with _client(app) as c:
        assert (await c.get("/_slowquery/queries")).status_code == 200


async def test_45_hook_receives_the_request(engine: AsyncEngine) -> None:
    seen: list[str | None] = []

    async def _hook(request: Request) -> bool:
        seen.append(request.headers.get("x-platform-token"))
        return request.headers.get("x-platform-token") == "good"

    app = _app(engine, authorize=_hook)
    async with _client(app) as c:
        assert (
            await c.get("/_slowquery/queries", headers={"x-platform-token": "bad"})
        ).status_code == 403
        assert (
            await c.get("/_slowquery/queries", headers={"x-platform-token": "good"})
        ).status_code == 200
    assert seen == ["bad", "good"]


async def test_45_static_secret_env_no_longer_grants_access(
    engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SLOWQUERY_PLATFORM_TOKEN", "s3cret")
    monkeypatch.setenv("DEMO_MODE", "true")
    app = _app(engine, authorize=None)
    async with _client(app) as c:
        resp = await c.get("/_slowquery/queries", headers={"X-Platform-Token": "s3cret"})
    assert resp.status_code == 403


async def test_46_authorization_runs_before_validation(engine: AsyncEngine) -> None:
    app = _app(engine, authorize=_deny)
    async with _client(app) as c:
        resp = await c.get("/_slowquery/queries", params={"limit": "0", "since": "nope"})
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Case 47 / 48 — validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "params",
    [
        {"limit": "0"},
        {"limit": "501"},
        {"limit": "ten"},
        {"since": "2026-10-20T00:00:00"},  # naive
        {"since": "yesterday"},
        {"request_id": "bad id"},
        {"request_id": "x" * 129},
        {"request_id": ""},
    ],
)
async def test_47_invalid_query_parameters_are_422(
    engine: AsyncEngine, params: dict[str, str]
) -> None:
    app = _app(engine)
    async with _client(app) as c:
        resp = await c.get("/_slowquery/queries", params=params)
    assert resp.status_code == 422


@pytest.mark.parametrize(
    "fid", ["0123456789ABCDEF", "0123456789abcde", "0123456789abcdef0", "zzzzzzzzzzzzzzzz"]
)
async def test_48_malformed_ids_are_404(engine: AsyncEngine, fid: str) -> None:
    app = _app(engine)
    async with _client(app) as c:
        resp = await c.get(f"/_slowquery/queries/{fid}")
    assert resp.status_code == 404


async def test_48_unknown_id_is_404(engine: AsyncEngine) -> None:
    app = _app(engine)
    async with _client(app) as c:
        assert (await c.get(f"/_slowquery/queries/{FP}")).status_code == 404


async def test_48_path_traversal_does_not_reach_the_route(engine: AsyncEngine) -> None:
    app = _app(engine)
    async with _client(app) as c:
        assert (await c.get("/_slowquery/queries/../x")).status_code == 404


# ---------------------------------------------------------------------------
# Case 24 / 25 through the routes (in-memory)
# ---------------------------------------------------------------------------


QUERY_ROW_FIELDS = {
    "id",
    "fingerprint",
    "first_seen",
    "last_seen",
    "call_count",
    "total_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "suggestions",
}


async def test_24_list_flushes_pending_and_returns_exact_fields(engine: AsyncEngine) -> None:
    app = _app(engine)
    async with _client(app) as c:
        _observe(app, 5.0, "R1")
        _observe(app, 7.0, "R1")
        _observe(app, 1.0, None, fp="fedcba9876543210", sql="select 1")
        app.state.slowquery_runtime.buffer.record(FP, 5.0)
        resp = await c.get("/_slowquery/queries")
        assert resp.status_code == 200
        rows = resp.json()
        assert [r["id"] for r in rows] == [FP, "fedcba9876543210"]
        assert set(rows[0]) == QUERY_ROW_FIELDS
        assert rows[0]["call_count"] == 2
        assert rows[0]["total_ms"] == 12.0
        assert rows[0]["max_ms"] == 7.0
        assert rows[0]["fingerprint"] == SQL
        assert rows[0]["p50_ms"] == 5.0
        datetime.fromisoformat(rows[0]["first_seen"])

        by_request = (await c.get("/_slowquery/queries", params={"request_id": "R1"})).json()
        assert [r["id"] for r in by_request] == [FP]
        assert set(by_request[0]) == QUERY_ROW_FIELDS
        assert by_request[0]["call_count"] == 2
        assert by_request[0]["p50_ms"] is None
        assert by_request[0]["p95_ms"] is None
        assert by_request[0]["p99_ms"] is None

        assert (await c.get("/_slowquery/queries", params={"request_id": "R9"})).json() == []
        assert len((await c.get("/_slowquery/queries", params={"limit": 1})).json()) == 1
        future = datetime(2100, 1, 1, tzinfo=UTC).isoformat()
        assert (await c.get("/_slowquery/queries", params={"since": future})).json() == []


async def test_25_detail_shape(engine: AsyncEngine) -> None:
    app = _app(engine, threshold_ms=10)
    async with _client(app) as c:
        _observe(app, 50.0, "R1")
        store = app.state.slowquery_store
        await store.upsert_plan(FP, {"Plan": {"Node Type": "Seq Scan"}}, 3.0, datetime.now(UTC))
        await store.insert_suggestions(
            FP,
            [
                Suggestion(
                    kind="index",
                    sql="CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
                    rationale="seq scan",
                    confidence=0.9,
                    source="rules",
                    rule_name="seq_scan_large_table",
                )
            ],
        )
        body = (await c.get(f"/_slowquery/queries/{FP}")).json()
    assert set(body) == {
        "fingerprint",
        "canonical_sql",
        "explain_plan",
        "suggestions",
        "recent_samples",
    }
    assert set(body["fingerprint"]) == QUERY_ROW_FIELDS
    assert body["canonical_sql"] == SQL
    assert set(body["explain_plan"]) == {"plan_json", "cost", "captured_at"}
    assert set(body["suggestions"][0]) == {"kind", "source", "sql", "rationale", "applied_at"}
    assert body["recent_samples"][0]["request_id"] == "R1"
    assert set(body["recent_samples"][0]) == {"duration_ms", "request_id", "sampled_at"}


# ---------------------------------------------------------------------------
# Case 44 — store not ready
# ---------------------------------------------------------------------------


class FlakyStore(InMemoryStoreWriter):
    """An in-memory store whose start() fails until told otherwise."""

    def __init__(self) -> None:
        super().__init__()
        self.up = False
        self.starts = 0

    async def start(self) -> None:
        self.starts += 1
        if not self.up:
            raise ConnectionRefusedError("database asleep")
        await super().start()


async def test_44_unreachable_store_serves_503_then_recovers(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    store = FlakyStore()
    app = _app(engine, store=store)
    async with _client(app) as c:
        _observe(app, 5.0, "R1")
        assert any(r.levelname == "ERROR" for r in caplog.records)
        assert (await c.get("/_slowquery/queries")).status_code == 503
        store.up = True
        resp = await c.get("/_slowquery/queries", params={"request_id": "R1"})
        assert resp.status_code == 200
        # Data observed while the store was down was held and written once it came up.
        assert [r["call_count"] for r in resp.json()] == [1]


class FailingWrites(InMemoryStoreWriter):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    async def write_batch(self, service: str, batch: Batch) -> None:
        if self.fail:
            raise ConnectionResetError("dropped")
        await super().write_batch(service, batch)


async def test_44_failed_flush_keeps_the_batch_for_the_next_one(engine: AsyncEngine) -> None:
    store = FailingWrites()
    app = _app(engine, store=store)
    runtime = app.state.slowquery_runtime
    async with app.router.lifespan_context(app):
        _observe(app, 5.0, "R1")
        await runtime.flush()
        _observe(app, 6.0, "R1")
        store.fail = False
        await runtime.flush()
        rows = await store.list_request_queries("paper-trail", "R1", since=None, limit=50)
    assert [(r.call_count, r.total_ms) for r in rows] == [(2, 11.0)]


# ---------------------------------------------------------------------------
# Case 27 / 53 — lifecycle
# ---------------------------------------------------------------------------


async def test_27_host_lifespan_runs_around_the_library(engine: AsyncEngine) -> None:
    order: list[str] = []

    @asynccontextmanager
    async def host_lifespan(_app: FastAPI) -> AsyncIterator[dict[str, Any]]:
        order.append("host-start")
        yield {"host_state": 1}
        order.append("host-stop")

    app = FastAPI(lifespan=host_lifespan)
    store = InMemoryStoreWriter()
    original_start, original_close = store.start, store.close

    async def _start() -> None:
        order.append("store-start")
        await original_start()

    async def _close() -> None:
        order.append("store-close")
        await original_close()

    store.start = _start  # type: ignore[method-assign]
    store.close = _close  # type: ignore[method-assign]
    install(app, engine, service="paper-trail", store=store, authorize=_allow)
    async with app.router.lifespan_context(app) as state:
        assert state == {"host_state": 1}
        order.append("serving")
    assert order == ["host-start", "store-start", "serving", "store-close", "host-stop"]


async def test_53_shutdown_flushes_pending_and_leaves_no_tasks(engine: AsyncEngine) -> None:
    store = InMemoryStoreWriter()
    app = _app(engine, store=store)
    before = {t.get_name() for t in asyncio.all_tasks()}
    async with app.router.lifespan_context(app):
        names = {t.get_name() for t in asyncio.all_tasks()} - before
        assert {"slowquery-flush", "ExplainWorker"} <= names
        _observe(app, 5.0, "R1")
    await asyncio.sleep(0)
    after = {t.get_name() for t in asyncio.all_tasks()}
    assert "slowquery-flush" not in after
    assert "ExplainWorker" not in after
    # The final flush wrote the pending observation before the store closed.
    assert [
        r.call_count
        for r in await store.list_request_queries("paper-trail", "R1", since=None, limit=5)
    ] == [1]
    assert not engine.sync_engine._slowquery_attached


async def test_53_shutdown_survives_a_failing_store_close(engine: AsyncEngine) -> None:
    class BadClose(InMemoryStoreWriter):
        async def close(self) -> None:
            raise RuntimeError("close failed")

    app = _app(engine, store=BadClose())
    async with app.router.lifespan_context(app):
        pass


async def test_27_a_second_lifespan_observes_again(engine: AsyncEngine) -> None:
    app = _app(engine)
    async with app.router.lifespan_context(app):
        assert engine.sync_engine._slowquery_attached is True
    assert engine.sync_engine._slowquery_attached is False
    async with app.router.lifespan_context(app):
        assert engine.sync_engine._slowquery_attached is True
        assert app.state.slowquery_runtime.ready


def test_27_lifespans_on_different_event_loops_both_work(engine: AsyncEngine) -> None:
    """Host test suites enter ``TestClient(app)`` repeatedly; each block runs the
    lifespan on a new event loop. The worker and the flush must work in each."""
    import time

    from starlette.testclient import TestClient

    from slowquery_detective.explain import ExplainJob

    app = _app(engine)
    runtime = app.state.slowquery_runtime
    for i in range(3):
        with TestClient(app) as client:
            assert client.get("/_slowquery/queries").status_code == 200
            job = ExplainJob(f"{i:016x}", "select 1", "SELECT 1", (), 500.0, 1, 0.0)
            assert runtime.worker.submit(job) is True
            deadline = time.monotonic() + 5.0
            while runtime.worker.processed_jobs < i + 1 and time.monotonic() < deadline:
                time.sleep(0.02)
            assert runtime.worker.processed_jobs == i + 1, f"worker stalled in lifespan {i}"
            assert runtime.worker.queued == 0


# ---------------------------------------------------------------------------
# Review follow-ups
# ---------------------------------------------------------------------------


async def test_09b_measure_call_is_not_attributed_to_its_request_id(engine: AsyncEngine) -> None:
    """bastion sends its X-Request-Id on the measure call too; whatever the
    authorize hook (or anything else) runs during that call must not be
    charged to the request being measured."""
    seen: list[str | None] = []

    async def _hook(_request: Request) -> bool:
        from slowquery_detective import current_request_id

        seen.append(current_request_id())
        return True

    app = _app(engine, authorize=_hook)
    async with _client(app) as c:
        resp = await c.get(
            "/_slowquery/queries", params={"request_id": "R"}, headers={"X-Request-Id": "R"}
        )
    assert resp.status_code == 200
    assert seen == [None]


async def test_45_starlette_http_exception_from_the_hook_propagates(engine: AsyncEngine) -> None:
    from starlette.exceptions import HTTPException as StarletteHTTPException

    async def _hook(_request: Request) -> bool:
        raise StarletteHTTPException(status_code=401, detail="expired")

    app = _app(engine, authorize=_hook)
    async with _client(app) as c:
        assert (await c.get("/_slowquery/queries")).status_code == 401
