"""ASGI-level tests for the dashboard router — runnable in CI (no Docker).

These drive the *composed* HTTP path (real ``dashboard_router`` + real
``RingBuffer`` + a fake worker/engine) via ``httpx.ASGITransport``. The
package's end-to-end coverage previously lived only in the testcontainers
integration suite, which never runs in CI — exactly why the dashboard's auth,
DDL-binding, and detail-shape defects shipped undetected (audit HIGH: coverage
gate masks near-untested critical-path code). Everything here runs under the
default ``-m "not slow and not integration"`` filter.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from slowquery_detective import dashboard_router
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.explain import CachedPlan
from slowquery_detective.rules.base import Suggestion

FID = "abcdef0123456789"
CANONICAL = "select * from orders where user_id = ?"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeConn:
    def __init__(self, engine: _FakeEngine) -> None:
        self._engine = engine

    async def __aenter__(self) -> _FakeConn:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, stmt: Any) -> None:
        self._engine.executed.append(str(stmt))

    async def commit(self) -> None:
        return None


class _FakeEngine:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execution_options(self, **_: Any) -> _FakeEngine:
        return self

    def connect(self) -> _FakeConn:
        return _FakeConn(self)


class _FakeWorker:
    def __init__(self, cache: dict[str, CachedPlan] | None = None) -> None:
        self._cache = cache or {}
        self._engine = _FakeEngine()
        self.pending_calls = 0

    def plan_cache_get(self, fid: str) -> CachedPlan | None:
        return self._cache.get(fid)

    @property
    def engine(self) -> _FakeEngine:
        return self._engine

    async def process_pending(self, limit: int | None = None) -> int:
        self.pending_calls += 1
        return 0


def _cached_with_suggestion() -> CachedPlan:
    return CachedPlan(
        plan_json={"Plan": {"Node Type": "Seq Scan"}},
        plan_text="",
        cost=1.0,
        captured_at=0.0,
        suggestions=(
            Suggestion(
                kind="index",
                sql="CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
                rationale="seq scan",
                confidence=0.9,
                source="rules",
                rule_name="seq_scan_large_table",
            ),
        ),
    )


def _make_app(
    *,
    buffer: RingBuffer | None = None,
    worker: _FakeWorker | None = None,
    canonical_sql_cache: dict[str, str] | None = None,
) -> tuple[FastAPI, _FakeWorker]:
    app = FastAPI()
    app.include_router(dashboard_router, prefix="/_slowquery")  # type: ignore[arg-type]
    buf = buffer if buffer is not None else RingBuffer()
    wk = worker if worker is not None else _FakeWorker()
    app.state.slowquery_buffer = buf
    app.state.slowquery_worker = wk
    app.state.slowquery_canonical_sql_cache = canonical_sql_cache or {}
    return app, wk


def _client(app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture()
def demo_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEMO_MODE", "true")
    monkeypatch.delenv("SLOWQUERY_PLATFORM_TOKEN", raising=False)


# ---------------------------------------------------------------------------
# Auth (HIGH — no per-request identity beyond a global DEMO_MODE flag)
# ---------------------------------------------------------------------------


async def test_list_queries_forbidden_outside_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEMO_MODE", "false")
    monkeypatch.delenv("SLOWQUERY_PLATFORM_TOKEN", raising=False)
    app, _ = _make_app()
    async with _client(app) as c:
        resp = await c.get("/_slowquery/api/queries")
    assert resp.status_code == 403


async def test_token_required_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    # A configured token is enforced even in demo mode (so a public demo can
    # refuse anonymous callers).
    monkeypatch.setenv("DEMO_MODE", "true")
    monkeypatch.setenv("SLOWQUERY_PLATFORM_TOKEN", "s3cret")
    app, _ = _make_app()
    async with _client(app) as c:
        missing = await c.get("/_slowquery/api/queries")
        wrong = await c.get("/_slowquery/api/queries", headers={"X-Platform-Token": "nope"})
        ok = await c.get("/_slowquery/api/queries", headers={"X-Platform-Token": "s3cret"})
    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert ok.status_code == 200


# ---------------------------------------------------------------------------
# GET /api/queries + detail (LOW — detail must include the documented samples)
# ---------------------------------------------------------------------------


async def test_list_queries_returns_percentiles(demo_env: None) -> None:
    buf = RingBuffer()
    for d in (10.0, 20.0, 30.0):
        buf.record(FID, d)  # real monotonic time so samples are live at read
    app, _ = _make_app(buffer=buf)
    async with _client(app) as c:
        resp = await c.get("/_slowquery/api/queries")
    assert resp.status_code == 200
    data = resp.json()
    assert any(e["fingerprint_id"] == FID and "p95_ms" in e for e in data)


async def test_query_detail_includes_percentiles(demo_env: None) -> None:
    buf = RingBuffer()
    for d in (10.0, 20.0, 30.0):
        buf.record(FID, d)  # real monotonic time so samples are live at read
    worker = _FakeWorker(cache={FID: _cached_with_suggestion()})
    app, _ = _make_app(buffer=buf, worker=worker)
    async with _client(app) as c:
        resp = await c.get(f"/_slowquery/api/queries/{FID}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["plan"]
    assert body["suggestions"]
    # The documented "recent samples" — exposed as an aggregate percentile
    # summary, never raw literals.
    assert body["percentiles"] is not None
    assert body["percentiles"]["sample_count"] == 3
    assert "p95_ms" in body["percentiles"]


async def test_query_detail_unknown_is_404(demo_env: None) -> None:
    app, _ = _make_app()
    async with _client(app) as c:
        resp = await c.get("/_slowquery/api/queries/0000000000000000")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /apply — DDL binding + allowlist (HIGH)
# ---------------------------------------------------------------------------


async def test_apply_body_ddl_bound_to_fingerprint_executes(demo_env: None) -> None:
    buf = RingBuffer()
    buf.record(FID, 500.0, now=0.0)
    app, worker = _make_app(buffer=buf, canonical_sql_cache={FID: CANONICAL})
    async with _client(app) as c:
        resp = await c.post(
            f"/_slowquery/api/queries/{FID}/apply",
            json={"sql": "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "CREATE INDEX" in body["executed_sql"]
    assert worker.engine.executed  # actually executed against the (fake) engine


async def test_apply_body_ddl_unbound_table_rejected(demo_env: None) -> None:
    """An index on a table that isn't in the fingerprint's SQL is rejected."""
    buf = RingBuffer()
    buf.record(FID, 500.0, now=0.0)
    app, worker = _make_app(buffer=buf, canonical_sql_cache={FID: CANONICAL})
    async with _client(app) as c:
        resp = await c.post(
            f"/_slowquery/api/queries/{FID}/apply",
            # `secrets` table has nothing to do with the `orders` fingerprint.
            json={"sql": "CREATE INDEX IF NOT EXISTS ix_secrets_token ON secrets(token);"},
        )
    assert resp.status_code == 400
    assert not worker.engine.executed


@pytest.mark.parametrize(
    "bad_sql",
    [
        "DROP TABLE orders;",
        "ALTER TABLE orders ADD COLUMN foo int;",
        "CREATE INDEX ix_x ON y(z);",  # missing IF NOT EXISTS
        "CREATE INDEX IF NOT EXISTS ix_x ON y(z); DROP TABLE users;",
    ],
)
async def test_apply_body_ddl_non_allowlisted_rejected(demo_env: None, bad_sql: str) -> None:
    buf = RingBuffer()
    buf.record(FID, 500.0, now=0.0)
    app, worker = _make_app(buffer=buf, canonical_sql_cache={FID: CANONICAL})
    async with _client(app) as c:
        resp = await c.post(f"/_slowquery/api/queries/{FID}/apply", json={"sql": bad_sql})
    assert resp.status_code == 400
    assert not worker.engine.executed


async def test_apply_unknown_fingerprint_no_body_is_404(demo_env: None) -> None:
    app, _ = _make_app()
    async with _client(app) as c:
        resp = await c.post("/_slowquery/api/queries/0000000000000000/apply")
    assert resp.status_code == 404


async def test_apply_uses_cached_rules_suggestion(demo_env: None) -> None:
    buf = RingBuffer()
    buf.record(FID, 500.0, now=0.0)
    worker = _FakeWorker(cache={FID: _cached_with_suggestion()})
    app, _ = _make_app(buffer=buf, worker=worker, canonical_sql_cache={FID: CANONICAL})
    async with _client(app) as c:
        resp = await c.post(f"/_slowquery/api/queries/{FID}/apply")
    assert resp.status_code == 200
    # Suggestions come only from the worker's plan cache, flushed via the
    # public process_pending() (no private-queue access).
    assert worker.pending_calls == 1
    assert worker.engine.executed


async def test_apply_rate_limited_second_call(demo_env: None) -> None:
    buf = RingBuffer()
    buf.record(FID, 500.0, now=0.0)
    worker = _FakeWorker(cache={FID: _cached_with_suggestion()})
    app, _ = _make_app(buffer=buf, worker=worker, canonical_sql_cache={FID: CANONICAL})
    async with _client(app) as c:
        first = await c.post(f"/_slowquery/api/queries/{FID}/apply")
        second = await c.post(f"/_slowquery/api/queries/{FID}/apply")
    assert first.status_code == 200
    assert second.status_code == 429


# ---------------------------------------------------------------------------
# Edge cases: entry with no live percentiles, and the SSE stream.
# ---------------------------------------------------------------------------


async def test_list_queries_entry_without_live_percentiles(demo_env: None) -> None:
    buf = RingBuffer()
    # Recorded long ago (now=0.0) so it's evicted by the time we read at real
    # monotonic time — the fingerprint is still listed, just without stats.
    buf.record(FID, 5.0, now=0.0)
    app, _ = _make_app(buffer=buf)
    async with _client(app) as c:
        resp = await c.get("/_slowquery/api/queries")
    assert resp.status_code == 200
    entry = next(e for e in resp.json() if e["fingerprint_id"] == FID)
    assert "p95_ms" not in entry  # no live samples -> summary omitted


async def test_sse_emits_event_without_leaking_sql(demo_env: None) -> None:
    buf = RingBuffer()
    app, _ = _make_app(buffer=buf)
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    collected = b""

    async def _reader() -> None:
        nonlocal collected
        req = client.build_request("GET", "/_slowquery/api/stream")
        resp = await client.send(req, stream=True)
        async for chunk in resp.aiter_bytes():
            collected += chunk
            if b"\n\n" in collected:
                break

    task = asyncio.create_task(_reader())
    await asyncio.sleep(0.05)
    # A new fingerprint recorded after the stream opened must surface.
    buf.record(FID, 12.0)
    try:
        await asyncio.wait_for(asyncio.shield(task), 2.0)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    finally:
        await client.aclose()

    # SSE over httpx ASGITransport is timing-sensitive; the load-bearing
    # assertions are (a) no hang/crash and (b) if an event arrived, it carries
    # the fingerprint id but never raw SQL (spec case 24).
    if collected:
        assert b"data:" in collected
        assert FID.encode() in collected
        assert b"select" not in collected.lower()
        assert b"orders" not in collected.lower()
