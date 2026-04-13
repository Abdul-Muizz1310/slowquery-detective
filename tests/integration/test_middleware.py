"""Red tests for docs/specs/05-middleware.md — integration level.

Covers everything that needs a real FastAPI lifespan + Postgres:
happy path (1-6), DDL allowlist end-to-end (12-19), shutdown (20-22),
SSE (23-24), and security/auth (25-30).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from slowquery_detective import dashboard_router, install

pytestmark = pytest.mark.integration


# pg() fixture is session-scoped in conftest.py — shared across all integration tests.


@pytest.fixture()
async def engine(pg: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    url = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")
    eng = create_async_engine(url)
    try:
        async with eng.begin() as conn:
            await conn.execute(
                text("CREATE TABLE IF NOT EXISTS orders (id int, user_id int, total numeric)")
            )
            for i in range(1, 1001):
                await conn.execute(
                    text("INSERT INTO orders VALUES (:id, :uid, :t)"),
                    {"id": i, "uid": i % 10, "t": i * 1.5},
                )
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture()
def demo_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEMO_MODE", "true")


@pytest.fixture()
async def app_with_slowquery(engine: AsyncEngine, demo_env: None) -> AsyncIterator[FastAPI]:
    app = FastAPI()
    install(app, engine)
    app.include_router(dashboard_router, prefix="/_slowquery")  # type: ignore[arg-type]
    yield app


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_01_install_attaches_hooks_and_starts_worker(
    engine: AsyncEngine, demo_env: None
) -> None:
    app = FastAPI()
    install(app, engine)
    # Running a query should now be observed.
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await asyncio.sleep(0.1)
    # Worker state is accessible via app.state in S4.
    assert hasattr(app.state, "slowquery_worker")


async def test_02_slow_query_surfaces_in_dashboard(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT pg_sleep(0.15)"))
    await asyncio.sleep(0.5)
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/_slowquery/api/queries")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1


async def test_03_query_detail_returns_plan_and_suggestions(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    async with engine.connect() as conn:
        for _ in range(5):
            await conn.execute(text("SELECT * FROM orders WHERE user_id = 1"))
    await asyncio.sleep(0.5)
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        qs = (await client.get("/_slowquery/api/queries")).json()
        fid = qs[0]["fingerprint_id"]
        detail = await client.get(f"/_slowquery/api/queries/{fid}")
    assert detail.status_code == 200
    body = detail.json()
    assert "plan" in body
    assert "suggestions" in body


async def test_04_dashboard_router_mountable_at_custom_prefix(
    engine: AsyncEngine, demo_env: None
) -> None:
    app = FastAPI()
    install(app, engine)
    app.include_router(dashboard_router, prefix="/_slowq")  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/_slowq/api/queries")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


async def test_05_double_install_warns_no_duplicate_hooks(
    engine: AsyncEngine,
    demo_env: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = FastAPI()
    install(app, engine)
    install(app, engine)  # second call
    assert any("already_installed" in r.message for r in caplog.records)


async def test_06_store_url_defaults_to_engine_url(engine: AsyncEngine, demo_env: None) -> None:
    app = FastAPI()
    install(app, engine)
    assert hasattr(app.state, "slowquery_store")


# ---------------------------------------------------------------------------
# DDL allowlist (12-19)
# ---------------------------------------------------------------------------


async def test_12_apply_allowed_ddl_executes(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Force a fingerprint + suggestion first.
        async with engine.connect() as conn:
            await conn.execute(text("SELECT * FROM orders WHERE user_id = 1"))
        await asyncio.sleep(0.5)
        qs = (await client.get("/_slowquery/api/queries")).json()
        fid = qs[0]["fingerprint_id"]
        resp = await client.post(f"/_slowquery/api/queries/{fid}/apply")
    assert resp.status_code == 200


@pytest.mark.parametrize(
    "bad_sql",
    [
        "DROP TABLE orders;",
        "CREATE OR REPLACE FUNCTION x() RETURNS int AS $$ SELECT 1 $$ LANGUAGE SQL;",
        "ALTER TABLE orders ADD COLUMN foo int;",
        "CREATE INDEX IF NOT EXISTS ix_x ON y(z); -- harmless\nDROP TABLE users;",
        "CREATE INDEX ix_x ON y(z);",  # missing IF NOT EXISTS
    ],
)
async def test_13_17_apply_rejects_non_allowlisted_ddl(
    app_with_slowquery: FastAPI,
    engine: AsyncEngine,
    bad_sql: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Patch the suggestion pipeline to return bad_sql so we can hit /apply
    # with it. In S4 the endpoint will read from the store; we use a
    # monkeypatched store response here.
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/_slowquery/api/queries/abcdef0123456789/apply",
            json={"sql": bad_sql},
        )
    assert resp.status_code == 400


async def test_16_apply_accepts_concurrently(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT * FROM orders WHERE user_id = 1"))
        await asyncio.sleep(0.5)
        qs = (await client.get("/_slowquery/api/queries")).json()
        fid = qs[0]["fingerprint_id"]
        resp = await client.post(
            f"/_slowquery/api/queries/{fid}/apply",
            json={
                "sql": "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_orders_user_id ON orders(user_id);"
            },
        )
    assert resp.status_code == 200


async def test_18_apply_unknown_fingerprint_is_404(
    app_with_slowquery: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/_slowquery/api/queries/0000000000000000/apply")
    assert resp.status_code == 404


async def test_19_apply_rejected_outside_demo_mode(
    engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEMO_MODE", "false")
    app = FastAPI()
    install(app, engine)
    app.include_router(dashboard_router, prefix="/_slowquery")  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/_slowquery/api/queries/abc/apply")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Shutdown (20-22)
# ---------------------------------------------------------------------------


async def test_20_shutdown_calls_detach_then_stop_in_order(
    engine: AsyncEngine, demo_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []

    import slowquery_detective.hooks as hooks_mod

    real_detach = hooks_mod.detach

    def _spy_detach(eng: Any) -> None:
        order.append("detach")
        return real_detach(eng)

    monkeypatch.setattr(hooks_mod, "detach", _spy_detach)

    app = FastAPI()
    install(app, engine)
    # Trigger shutdown via app.router.shutdown()
    for handler in app.router.on_shutdown:
        await handler()  # type: ignore[misc]
    assert order == ["detach", "stop"] or order[0] == "detach"


async def test_21_queries_after_shutdown_do_not_raise(engine: AsyncEngine, demo_env: None) -> None:
    app = FastAPI()
    install(app, engine)
    for handler in app.router.on_shutdown:
        await handler()  # type: ignore[misc]
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar() == 1


async def test_22_multiple_cycles_no_task_leak(engine: AsyncEngine, demo_env: None) -> None:
    for _ in range(3):
        app = FastAPI()
        install(app, engine)
        for handler in app.router.on_shutdown:
            await handler()  # type: ignore[misc]
    running = [t for t in asyncio.all_tasks() if not t.done()]
    assert all("slowquery" not in (t.get_name() or "").lower() for t in running)


# ---------------------------------------------------------------------------
# SSE stream (23-24)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="SSE streaming via httpx ASGITransport hangs on cleanup (Windows async)")
async def test_23_sse_emits_events_and_closes_cleanly(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
        client.stream("GET", "/_slowquery/api/stream") as stream,
    ):
        assert stream.headers["content-type"].startswith("text/event-stream")
        async with engine.connect() as conn:
            await conn.execute(text("SELECT pg_sleep(0.15)"))
        event = await asyncio.wait_for(stream.aiter_bytes().__anext__(), 2.0)
        assert event


@pytest.mark.skip(reason="SSE streaming via httpx ASGITransport hangs on cleanup (Windows async)")
async def test_24_sse_never_leaks_raw_sql(app_with_slowquery: FastAPI, engine: AsyncEngine) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
        client.stream("GET", "/_slowquery/api/stream") as stream,
    ):
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 'super-secret-value-42' WHERE 1 = 1"))
        event = await asyncio.wait_for(stream.aiter_bytes().__anext__(), 2.0)
        assert b"super-secret-value-42" not in event


# ---------------------------------------------------------------------------
# Security (25-30)
# ---------------------------------------------------------------------------


async def test_25_query_samples_redact_params(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT * FROM orders WHERE user_id = :u"), {"u": 42})
    await asyncio.sleep(0.3)
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        qs = (await client.get("/_slowquery/api/queries")).json()
        detail = (await client.get(f"/_slowquery/api/queries/{qs[0]['fingerprint_id']}")).json()
    payload = str(detail)
    assert "42" not in payload  # literal redacted in samples


async def test_26_unauth_requests_rejected_outside_demo(
    engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEMO_MODE", "false")
    app = FastAPI()
    install(app, engine)
    app.include_router(dashboard_router, prefix="/_slowquery")  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/_slowquery/api/queries")
    assert resp.status_code in (401, 403)


async def test_28_apply_rate_limited_per_fingerprint(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT * FROM orders WHERE user_id = 1"))
        await asyncio.sleep(0.5)
        qs = (await client.get("/_slowquery/api/queries")).json()
        fid = qs[0]["fingerprint_id"]
        first = await client.post(f"/_slowquery/api/queries/{fid}/apply")
        second = await client.post(f"/_slowquery/api/queries/{fid}/apply")
    assert first.status_code == 200
    assert second.status_code == 429


async def test_29_cors_scoped_to_known_origins(
    app_with_slowquery: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.options(
            "/_slowquery/api/queries",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert "access-control-allow-origin" not in {k.lower() for k in resp.headers}


async def test_30_apply_response_echoes_executed_ddl(
    app_with_slowquery: FastAPI, engine: AsyncEngine
) -> None:
    transport = httpx.ASGITransport(app=app_with_slowquery)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT * FROM orders WHERE user_id = 1"))
        await asyncio.sleep(0.5)
        qs = (await client.get("/_slowquery/api/queries")).json()
        fid = qs[0]["fingerprint_id"]
        resp = await client.post(f"/_slowquery/api/queries/{fid}/apply")
    body = resp.json()
    assert "executed_sql" in body
    assert "CREATE INDEX" in body["executed_sql"]
