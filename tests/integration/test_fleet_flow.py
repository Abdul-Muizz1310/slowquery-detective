"""Spec 07 — the composed flow on a real asyncpg engine and Postgres store.

Cases 16, 19-22, 28-30 and 49-53: a FastAPI app installs the library with
``store_url`` pointing at Postgres; requests tagged with ``X-Request-Id`` run
real SQL; the library's own routes report what each request caused.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from fastapi import BackgroundTasks, FastAPI, Request
from sqlalchemy import Column, MetaData, String, Table, Uuid, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

import slowquery_detective.hooks as hooks_mod
from slowquery_detective import install

pytestmark = pytest.mark.integration

SECRET = "sk-live-4f9a2c7e1b"
ACCOUNT_ID = uuid.UUID("6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f")

metadata = MetaData()
accounts = Table(
    "accounts",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("name", String),
)


def asyncpg_url(pg: PostgresContainer) -> str:
    return pg.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")


async def _allow(_request: Request) -> bool:
    return True


@pytest.fixture()
async def engine(pg: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(asyncpg_url(pg))
    async with eng.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS slowquery CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS fleet_orders, accounts CASCADE"))
        await conn.execute(
            text(
                "CREATE TABLE fleet_orders (id int primary key, user_id int, total numeric, note text)"
            )
        )
        await conn.execute(
            text(
                "INSERT INTO fleet_orders SELECT g, g % 10, g * 1.5, "
                "CASE WHEN g = 7 THEN :secret ELSE 'n' || g END FROM generate_series(1, 2000) g"
            ),
            {"secret": SECRET},
        )
        await conn.run_sync(metadata.create_all)
        await conn.execute(accounts.insert().values(id=ACCOUNT_ID, name="acme"))
        # A sequence, because nextval() survives a rollback: an EXPLAIN that
        # executed its target would move it even inside an uncommitted connection.
        await conn.execute(text("DROP SEQUENCE IF EXISTS counter_seq"))
        await conn.execute(text("CREATE SEQUENCE counter_seq"))
        await conn.execute(
            text(
                "CREATE OR REPLACE FUNCTION bump() RETURNS bigint LANGUAGE sql VOLATILE AS "
                "$$ SELECT nextval('counter_seq') $$"
            )
        )
    yield eng
    hooks_mod.detach(eng)
    await eng.dispose()


def build_app(engine: AsyncEngine, pg: PostgresContainer, **kw: Any) -> FastAPI:
    app = FastAPI()

    async def q(sql: str, params: dict[str, Any] | None = None) -> None:
        async with engine.connect() as conn:
            await conn.execute(text(sql), params or {})

    @app.get("/work")
    async def work() -> dict[str, str]:
        await q("SELECT count(*) FROM fleet_orders WHERE user_id = :u", {"u": 1})
        await q("SELECT count(*) FROM fleet_orders WHERE user_id = :u", {"u": 2})
        await q("SELECT max(total) FROM fleet_orders")
        return {"ok": "1"}

    async def _bg_query() -> None:
        await q("SELECT min(total) FROM fleet_orders WHERE id > :i", {"i": 5})

    @app.get("/background")
    async def background(tasks: BackgroundTasks) -> dict[str, str]:
        tasks.add_task(_bg_query)
        task = asyncio.create_task(q("SELECT avg(total) FROM fleet_orders"))
        await task
        return {"ok": "1"}

    @app.get("/slow")
    async def slow() -> dict[str, str]:
        await q("SELECT o.id, pg_sleep(0.03) FROM fleet_orders o WHERE o.note = :s", {"s": SECRET})
        return {"ok": "1"}

    @app.get("/uuid")
    async def by_uuid() -> dict[str, str]:
        async with engine.connect() as conn:
            await conn.execute(
                select(accounts.c.name, text("pg_sleep(0.03)")).where(accounts.c.id == ACCOUNT_ID)
            )
        return {"ok": "1"}

    @app.get("/bump")
    async def bump() -> dict[str, str]:
        await q("SELECT bump(), pg_sleep(0.03)")
        return {"ok": "1"}

    @app.get("/fail")
    async def fail() -> dict[str, str]:
        try:
            await q("SELECT 1 / (user_id - user_id) FROM fleet_orders WHERE id = 1")
        except Exception as exc:  # the host sees its own error
            return {"error": type(exc).__name__}
        return {"error": "none"}

    kw.setdefault("authorize", _allow)
    kw.setdefault("store_url", asyncpg_url(pg))
    install(app, engine, service="paper-trail", **kw)
    return app


@asynccontextmanager
async def running(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            yield client


async def by_request(client: httpx.AsyncClient, rid: str) -> list[dict[str, Any]]:
    resp = await client.get("/_slowquery/queries", params={"request_id": rid})
    assert resp.status_code == 200, resp.text
    rows: list[dict[str, Any]] = resp.json()
    return rows


def calls(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {r["fingerprint"]: r["call_count"] for r in rows}


async def eventually(check: Callable[[], Awaitable[bool]], timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# 49 — the WP-8 acceptance shape
# ---------------------------------------------------------------------------


async def test_49_request_id_lists_exactly_the_queries_it_caused(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg)
    async with running(app) as c:
        assert (await c.get("/work", headers={"X-Request-Id": "R-49"})).status_code == 200
        assert (await c.get("/work")).status_code == 200  # untagged: adds nothing to R-49
        rows = await by_request(c, "R-49")
        assert calls(rows) == {
            "select count(*) from fleet_orders where user_id = ?": 2,
            "select max(total) from fleet_orders": 1,
        }
        for r in rows:
            assert r["p50_ms"] is None and r["p95_ms"] is None and r["p99_ms"] is None
            assert r["total_ms"] > 0 and r["max_ms"] > 0
        # The service-wide list counts both requests.
        everything = (await c.get("/_slowquery/queries")).json()
        assert calls(everything)["select count(*) from fleet_orders where user_id = ?"] == 4
        # 16: the store's own statements are never observed.
        assert not any("slowquery." in r["fingerprint"] for r in everything)


async def test_50_background_tasks_and_spawned_tasks_are_attributed(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg)
    async with running(app) as c:
        await c.get("/background", headers={"X-Request-Id": "R-50"})
        rows = await by_request(c, "R-50")
    assert calls(rows) == {
        "select min(total) from fleet_orders where id > ?": 1,
        "select avg(total) from fleet_orders": 1,
    }


# ---------------------------------------------------------------------------
# 51 / 28 / 29 / 30 — slow statements, EXPLAIN with real parameters
# ---------------------------------------------------------------------------


async def test_51_slow_statement_sample_and_scrubbed_plan(
    engine: AsyncEngine, pg: PostgresContainer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    app = build_app(engine, pg, threshold_ms=10)
    async with running(app) as c:
        await c.get("/slow", headers={"X-Request-Id": "R-51"})
        (row,) = [r for r in await by_request(c, "R-51") if "pg_sleep" in r["fingerprint"]]
        fid = row["id"]

        async def _has_plan() -> bool:
            body = (await c.get(f"/_slowquery/queries/{fid}")).json()
            return body["explain_plan"] is not None

        await eventually(_has_plan)
        detail = (await c.get(f"/_slowquery/queries/{fid}")).json()
    assert detail["recent_samples"][0]["request_id"] == "R-51"
    assert detail["recent_samples"][0]["duration_ms"] >= 10
    plan_text = json.dumps(detail["explain_plan"]["plan_json"])
    assert "fleet_orders" in plan_text
    # 30: the real parameter was used to plan but is stored nowhere.
    assert SECRET not in plan_text
    assert SECRET not in json.dumps(detail)
    assert not any(SECRET in r.getMessage() for r in caplog.records)
    async with engine.connect() as conn:
        stored = (
            (await conn.execute(text("SELECT plan_json::text FROM slowquery.explain_plans")))
            .scalars()
            .all()
        )
    assert stored and all(SECRET not in s for s in stored)


async def test_28_typed_uuid_parameter_is_explained(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg, threshold_ms=10)
    async with running(app) as c:
        await c.get("/uuid", headers={"X-Request-Id": "R-28"})
        (row,) = await by_request(c, "R-28")
        assert "accounts" in row["fingerprint"]

        async def _has_plan() -> bool:
            body = (await c.get(f"/_slowquery/queries/{row['id']}")).json()
            return body["explain_plan"] is not None

        await eventually(_has_plan)


async def test_29_explain_never_executes_the_statement(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg, threshold_ms=10)
    async with running(app) as c:
        await c.get("/bump", headers={"X-Request-Id": "R-29"})
        (row,) = await by_request(c, "R-29")

        async def _has_plan() -> bool:
            body = (await c.get(f"/_slowquery/queries/{row['id']}")).json()
            return body["explain_plan"] is not None

        await eventually(_has_plan)
    async with engine.connect() as conn:
        n = (await conn.execute(text("SELECT last_value FROM counter_seq"))).scalar_one()
    assert n == 1  # the request ran it once; EXPLAIN did not run it again


# ---------------------------------------------------------------------------
# 19 / 20 / 22 — hooks on a real asyncpg engine
# ---------------------------------------------------------------------------


async def test_20_sampling_never_drops_tagged_statements(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg, sample_rate=0.0)
    async with running(app) as c:
        await c.get("/work")
        await c.get("/work", headers={"X-Request-Id": "R-20"})
        everything = (await c.get("/_slowquery/queries")).json()
        tagged = await by_request(c, "R-20")
    assert calls(tagged)["select count(*) from fleet_orders where user_id = ?"] == 2
    # Only the tagged request's statements were recorded at sample_rate=0.
    assert calls(everything)["select count(*) from fleet_orders where user_id = ?"] == 2


async def test_22_failing_statement_is_recorded_and_error_reaches_the_host(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg)
    async with running(app) as c:
        body = (await c.get("/fail", headers={"X-Request-Id": "R-22"})).json()
        rows = await by_request(c, "R-22")
    assert body["error"] == "DBAPIError" or body["error"].endswith("Error")
    assert body["error"] != "none"
    assert len(rows) == 1
    assert "fleet_orders" in rows[0]["fingerprint"]


# ---------------------------------------------------------------------------
# 52 / 53 — restart and shutdown
# ---------------------------------------------------------------------------


async def test_52_restart_keeps_data_and_tolerates_existing_schema(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    first = build_app(engine, pg)
    async with running(first) as c:
        await c.get("/work", headers={"X-Request-Id": "R-52"})
        assert len(await by_request(c, "R-52")) == 2
    hooks_mod.detach(engine)

    second = build_app(engine, pg)
    async with running(second) as c:
        rows = await by_request(c, "R-52")
        await c.get("/work", headers={"X-Request-Id": "R-52"})
        again = await by_request(c, "R-52")
    assert len(rows) == 2
    assert calls(again)["select count(*) from fleet_orders where user_id = ?"] == 4


async def test_53_shutdown_flushes_and_leaves_the_host_engine_usable(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    app = build_app(engine, pg)
    async with running(app) as c:
        await c.get("/work", headers={"X-Request-Id": "R-53"})
        # no read here: the final flush on shutdown must write it
    async with engine.connect() as conn:
        n = (
            await conn.execute(
                text("SELECT sum(calls) FROM slowquery.request_stats WHERE request_id = 'R-53'")
            )
        ).scalar_one()
        assert n == 3
        assert (await conn.execute(text("SELECT 1"))).scalar_one() == 1


# ---------------------------------------------------------------------------
# LLM fallback wiring — OpenRouter mocked, no live request
# ---------------------------------------------------------------------------


async def test_llm_fallback_gets_a_scrubbed_plan_and_its_suggestion_is_served(
    engine: AsyncEngine, pg: PostgresContainer
) -> None:
    import respx
    from pydantic import SecretStr

    from slowquery_detective import LlmConfig

    answer = {
        "diagnosis": "a sequential scan filters fleet_orders on note",
        "suggestion": "CREATE INDEX IF NOT EXISTS ix_fleet_orders_note ON fleet_orders(note)",
        "confidence": 0.8,
        "kind": "index",
    }
    cfg = LlmConfig(enabled=True, api_key=SecretStr("sk-or-test-key"))
    with respx.mock(assert_all_called=False) as mock:
        route = mock.post("https://openrouter.ai/api/v1/chat/completions").mock(
            return_value=httpx.Response(
                200, json={"choices": [{"message": {"content": json.dumps(answer)}}]}
            )
        )
        app = build_app(engine, pg, threshold_ms=10, enable_llm=True, llm_config=cfg)
        async with running(app) as c:
            await c.get("/slow", headers={"X-Request-Id": "R-llm"})
            (row,) = [r for r in await by_request(c, "R-llm") if "pg_sleep" in r["fingerprint"]]

            async def _has_suggestion() -> bool:
                body = (await c.get(f"/_slowquery/queries/{row['id']}")).json()
                return bool(body["suggestions"])

            await eventually(_has_suggestion)
            detail = (await c.get(f"/_slowquery/queries/{row['id']}")).json()
    assert route.call_count == 1
    sent = route.calls[0].request.content.decode()
    assert SECRET not in sent
    assert "fleet_orders" in sent
    (s,) = detail["suggestions"]
    assert s["source"] == "llm"
    assert s["sql"] == answer["suggestion"]
