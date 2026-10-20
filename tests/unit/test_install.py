"""Spec 07 cases 1-6 — the install() contract."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import slowquery_detective.hooks as hooks_mod
from slowquery_detective import (
    InMemoryStoreWriter,
    LlmConfig,
    PostgresStoreWriter,
    install,
)
from slowquery_detective.tagging import RequestIdMiddleware

# Port 1 is never listening: any accidental connection attempt fails loudly.
UNREACHABLE = "postgresql+asyncpg://u:p@127.0.0.1:1/db"


@pytest.fixture()
def engine() -> Iterator[AsyncEngine]:
    eng = create_async_engine(UNREACHABLE)
    yield eng
    hooks_mod.detach(eng)


async def _allow(_request: Request) -> bool:
    return True


def _kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"service": "paper-trail"}
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# Case 1 — argument contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("over", "exc"),
    [
        ({"service": ""}, ValueError),
        ({"service": "Paper-Trail"}, ValueError),
        ({"service": "-lead"}, ValueError),
        ({"service": "a" * 65}, ValueError),
        ({"service": "has space"}, ValueError),
        ({"threshold_ms": 0}, ValueError),
        ({"threshold_ms": -5}, ValueError),
        ({"threshold_ms": True}, TypeError),
        ({"threshold_ms": 1.5}, TypeError),
        ({"sample_rate": -0.01}, ValueError),
        ({"sample_rate": 1.01}, ValueError),
        ({"sample_rate": float("nan")}, ValueError),
        ({"retention_s": 0}, ValueError),
        ({"retention_s": False}, TypeError),
        ({"retention_s": 60.0}, TypeError),
        ({"store_url": UNREACHABLE, "store": InMemoryStoreWriter()}, ValueError),
        ({"store_url": "sqlite+aiosqlite:///x.db"}, ValueError),
        ({"store_url": "mysql://u:p@h/db"}, ValueError),
        ({"mount_path": ""}, ValueError),
        ({"mount_path": "_slowquery"}, ValueError),
        ({"mount_path": "/_slowquery/"}, ValueError),
        ({"mount_path": "/"}, ValueError),
        ({"mount_path": "/a b"}, ValueError),
        ({"mount_path": "/a//b"}, ValueError),
        ({"authorize": "yes"}, TypeError),
        ({"enable_llm": True}, ValueError),
    ],
)
def test_01_invalid_arguments_are_rejected(
    engine: AsyncEngine, over: dict[str, Any], exc: type[Exception]
) -> None:
    app = FastAPI()
    with pytest.raises(exc):
        install(app, engine, **_kwargs(**over))
    # Nothing was wired on a rejected call.
    assert not getattr(app.state, "slowquery_installed", False)
    assert app.user_middleware == []
    assert not getattr(engine.sync_engine, "_slowquery_attached", False)


def test_01_service_is_required(engine: AsyncEngine) -> None:
    with pytest.raises(TypeError):
        install(FastAPI(), engine)  # type: ignore[call-arg]


def test_01_none_and_wrong_types_for_app_and_engine(engine: AsyncEngine) -> None:
    with pytest.raises(ValueError):
        install(None, engine, service="x")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        install(FastAPI(), None, service="x")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        install(object(), engine, service="x")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        install(FastAPI(), engine.sync_engine, service="x")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Case 2 / 3 — wiring and idempotency
# ---------------------------------------------------------------------------


async def _status(app: FastAPI, path: str) -> int:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            return (await client.get(path)).status_code


async def test_02_valid_install_wires_middleware_routes_and_hooks(engine: AsyncEngine) -> None:
    app = FastAPI()
    install(app, engine, service="paper-trail", authorize=_allow, mount_path="/obs/sq")
    assert app.state.slowquery_installed is True
    assert [m.cls for m in app.user_middleware] == [RequestIdMiddleware]
    assert engine.sync_engine._slowquery_attached is True
    # The library's routes stay out of the host's OpenAPI document.
    assert not any("/obs/sq" in p for p in app.openapi()["paths"])
    assert await _status(app, "/obs/sq/queries") == 200
    assert await _status(app, "/obs/sq/queries/0123456789abcdef") == 404
    assert await _status(app, "/_slowquery/queries") == 404


def test_03_second_install_warns_and_changes_nothing(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    app = FastAPI()
    install(app, engine, service="paper-trail")
    routes_before = len(app.routes)
    listeners = engine.sync_engine._slowquery_listeners
    with caplog.at_level(logging.WARNING, logger="slowquery.middleware"):
        install(app, engine, service="paper-trail", mount_path="/other")
    assert any("already_installed" in r.message for r in caplog.records)
    assert len(app.routes) == routes_before
    assert len(app.user_middleware) == 1
    assert engine.sync_engine._slowquery_listeners is listeners
    before, after, _err = listeners
    assert event.contains(engine.sync_engine, "after_cursor_execute", after)


# ---------------------------------------------------------------------------
# Case 4 — store selection
# ---------------------------------------------------------------------------


def test_04_default_store_is_in_memory(engine: AsyncEngine) -> None:
    app = FastAPI()
    install(app, engine, service="paper-trail")
    assert isinstance(app.state.slowquery_store, InMemoryStoreWriter)


def test_04_injected_store_is_used_as_given(engine: AsyncEngine) -> None:
    app = FastAPI()
    store = InMemoryStoreWriter()
    install(app, engine, service="paper-trail", store=store)
    assert app.state.slowquery_store is store


def test_04_store_url_selects_postgres_without_connecting(engine: AsyncEngine) -> None:
    app = FastAPI()
    install(
        app,
        engine,
        service="paper-trail",
        store_url="postgres://u:p@127.0.0.1:1/db?sslmode=disable",
    )
    store = app.state.slowquery_store
    assert isinstance(store, PostgresStoreWriter)
    assert store.url.startswith("postgresql+asyncpg://")


# ---------------------------------------------------------------------------
# Case 5 — install after start
# ---------------------------------------------------------------------------


async def test_05_install_after_the_app_started_raises(engine: AsyncEngine) -> None:
    app = FastAPI()

    @app.get("/ping")
    async def ping() -> dict[str, str]:
        return {"ok": "1"}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        assert (await client.get("/ping")).status_code == 200
    with pytest.raises(RuntimeError):
        install(app, engine, service="paper-trail")
    assert not getattr(app.state, "slowquery_installed", False)
    assert not getattr(engine.sync_engine, "_slowquery_attached", False)


# ---------------------------------------------------------------------------
# Case 6 — free model defaults
# ---------------------------------------------------------------------------


def test_06_llm_config_defaults_are_the_free_models(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "SLOWQUERY_LLM_MODEL_PRIMARY",
        "SLOWQUERY_LLM_MODEL_FAST",
        "SLOWQUERY_LLM_MODEL_FALLBACK",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = LlmConfig()
    assert cfg.model_primary == "google/gemma-4-26b-a4b-it:free"
    assert cfg.model_fast == "minimax/minimax-m3.1-flash:free"
    assert cfg.model_fallback == "google/gemma-4-31b-it:free"


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def test_public_surface() -> None:
    import slowquery_detective as sd

    assert sd.__version__ == "0.2.1"
    assert set(sd.__all__) == {
        "InMemoryStoreWriter",
        "LlmConfig",
        "PostgresStoreWriter",
        "QueryDetail",
        "QueryRow",
        "StoreWriter",
        "SuggestionOut",
        "__version__",
        "current_request_id",
        "install",
        "request_scope",
    }
    assert not hasattr(sd, "dashboard_router")
