"""Additional unit tests for middleware.py — coverage of install() body.

Requires a Starlette compatibility shim for ``add_event_handler`` which
was removed in Starlette 1.0. The library's ``install()`` calls that
method, so we patch it for these tests.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from starlette.applications import Starlette

from slowquery_detective.middleware import _engine_url

# ---------------------------------------------------------------------------
# Starlette compat shim (same approach as demo-backend's observability.py)
# ---------------------------------------------------------------------------


def _ensure_add_event_handler() -> None:
    """Add the shim if Starlette doesn't have add_event_handler."""
    if not hasattr(Starlette, "add_event_handler"):

        def _compat(self: Starlette, event_type: str, func: Any) -> None:
            if event_type == "startup":
                self.router.on_startup.append(func)
            elif event_type == "shutdown":
                self.router.on_shutdown.append(func)

        Starlette.add_event_handler = _compat  # type: ignore[attr-defined]


_ensure_add_event_handler()


# ---------------------------------------------------------------------------
# _engine_url helper
# ---------------------------------------------------------------------------


def test_engine_url_returns_str_of_url() -> None:
    engine = MagicMock()
    engine.url = "sqlite:///:memory:"
    assert _engine_url(engine) == "sqlite:///:memory:"


def test_engine_url_no_url_attr_returns_empty_string() -> None:
    assert _engine_url(object()) == ""


def test_engine_url_url_is_none() -> None:
    engine = MagicMock()
    engine.url = None
    assert _engine_url(engine) == ""


# ---------------------------------------------------------------------------
# install() happy path with mock engine
# ---------------------------------------------------------------------------


def _mock_engine() -> MagicMock:
    """Return a mock engine that install() can attach to."""
    engine = MagicMock()
    engine.sync_engine = engine  # sync_engine returns self (like a sync engine)
    engine._slowquery_attached = False
    return engine


def test_install_attaches_state_to_app() -> None:
    """install() should attach buffer, store, worker, threshold to app.state."""
    from slowquery_detective import install

    engine = _mock_engine()
    app = FastAPI()

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine)

    assert hasattr(app.state, "slowquery_buffer")
    assert hasattr(app.state, "slowquery_store")
    assert hasattr(app.state, "slowquery_worker")
    assert hasattr(app.state, "slowquery_threshold_ms")
    assert app.state.slowquery_threshold_ms == 100


def test_install_custom_threshold() -> None:
    from slowquery_detective import install

    engine = _mock_engine()
    app = FastAPI()

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine, threshold_ms=500)

    assert app.state.slowquery_threshold_ms == 500


def test_install_idempotent(caplog: Any) -> None:
    """Second install on the same app warns and does nothing."""
    from slowquery_detective import install

    engine = _mock_engine()
    app = FastAPI()

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine)
        install(app, engine)

    assert any("already_installed" in r.message for r in caplog.records)


def test_install_with_store_url() -> None:
    from slowquery_detective import install

    engine = _mock_engine()
    app = FastAPI()

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine, store_url="postgresql://localhost:5432/test")

    assert app.state.slowquery_store is not None


def test_install_registers_startup_and_shutdown_handlers() -> None:
    from slowquery_detective import install

    engine = _mock_engine()
    app = FastAPI()
    startup_before = len(app.router.on_startup)
    shutdown_before = len(app.router.on_shutdown)

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine)

    assert len(app.router.on_startup) > startup_before
    assert len(app.router.on_shutdown) > shutdown_before


def test_install_with_llm_config() -> None:
    """install() with enable_llm=True and a config wires the explainer."""
    from slowquery_detective import install
    from slowquery_detective.llm_explainer import LlmConfig

    engine = _mock_engine()
    app = FastAPI()
    config = LlmConfig(
        enabled=True,
        api_key="test-key",
        model_primary="test-model",
        model_fast="test-fast",
        model_fallback="test-fallback",
    )

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine, enable_llm=True, llm_config=config)

    assert app.state.slowquery_worker is not None


# ---------------------------------------------------------------------------
# Shutdown handler
# ---------------------------------------------------------------------------


async def test_shutdown_handler_detaches_and_stops_worker() -> None:
    from slowquery_detective import install

    engine = _mock_engine()
    app = FastAPI()

    with patch("slowquery_detective.middleware.attach"):
        install(app, engine)

    # Execute shutdown handlers
    with patch("slowquery_detective.hooks.detach"):
        for handler in app.router.on_shutdown:
            await handler()

    # Buffer should be cleared
    assert app.state.slowquery_buffer.keys() == frozenset()
