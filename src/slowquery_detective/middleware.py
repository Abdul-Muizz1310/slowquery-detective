"""FastAPI integration — see ``docs/specs/05-middleware.md``.

``install(app, engine)`` wires every component in this package onto a
FastAPI app + SQLAlchemy engine:

1. Construct a :class:`RingBuffer`.
2. ``hooks.attach(engine, buffer, sample_rate=...)``.
3. Construct :class:`ExplainWorker`, start it on app startup, stop on
   shutdown.
4. Register shutdown handlers that detach the hooks and stop the worker.

The full dashboard router + HTTP surface is exercised by
``tests/integration/test_middleware.py`` — the unit tests here cover
argument validation only.
"""

from __future__ import annotations

import logging
from typing import Any

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.explain import ExplainWorker
from slowquery_detective.hooks import attach, detach
from slowquery_detective.llm_explainer import LlmConfig
from slowquery_detective.llm_explainer import explain as llm_explain
from slowquery_detective.rules import run_rules
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

_LOG = logging.getLogger("slowquery.middleware")

_INSTALLED_ATTR = "_slowquery_installed"


def install(
    app: Any,
    engine: Any,
    *,
    threshold_ms: int = 100,
    sample_rate: float = 1.0,
    store_url: str | None = None,
    enable_llm: bool = False,
    llm_config: LlmConfig | None = None,
) -> None:
    """Attach slowquery-detective to a FastAPI app + SQLAlchemy engine.

    3-line integration::

        from slowquery_detective import install
        install(app, engine)
    """
    if app is None:
        raise ValueError("app must not be None")
    if engine is None:
        raise ValueError("engine must not be None")
    if threshold_ms <= 0:
        raise ValueError("threshold_ms must be > 0")
    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be in [0.0, 1.0]")
    if enable_llm and llm_config is None:
        raise ValueError("enable_llm=True requires llm_config")

    # Idempotent install.
    if getattr(app.state, _INSTALLED_ATTR, False):
        _LOG.warning("slowquery.middleware.already_installed")
        return

    buffer = RingBuffer()
    attach(engine, buffer, sample_rate=sample_rate)

    store = StoreWriter(store_url or _engine_url(engine))

    def _rules_adapter(plan: dict[str, Any], canonical_sql: str) -> list[Suggestion]:
        return run_rules(plan, canonical_sql, fingerprint_id="")

    explainer = None
    if enable_llm and llm_config is not None:
        cfg = llm_config  # capture for closure

        async def _explainer(
            canonical_sql: str,
            plan_json: dict[str, Any],
            *,
            fingerprint_id: str,
        ) -> Suggestion | None:
            return await llm_explain(
                canonical_sql,
                plan_json,
                config=cfg,
                fingerprint_id=fingerprint_id,
            )

        explainer = _explainer

    worker = ExplainWorker(
        engine=engine,
        store=store,
        rules=_rules_adapter,
        explainer=explainer,
    )

    app.state.slowquery_buffer = buffer
    app.state.slowquery_store = store
    app.state.slowquery_worker = worker
    app.state.slowquery_threshold_ms = threshold_ms
    setattr(app.state, _INSTALLED_ATTR, True)

    # FastAPI's decorator form is untyped under mypy-strict; use
    # ``add_event_handler`` (also public) to avoid ``# type: ignore``.
    async def _on_startup() -> None:
        await worker.start()

    async def _on_shutdown() -> None:
        detach(engine)
        await worker.stop()
        buffer.clear()

    app.add_event_handler("startup", _on_startup)
    app.add_event_handler("shutdown", _on_shutdown)


def _engine_url(engine: Any) -> str:
    """Return the engine's connection URL as a string (best effort)."""
    url = getattr(engine, "url", None)
    return str(url) if url is not None else ""
