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
import time
from typing import Any

import slowquery_detective.hooks as _hooks_mod
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.explain import ExplainJob, ExplainWorker
from slowquery_detective.hooks import attach
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

    # Cache of fingerprint_id -> canonical_sql for on-demand suggestion
    # generation in the dashboard router.
    canonical_sql_cache: dict[str, str] = {}

    def _on_record(fp_id: str, canonical_sql: str, duration_ms: float) -> None:
        """Submit an explain job for every observed query.

        The worker's per-fingerprint cooldown ensures we don't re-run EXPLAIN
        for the same fingerprint more than once per cooldown window.
        """
        canonical_sql_cache[fp_id] = canonical_sql
        job = ExplainJob(
            fingerprint_id=fp_id,
            canonical_sql=canonical_sql,
            observed_ms=duration_ms,
            enqueued_at=time.monotonic(),
        )
        worker.submit(job)

    attach(engine, buffer, sample_rate=sample_rate, on_record=_on_record)

    app.state.slowquery_buffer = buffer
    app.state.slowquery_store = store
    app.state.slowquery_worker = worker
    app.state.slowquery_engine = engine
    app.state.slowquery_threshold_ms = threshold_ms
    app.state.slowquery_canonical_sql_cache = canonical_sql_cache
    setattr(app.state, _INSTALLED_ATTR, True)

    # Start the worker eagerly so it processes explain jobs immediately.
    # Also register startup/shutdown handlers for apps that use lifespan.
    import asyncio

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(worker.start())
    except RuntimeError:
        pass  # no running loop; worker will be started on startup

    async def _on_startup() -> None:
        await worker.start()

    async def _on_shutdown() -> None:
        _hooks_mod.detach(engine)
        await worker.stop()
        buffer.clear()

    app.router.add_event_handler("startup", _on_startup)
    app.router.add_event_handler("shutdown", _on_shutdown)


def _engine_url(engine: Any) -> str:
    """Return the engine's connection URL as a string (best effort)."""
    url = getattr(engine, "url", None)
    return str(url) if url is not None else ""
