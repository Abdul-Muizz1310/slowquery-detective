"""``install()`` — validation and wiring. See ``docs/specs/07-fleet-observability.md``.

``install(app, engine, service=...)`` checks every argument before it touches
anything, then:

1. picks the store (``store``, else ``PostgresStoreWriter(store_url)``, else
   the in-memory store);
2. builds the ring buffer, recorder, EXPLAIN worker and runtime;
3. adds the request-tagging ASGI middleware;
4. mounts the read routes at ``mount_path`` behind ``authorize``;
5. wraps the app's lifespan so the runtime starts after the host's startup and
   stops before the host's shutdown (Starlette 1.x has no
   ``add_event_handler``; upstreamed from slowquery-demo-backend's shim 1);
6. attaches the SQLAlchemy hooks with ``runtime.observe`` as the callback.

It must run before the app serves its first request or lifespan event.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Final

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

import slowquery_detective.hooks as hooks
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.explain import ExplainerCallable, ExplainWorker
from slowquery_detective.llm_explainer import LlmConfig
from slowquery_detective.llm_explainer import explain as llm_explain
from slowquery_detective.pg_store import PostgresStoreWriter
from slowquery_detective.recorder import Recorder
from slowquery_detective.router import Authorize, build_router
from slowquery_detective.rules import run_rules
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.runtime import Runtime
from slowquery_detective.store import InMemoryStoreWriter, StoreWriter
from slowquery_detective.tagging import RequestIdMiddleware

_LOG = logging.getLogger("slowquery.middleware")

_INSTALLED_ATTR: Final = "slowquery_installed"
_SERVICE_RE: Final = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_MOUNT_RE: Final = re.compile(r"(/[A-Za-z0-9._~-]+)+")


def _require_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _validate(
    app: object,
    engine: object,
    *,
    service: object,
    threshold_ms: object,
    sample_rate: object,
    store_url: str | None,
    store: StoreWriter | None,
    authorize: object,
    mount_path: object,
    retention_s: object,
    enable_llm: bool,
    llm_config: LlmConfig | None,
) -> None:
    if app is None:
        raise ValueError("app must not be None")
    if not isinstance(app, FastAPI):
        raise TypeError("app must be a fastapi.FastAPI")
    if engine is None:
        raise ValueError("engine must not be None")
    if not isinstance(engine, AsyncEngine):
        raise TypeError("engine must be a sqlalchemy.ext.asyncio.AsyncEngine")
    if not isinstance(service, str) or _SERVICE_RE.fullmatch(service) is None:
        raise ValueError("service must match [a-z0-9][a-z0-9_-]{0,63}")
    _require_int("threshold_ms", threshold_ms)
    _require_int("retention_s", retention_s)
    if (
        isinstance(sample_rate, bool)
        or not isinstance(sample_rate, int | float)
        or not math.isfinite(sample_rate)
        or not 0.0 <= sample_rate <= 1.0
    ):
        raise ValueError("sample_rate must be in [0.0, 1.0]")
    if store_url is not None and store is not None:
        raise ValueError("pass store_url or store, not both")
    if store is not None and not isinstance(store, StoreWriter):
        raise TypeError("store must be a StoreWriter")
    if not isinstance(mount_path, str) or _MOUNT_RE.fullmatch(mount_path) is None:
        raise ValueError("mount_path must look like /_slowquery (leading /, no trailing /)")
    if authorize is not None and not callable(authorize):
        raise TypeError("authorize must be an async callable taking the Request, or None")
    if enable_llm and llm_config is None:
        raise ValueError("enable_llm=True requires llm_config")


class _LlmClient:
    """The keep-alive HTTP client for OpenRouter, one per lifespan.

    One client is reused across cascades instead of a new TLS handshake per
    fallback. It is closed at shutdown and opened again at the next startup,
    so an app whose lifespan runs more than once keeps its LLM fallback.
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    def open(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient()
        return self._client

    @property
    def client(self) -> httpx.AsyncClient:
        return self.open()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def _llm(
    enable_llm: bool, llm_config: LlmConfig | None
) -> tuple[ExplainerCallable | None, _LlmClient | None]:
    if not enable_llm or llm_config is None:
        return None, None
    cfg = llm_config
    holder = _LlmClient()

    async def _explainer(
        canonical_sql: str, plan_json: dict[str, Any], *, fingerprint_id: str
    ) -> Suggestion | None:
        return await llm_explain(
            canonical_sql,
            plan_json,
            config=cfg,
            fingerprint_id=fingerprint_id,
            client=holder.client,
        )

    return _explainer, holder


def _wrap_lifespan(app: FastAPI, runtime: Runtime) -> None:
    host: Callable[[Any], AbstractAsyncContextManager[Any]] = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app_: Any) -> AsyncIterator[Any]:
        async with host(app_) as state:
            await runtime.start()
            try:
                yield state
            finally:
                await runtime.stop()

    app.router.lifespan_context = lifespan


def install(
    app: FastAPI,
    engine: AsyncEngine,
    *,
    service: str,
    threshold_ms: int = 100,
    sample_rate: float = 1.0,
    store_url: str | None = None,
    store: StoreWriter | None = None,
    authorize: Authorize | None = None,
    mount_path: str = "/_slowquery",
    retention_s: int = 604_800,
    enable_llm: bool = False,
    llm_config: LlmConfig | None = None,
) -> None:
    """Attach slowquery-detective to a FastAPI app and its async engine.

    Fleet integration::

        from slowquery_detective import install
        install(app, engine, service="paper-trail",
                store_url=settings.database_url, authorize=verify_platform_token)
    """
    _validate(
        app,
        engine,
        service=service,
        threshold_ms=threshold_ms,
        sample_rate=sample_rate,
        store_url=store_url,
        store=store,
        authorize=authorize,
        mount_path=mount_path,
        retention_s=retention_s,
        enable_llm=enable_llm,
        llm_config=llm_config,
    )
    if getattr(app.state, _INSTALLED_ATTR, False):
        _LOG.warning("slowquery.middleware.already_installed")
        return
    if app.middleware_stack is not None:
        raise RuntimeError("install() must run before the app serves its first request")

    chosen: StoreWriter
    if store is not None:
        chosen = store
    elif store_url is not None:
        chosen = PostgresStoreWriter(store_url)
    else:
        chosen = InMemoryStoreWriter()

    buffer = RingBuffer()
    explainer, llm_client = _llm(enable_llm, llm_config)
    worker = ExplainWorker(engine=engine, store=chosen, rules=run_rules, explainer=explainer)

    def _on_start() -> None:
        # A lifespan that ran before detached the hooks and closed the client.
        if not hooks.is_attached(engine):
            hooks.attach(engine, buffer, sample_rate=float(sample_rate), on_record=runtime.observe)
        if llm_client is not None:
            llm_client.open()

    async def _on_stop() -> None:
        hooks.detach(engine)
        buffer.clear()
        if llm_client is not None:
            await llm_client.aclose()

    runtime = Runtime(
        service=service,
        store=chosen,
        recorder=Recorder(threshold_ms=threshold_ms),
        buffer=buffer,
        worker=worker,
        retention_s=retention_s,
        on_start=_on_start,
        on_stop=_on_stop,
    )

    app.add_middleware(RequestIdMiddleware, exclude_prefix=mount_path)
    app.include_router(build_router(runtime, authorize), prefix=mount_path)
    _wrap_lifespan(app, runtime)
    hooks.attach(engine, buffer, sample_rate=float(sample_rate), on_record=runtime.observe)

    app.state.slowquery_runtime = runtime
    app.state.slowquery_store = chosen
    app.state.slowquery_worker = worker
    app.state.slowquery_buffer = buffer
    setattr(app.state, _INSTALLED_ATTR, True)
