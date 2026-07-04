"""Dashboard API router — see ``docs/specs/05-middleware.md``.

Provides the HTTP surface for the slowquery-detective dashboard:

- ``GET /api/queries`` — list observed fingerprints with summary stats.
- ``GET /api/queries/{fingerprint_id}`` — detail view with plan + suggestions.
- ``POST /api/queries/{fingerprint_id}/apply`` — execute an allowlisted DDL.
- ``GET /api/stream`` — SSE stream of newly observed fingerprints.

The DDL allowlist regex lives here as a module-level constant so the rules
engine, the middleware, and the red tests all share exactly one definition.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import re
import time
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text

_LOG = logging.getLogger("slowquery.dashboard")

# Only ``CREATE INDEX [CONCURRENTLY] IF NOT EXISTS ix_...`` is ever executable
# via ``POST /api/queries/{id}/apply``. Anything else returns 400.
DDL_ALLOWLIST_REGEX: re.Pattern[str] = re.compile(
    r"^CREATE INDEX( CONCURRENTLY)? IF NOT EXISTS "
    r'ix_[A-Za-z0-9_]+ ON [A-Za-z0-9_"]+\s*\('
    r"[A-Za-z0-9_,\s()]+\);?\Z"  # \Z not $ — prevents newline injection
)


def _is_demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "").lower() == "true"


def _expected_token() -> str | None:
    """The configured dashboard token, or ``None`` when auth is not enforced."""
    tok = os.environ.get("SLOWQUERY_PLATFORM_TOKEN", "").strip()
    return tok or None


# Extract the target table + column expression from an already-allowlisted
# ``CREATE INDEX ... ON <table>(<cols>)`` statement.
_INDEX_TARGET_RE = re.compile(
    r"\bon\s+\"?(?P<table>[A-Za-z0-9_]+)\"?\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE,
)
_IDENTIFIER_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _identifier_tokens(text_value: str) -> set[str]:
    return {m.group(0).lower() for m in _IDENTIFIER_TOKEN_RE.finditer(text_value)}


def _ddl_bound_to_fingerprint(ddl: str, canonical_sql: str | None) -> bool:
    """True iff every table/column identifier in ``ddl`` appears in the SQL.

    Binds a body-supplied ``CREATE INDEX`` to the fingerprint it claims to
    optimize: an anonymous caller can't force ``CREATE INDEX ... ON any_table``
    unless that table and its columns actually appear in the fingerprint's
    canonical SQL (audit HIGH — no binding between DDL and fingerprint).
    """
    if not canonical_sql:
        return False
    m = _INDEX_TARGET_RE.search(ddl)
    if m is None:
        return False
    target_ids = _identifier_tokens(m.group("table")) | _identifier_tokens(m.group("cols"))
    if not target_ids:
        return False
    return target_ids.issubset(_identifier_tokens(canonical_sql))


class _ApplyRequest(BaseModel):
    sql: str | None = None


def _build_router() -> APIRouter:
    """Construct the APIRouter exposed by the package."""
    router = APIRouter()

    _APPLY_COOLDOWN = 5.0  # seconds

    # ---------------------------------------------------------------
    # Auth guard
    # ---------------------------------------------------------------
    def _check_auth(request: Request) -> None:
        """Per-request auth.

        If a dashboard token is configured (``SLOWQUERY_PLATFORM_TOKEN``),
        every request must carry a matching ``X-Platform-Token`` header —
        this is what lets a *public* demo (which must run with
        ``DEMO_MODE=true`` to expose ``/apply``) still refuse anonymous
        callers. When no token is configured, fall back to the ``DEMO_MODE``
        gate (403 outside demo mode) for local development.
        """
        expected = _expected_token()
        if expected is not None:
            provided = request.headers.get("x-platform-token")
            if not provided or not hmac.compare_digest(provided, expected):
                raise HTTPException(
                    status_code=401,
                    detail="Missing or invalid X-Platform-Token",
                )
            return
        if not _is_demo_mode():
            raise HTTPException(status_code=403, detail="Forbidden outside demo mode")

    # ---------------------------------------------------------------
    # GET /api/queries
    # ---------------------------------------------------------------
    @router.get("/api/queries")
    async def list_queries(request: Request) -> Any:
        _check_auth(request)
        buf = request.app.state.slowquery_buffer

        results: list[dict[str, Any]] = []
        for fid in buf:
            p = buf.percentiles(fid)
            entry: dict[str, Any] = {"fingerprint_id": fid}
            if p is not None:
                entry["sample_count"] = p.sample_count
                entry["p50_ms"] = p.p50_ms
                entry["p95_ms"] = p.p95_ms
                entry["p99_ms"] = p.p99_ms
                entry["max_ms"] = p.max_ms
            results.append(entry)
        return results

    # ---------------------------------------------------------------
    # GET /api/queries/{fingerprint_id}
    # ---------------------------------------------------------------
    @router.get("/api/queries/{fingerprint_id}")
    async def query_detail(fingerprint_id: str, request: Request) -> Any:
        _check_auth(request)
        worker = request.app.state.slowquery_worker
        buf = request.app.state.slowquery_buffer

        if fingerprint_id not in buf:
            raise HTTPException(status_code=404, detail="Fingerprint not found")

        cached = worker.plan_cache_get(fingerprint_id)
        plan: dict[str, Any] = {}
        suggestions: list[dict[str, Any]] = []

        if cached is not None:
            plan = cached.plan_json
            suggestions = [s.model_dump() for s in cached.suggestions]

        # Include the fingerprint's current percentile summary so the detail
        # response honors the documented "plan + suggestions + samples" shape.
        # Only aggregate percentiles are exposed — never raw literals (the
        # buffer never stores them anyway).
        percentiles: dict[str, Any] | None = None
        p = buf.percentiles(fingerprint_id)
        if p is not None:
            percentiles = {
                "sample_count": p.sample_count,
                "p50_ms": p.p50_ms,
                "p95_ms": p.p95_ms,
                "p99_ms": p.p99_ms,
                "max_ms": p.max_ms,
            }

        return {
            "fingerprint_id": fingerprint_id,
            "plan": plan,
            "suggestions": suggestions,
            "percentiles": percentiles,
        }

    # ---------------------------------------------------------------
    # Cached-suggestion helper (rules-engine output only)
    # ---------------------------------------------------------------
    async def _cached_suggestion_ddl(fingerprint_id: str, worker: Any) -> str | None:
        """Return the first allowlisted DDL suggestion for a fingerprint.

        Flushes any queued EXPLAIN jobs via the worker's public
        ``process_pending`` (no reaching into private internals) so a
        freshly-observed query surfaces its rules-engine suggestion. Only
        suggestions produced by the tested, plan-aware rules engine are
        eligible — there is no free-form regex fallback (audit MEDIUM: the
        on-demand fallback bound WHERE columns to the wrong table on JOINs).
        """
        try:
            await worker.process_pending()
        except Exception:
            _LOG.debug("slowquery.dashboard.process_pending_error", exc_info=True)

        cached = worker.plan_cache_get(fingerprint_id)
        if cached is None:
            return None
        for s in cached.suggestions:
            if s.sql and DDL_ALLOWLIST_REGEX.match(s.sql.strip()):
                return str(s.sql.strip())
        return None

    # ---------------------------------------------------------------
    # POST /api/queries/{fingerprint_id}/apply
    # ---------------------------------------------------------------
    @router.post("/api/queries/{fingerprint_id}/apply")
    async def apply_ddl(fingerprint_id: str, request: Request) -> Any:
        _check_auth(request)
        worker = request.app.state.slowquery_worker
        buf = request.app.state.slowquery_buffer
        sql_cache: dict[str, str] = getattr(request.app.state, "slowquery_canonical_sql_cache", {})

        # Parse optional body.
        body: _ApplyRequest | None = None
        try:
            raw = await request.json()
            body = _ApplyRequest.model_validate(raw)
        except Exception:
            pass

        ddl: str | None = None
        if body is not None and body.sql is not None:
            # Body-supplied DDL: validate the allowlist *first* so malformed /
            # destructive DDL is rejected (400) regardless of fingerprint, then
            # bind it to the fingerprint's actual query.
            ddl_candidate = body.sql.strip()
            if "\n" in ddl_candidate or "\r" in ddl_candidate:
                raise HTTPException(status_code=400, detail="DDL not on allowlist")
            if not DDL_ALLOWLIST_REGEX.match(ddl_candidate):
                raise HTTPException(status_code=400, detail="DDL not on allowlist")
            if fingerprint_id not in buf:
                raise HTTPException(status_code=404, detail="Unknown fingerprint")
            if not _ddl_bound_to_fingerprint(ddl_candidate, sql_cache.get(fingerprint_id)):
                raise HTTPException(
                    status_code=400,
                    detail="DDL does not correspond to this fingerprint",
                )
            ddl = ddl_candidate
        else:
            if fingerprint_id not in buf:
                raise HTTPException(status_code=404, detail="Unknown fingerprint")
            ddl = await _cached_suggestion_ddl(fingerprint_id, worker)
            if ddl is None:
                raise HTTPException(status_code=404, detail="No applicable DDL suggestion")

        # Final allowlist re-check (defense in depth) — reject newlines first.
        ddl_stripped = ddl.strip()
        if "\n" in ddl_stripped or "\r" in ddl_stripped:
            raise HTTPException(status_code=400, detail="DDL not on allowlist")
        if not DDL_ALLOWLIST_REGEX.match(ddl_stripped):
            raise HTTPException(status_code=400, detail="DDL not on allowlist")

        # Rate limit per fingerprint (scoped to this app instance).
        apply_ts: dict[str, float] = getattr(request.app.state, "_slowquery_apply_timestamps", {})
        if not hasattr(request.app.state, "_slowquery_apply_timestamps"):
            request.app.state._slowquery_apply_timestamps = apply_ts
        now = time.monotonic()
        last = apply_ts.get(fingerprint_id)
        if last is not None and (now - last) < _APPLY_COOLDOWN:
            raise HTTPException(status_code=429, detail="Rate limited")

        # Execute the DDL. Use AUTOCOMMIT isolation for CONCURRENTLY
        # statements which cannot run inside a transaction block.
        try:
            async_engine = worker.engine
            is_concurrent = "CONCURRENTLY" in ddl_stripped.upper()
            if is_concurrent:
                async with async_engine.execution_options(
                    isolation_level="AUTOCOMMIT"
                ).connect() as conn:
                    await conn.execute(text(ddl_stripped))
            else:
                async with async_engine.connect() as conn:
                    await conn.execute(text(ddl_stripped))
                    await conn.commit()
        except Exception as exc:
            _LOG.error("slowquery.dashboard.apply_error", exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        apply_ts[fingerprint_id] = now
        return {"executed_sql": ddl_stripped, "status": "ok"}

    # ---------------------------------------------------------------
    # GET /api/stream — SSE
    # ---------------------------------------------------------------
    @router.get("/api/stream")
    async def sse_stream(request: Request) -> StreamingResponse:
        _check_auth(request)
        buf = request.app.state.slowquery_buffer

        async def _event_generator() -> AsyncGenerator[str, None]:
            seen: set[str] = set(buf.keys())
            while True:
                current = buf.keys()
                new_keys = current - seen
                for fid in new_keys:
                    p = buf.percentiles(fid)
                    data: dict[str, Any] = {"fingerprint_id": fid}
                    if p is not None:
                        data["sample_count"] = p.sample_count
                        data["p50_ms"] = p.p50_ms
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"
                seen = set(current)
                await asyncio.sleep(0.1)

        return StreamingResponse(
            _event_generator(),
            media_type="text/event-stream",
        )

    return router


dashboard_router: APIRouter = _build_router()
