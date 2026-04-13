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
import json
import logging
import os
import re
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text

from slowquery_detective.explain import ExplainJob, synthesize_params
from slowquery_detective.fingerprint import fingerprint as fingerprint_fn
from slowquery_detective.rules import run_rules

_LOG = logging.getLogger("slowquery.dashboard")

# Only ``CREATE INDEX [CONCURRENTLY] IF NOT EXISTS ix_...`` is ever executable
# via ``POST /api/queries/{id}/apply``. Anything else returns 400.
DDL_ALLOWLIST_REGEX: re.Pattern[str] = re.compile(
    r"^CREATE INDEX( CONCURRENTLY)? IF NOT EXISTS "
    r'ix_[A-Za-z0-9_]+ ON [A-Za-z0-9_"]+\s*\('
    r"[A-Za-z0-9_,\s()]+\);?$"
)


def _is_demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "").lower() == "true"


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
        if not _is_demo_mode():
            raise HTTPException(status_code=403, detail="Forbidden outside demo mode")

    # ---------------------------------------------------------------
    # GET /api/queries
    # ---------------------------------------------------------------
    @router.get("/api/queries")
    async def list_queries(request: Request) -> Any:
        _check_auth(request)
        worker = request.app.state.slowquery_worker
        buf = request.app.state.slowquery_buffer

        results: list[dict[str, Any]] = []
        for fid in buf.keys():
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

        if fingerprint_id not in buf.keys():
            raise HTTPException(status_code=404, detail="Fingerprint not found")

        cached = worker.plan_cache_get(fingerprint_id)
        plan: dict[str, Any] = {}
        suggestions: list[dict[str, Any]] = []

        if cached is not None:
            plan = cached.plan_json
            suggestions = [s.model_dump() for s in cached.suggestions]

        return {
            "fingerprint_id": fingerprint_id,
            "plan": plan,
            "suggestions": suggestions,
        }

    # ---------------------------------------------------------------
    # On-demand EXPLAIN + suggestion helper
    # ---------------------------------------------------------------
    _WHERE_COL_RE = re.compile(
        r"\bwhere\s+(?:\"?(\w+)\"?\.)?\"?(\w+)\"?\s*(?:=|>|<|>=|<=|!=|<>|in|like)",
        re.IGNORECASE,
    )
    _FROM_TABLE_RE = re.compile(
        r"\bfrom\s+\"?(\w+)\"?",
        re.IGNORECASE,
    )

    async def _get_or_generate_suggestion(
        fingerprint_id: str, worker: Any, buf: Any, request: Request
    ) -> Any:
        """Return a CachedPlan with suggestions, generating one on-the-fly if needed.

        If the rules engine produced suggestions, return the cached plan.
        Otherwise, generate a best-effort index suggestion from the
        canonical SQL's WHERE clause.
        """
        from slowquery_detective.explain import CachedPlan

        sql_cache = getattr(request.app.state, "slowquery_canonical_sql_cache", {})

        # Process any pending jobs.
        try:
            while not worker._queue.empty():
                job = worker._queue.get_nowait()
                sql_cache[job.fingerprint_id] = job.canonical_sql
                await worker._process_one(job)
        except Exception:
            pass

        cached = worker.plan_cache_get(fingerprint_id)
        if cached is not None and cached.suggestions:
            return cached

        # Generate a best-effort index suggestion from the canonical SQL.
        canonical_sql = sql_cache.get(fingerprint_id)
        if canonical_sql is None:
            return cached

        match = _WHERE_COL_RE.search(canonical_sql)
        table_match = _FROM_TABLE_RE.search(canonical_sql)
        if match and table_match:
            col = match.group(2)
            table = table_match.group(1)
            from slowquery_detective.rules.base import IDENTIFIER_RE, Suggestion

            if IDENTIFIER_RE.match(col) and IDENTIFIER_RE.match(table):
                ddl_sql = f"CREATE INDEX IF NOT EXISTS ix_{table}_{col} ON {table}({col});"
                suggestion = Suggestion(
                    kind="index",
                    sql=ddl_sql,
                    rationale=f"Index on {table}.{col} for WHERE clause",
                    confidence=0.7,
                    source="rules",
                    rule_name="on_demand_index",
                )
                plan_json = cached.plan_json if cached else {}
                return CachedPlan(
                    plan_json=plan_json,
                    plan_text="",
                    cost=0.0,
                    captured_at=time.monotonic(),
                    suggestions=(suggestion,),
                )

        return cached

    # ---------------------------------------------------------------
    # POST /api/queries/{fingerprint_id}/apply
    # ---------------------------------------------------------------
    @router.post("/api/queries/{fingerprint_id}/apply")
    async def apply_ddl(fingerprint_id: str, request: Request) -> Any:
        _check_auth(request)
        worker = request.app.state.slowquery_worker
        buf = request.app.state.slowquery_buffer
        engine = getattr(request.app.state, "slowquery_engine", None)

        # Parse optional body.
        body: _ApplyRequest | None = None
        try:
            raw = await request.json()
            body = _ApplyRequest.model_validate(raw)
        except Exception:
            pass

        # Determine the DDL to execute.
        ddl: str | None = None
        if body is not None and body.sql is not None:
            ddl = body.sql
        else:
            if fingerprint_id not in buf.keys():
                raise HTTPException(status_code=404, detail="Unknown fingerprint")

            # Look up suggestions — from cache or generated on-the-fly.
            cached = await _get_or_generate_suggestion(
                fingerprint_id, worker, buf, request
            )
            if cached is not None:
                for s in cached.suggestions:
                    if s.sql and DDL_ALLOWLIST_REGEX.match(s.sql.strip()):
                        ddl = s.sql.strip()
                        break
            if ddl is None:
                raise HTTPException(status_code=404, detail="No applicable DDL suggestion")

        # Validate against the allowlist.
        ddl_stripped = ddl.strip()
        if not DDL_ALLOWLIST_REGEX.match(ddl_stripped):
            raise HTTPException(status_code=400, detail="DDL not on allowlist")

        # Rate limit per fingerprint (scoped to this app instance).
        apply_ts: dict[str, float] = getattr(
            request.app.state, "_slowquery_apply_timestamps", {}
        )
        if not hasattr(request.app.state, "_slowquery_apply_timestamps"):
            request.app.state._slowquery_apply_timestamps = apply_ts
        now = time.monotonic()
        last = apply_ts.get(fingerprint_id)
        if last is not None and (now - last) < _APPLY_COOLDOWN:
            raise HTTPException(status_code=429, detail="Rate limited")

        # Execute the DDL. Use AUTOCOMMIT isolation for CONCURRENTLY
        # statements which cannot run inside a transaction block.
        try:
            async_engine = worker._engine
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
        worker = request.app.state.slowquery_worker

        async def _event_generator():
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
