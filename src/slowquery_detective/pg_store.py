"""Postgres store — spec 07 §Stores, cases 16-22 and 34-42.

Upstreamed from slowquery-demo-backend's ``PostgresStoreWriter`` (its
DEVIATIONS §8) and reshaped for the fleet:

- its **own** async engine (small pool, pre-ping, recycle; asyncpg's statement
  cache off behind PgBouncer), never instrumented by the hooks;
- schema ``slowquery``, created at startup by idempotent ``CREATE … IF NOT
  EXISTS`` DDL under a transaction-scoped advisory lock, so a boot that runs it
  twice, or two processes that boot together, both succeed;
- batched, additive upserts in one transaction per flush, rows in key order;
- no foreign keys: plans and suggestions may arrive before a fingerprint's
  first flush.

Every statement lives in a module-level constant.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Final

from pydantic import JsonValue
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from slowquery_detective.recorder import Batch
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.schemas import (
    ExplainPlanOut,
    QueryDetail,
    QueryRow,
    SampleOut,
    SuggestionOut,
)
from slowquery_detective.store import RECENT_SAMPLES, StoreWriter, to_datetime
from slowquery_detective.store_url import normalise_store_url

SCHEMA_VERSION: Final = 1
# pg_advisory_xact_lock key: "slow" in ASCII.
_LOCK_KEY: Final = 0x736C6F77

_SQL_SCHEMA_PRESENT: Final = (
    "SELECT to_regclass('slowquery.query_fingerprints') IS NOT NULL"
    " AND to_regclass('slowquery.request_stats') IS NOT NULL"
    " AND to_regclass('slowquery.query_samples') IS NOT NULL"
    " AND to_regclass('slowquery.explain_plans') IS NOT NULL"
    " AND to_regclass('slowquery.suggestions') IS NOT NULL"
    " AND to_regclass('slowquery.schema_version') IS NOT NULL"
)
_SQL_LOCK: Final = "SELECT pg_advisory_xact_lock(:key)"

DDL: Final[tuple[str, ...]] = (
    "CREATE SCHEMA IF NOT EXISTS slowquery",
    """CREATE TABLE IF NOT EXISTS slowquery.query_fingerprints (
        service text NOT NULL,
        id text NOT NULL CHECK (id ~ '^[0-9a-f]{16}$'),
        canonical_sql text NOT NULL,
        first_seen timestamptz NOT NULL,
        last_seen timestamptz NOT NULL,
        call_count bigint NOT NULL CHECK (call_count > 0),
        total_ms double precision NOT NULL CHECK (total_ms >= 0),
        max_ms double precision NOT NULL CHECK (max_ms >= 0),
        p50_ms double precision,
        p95_ms double precision,
        p99_ms double precision,
        PRIMARY KEY (service, id)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_query_fingerprints_service_last_seen"
    " ON slowquery.query_fingerprints (service, last_seen)",
    """CREATE TABLE IF NOT EXISTS slowquery.request_stats (
        service text NOT NULL,
        request_id text NOT NULL,
        fingerprint_id text NOT NULL,
        calls bigint NOT NULL CHECK (calls > 0),
        total_ms double precision NOT NULL CHECK (total_ms >= 0),
        max_ms double precision NOT NULL CHECK (max_ms >= 0),
        first_seen timestamptz NOT NULL,
        last_seen timestamptz NOT NULL,
        PRIMARY KEY (service, request_id, fingerprint_id)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_request_stats_service_last_seen"
    " ON slowquery.request_stats (service, last_seen)",
    """CREATE TABLE IF NOT EXISTS slowquery.query_samples (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        service text NOT NULL,
        fingerprint_id text NOT NULL,
        request_id text,
        duration_ms double precision NOT NULL CHECK (duration_ms >= 0),
        sampled_at timestamptz NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS ix_query_samples_service_fp_sampled_at"
    " ON slowquery.query_samples (service, fingerprint_id, sampled_at DESC)",
    "CREATE INDEX IF NOT EXISTS ix_query_samples_service_sampled_at"
    " ON slowquery.query_samples (service, sampled_at)",
    """CREATE TABLE IF NOT EXISTS slowquery.explain_plans (
        fingerprint_id text PRIMARY KEY,
        plan_json jsonb NOT NULL,
        cost double precision,
        captured_at timestamptz NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS slowquery.suggestions (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        fingerprint_id text NOT NULL,
        kind text NOT NULL CHECK (kind IN ('index', 'rewrite', 'denormalize', 'partition')),
        source text NOT NULL CHECK (source IN ('rules', 'llm')),
        sql text NOT NULL DEFAULT '',
        rationale text NOT NULL,
        applied_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_suggestions_fp_kind_sql UNIQUE (fingerprint_id, kind, sql)
    )""",
    """CREATE TABLE IF NOT EXISTS slowquery.schema_version (
        version integer PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
    )""",
    f"INSERT INTO slowquery.schema_version (version) VALUES ({SCHEMA_VERSION})"
    " ON CONFLICT (version) DO NOTHING",
)

_SQL_UPSERT_FINGERPRINT: Final = """
INSERT INTO slowquery.query_fingerprints AS f
    (service, id, canonical_sql, first_seen, last_seen, call_count, total_ms, max_ms,
     p50_ms, p95_ms, p99_ms)
VALUES (:service, :id, :sql, :first_seen, :last_seen, :calls, :total_ms, :max_ms,
        :p50, :p95, :p99)
ON CONFLICT (service, id) DO UPDATE SET
    first_seen = LEAST(f.first_seen, EXCLUDED.first_seen),
    last_seen = GREATEST(f.last_seen, EXCLUDED.last_seen),
    call_count = f.call_count + EXCLUDED.call_count,
    total_ms = f.total_ms + EXCLUDED.total_ms,
    max_ms = GREATEST(f.max_ms, EXCLUDED.max_ms),
    p50_ms = COALESCE(EXCLUDED.p50_ms, f.p50_ms),
    p95_ms = COALESCE(EXCLUDED.p95_ms, f.p95_ms),
    p99_ms = COALESCE(EXCLUDED.p99_ms, f.p99_ms)
"""
_SQL_UPSERT_REQUEST: Final = """
INSERT INTO slowquery.request_stats AS r
    (service, request_id, fingerprint_id, calls, total_ms, max_ms, first_seen, last_seen)
VALUES (:service, :request_id, :fid, :calls, :total_ms, :max_ms, :first_seen, :last_seen)
ON CONFLICT (service, request_id, fingerprint_id) DO UPDATE SET
    calls = r.calls + EXCLUDED.calls,
    total_ms = r.total_ms + EXCLUDED.total_ms,
    max_ms = GREATEST(r.max_ms, EXCLUDED.max_ms),
    first_seen = LEAST(r.first_seen, EXCLUDED.first_seen),
    last_seen = GREATEST(r.last_seen, EXCLUDED.last_seen)
"""
_SQL_INSERT_SAMPLE: Final = """
INSERT INTO slowquery.query_samples (service, fingerprint_id, request_id, duration_ms, sampled_at)
VALUES (:service, :fid, :request_id, :duration_ms, :sampled_at)
"""
_SQL_UPSERT_PLAN: Final = """
INSERT INTO slowquery.explain_plans AS p (fingerprint_id, plan_json, cost, captured_at)
VALUES (:fid, CAST(:plan AS jsonb), :cost, :captured_at)
ON CONFLICT (fingerprint_id) DO UPDATE SET
    plan_json = EXCLUDED.plan_json, cost = EXCLUDED.cost, captured_at = EXCLUDED.captured_at
WHERE EXCLUDED.captured_at >= p.captured_at
"""
_SQL_INSERT_SUGGESTION: Final = """
INSERT INTO slowquery.suggestions (fingerprint_id, kind, source, sql, rationale)
VALUES (:fid, :kind, :source, :sql, :rationale)
ON CONFLICT (fingerprint_id, kind, sql) DO NOTHING
"""
_SQL_SWEEP_SAMPLES: Final = (
    "DELETE FROM slowquery.query_samples WHERE service = :service AND sampled_at < :cutoff"
)
_SQL_SWEEP_REQUESTS: Final = (
    "DELETE FROM slowquery.request_stats WHERE service = :service AND last_seen < :cutoff"
)

_FP_COLUMNS: Final = (
    "id, canonical_sql, first_seen, last_seen, call_count, total_ms, max_ms, p50_ms, p95_ms, p99_ms"
)
_SQL_LIST: Final = (
    f"SELECT {_FP_COLUMNS} FROM slowquery.query_fingerprints WHERE service = :service"
    " ORDER BY total_ms DESC, id LIMIT :limit"
)
_SQL_LIST_SINCE: Final = (
    f"SELECT {_FP_COLUMNS} FROM slowquery.query_fingerprints"
    " WHERE service = :service AND last_seen >= :since ORDER BY total_ms DESC, id LIMIT :limit"
)
_REQ_SELECT: Final = (
    "SELECT r.fingerprint_id, f.canonical_sql, r.first_seen, r.last_seen, r.calls,"
    " r.total_ms, r.max_ms FROM slowquery.request_stats r"
    " JOIN slowquery.query_fingerprints f ON f.service = r.service AND f.id = r.fingerprint_id"
    " WHERE r.service = :service AND r.request_id = :request_id"
)
_SQL_LIST_REQUEST: Final = f"{_REQ_SELECT} ORDER BY r.total_ms DESC, r.fingerprint_id LIMIT :limit"
_SQL_LIST_REQUEST_SINCE: Final = (
    f"{_REQ_SELECT} AND r.last_seen >= :since ORDER BY r.total_ms DESC, r.fingerprint_id"
    " LIMIT :limit"
)
_SQL_SUGGESTIONS_FOR: Final = (
    "SELECT fingerprint_id, kind, source, sql, rationale, applied_at FROM slowquery.suggestions"
    " WHERE fingerprint_id = ANY(:ids) ORDER BY id"
)
_SQL_GET_FINGERPRINT: Final = (
    f"SELECT {_FP_COLUMNS} FROM slowquery.query_fingerprints WHERE service = :service AND id = :fid"
)
_SQL_GET_PLAN: Final = (
    "SELECT plan_json::text, cost, captured_at FROM slowquery.explain_plans"
    " WHERE fingerprint_id = :fid"
)
_SQL_RECENT_SAMPLES: Final = (
    "SELECT duration_ms, request_id, sampled_at FROM slowquery.query_samples"
    " WHERE service = :service AND fingerprint_id = :fid"
    " ORDER BY sampled_at DESC, id DESC LIMIT :limit"
)


CONNECT_TIMEOUT_S: Final = 10.0


def connect_args_for(url: str) -> dict[str, Any]:
    """Driver arguments for the store engine. Pure.

    A short connect timeout, because a read route waits on the store (asyncpg's
    own default is 60 s); asyncpg's statement cache off behind PgBouncer
    (Neon's ``-pooler`` hosts), where prepared statements do not survive.
    """
    parsed = make_url(url)
    if parsed.drivername == "postgresql+psycopg":
        return {"connect_timeout": int(CONNECT_TIMEOUT_S)}
    args: dict[str, Any] = {"timeout": CONNECT_TIMEOUT_S}
    if "-pooler" in (parsed.host or ""):
        args["statement_cache_size"] = 0
    return args


def _engine_for(url: str) -> AsyncEngine:
    return create_async_engine(
        url,
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args=connect_args_for(url),
    )


class PostgresStoreWriter(StoreWriter):
    """The service's own Postgres, schema ``slowquery``."""

    def __init__(self, store_url: str) -> None:
        self.url = normalise_store_url(store_url)
        self.engine = _engine_for(self.url)

    # -- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        async with self.engine.begin() as conn:
            if (await conn.execute(text(_SQL_SCHEMA_PRESENT))).scalar_one():
                return
            await conn.execute(text(_SQL_LOCK), {"key": _LOCK_KEY})
            for statement in DDL:
                await conn.execute(text(statement))

    async def close(self) -> None:
        await self.engine.dispose()

    # -- writes ----------------------------------------------------------------

    async def write_batch(self, service: str, batch: Batch) -> None:
        if batch.is_empty():
            return
        fingerprints = [
            {
                "service": service,
                "id": fid,
                "sql": d.canonical_sql,
                "first_seen": to_datetime(d.first_seen),
                "last_seen": to_datetime(d.last_seen),
                "calls": d.calls,
                "total_ms": d.total_ms,
                "max_ms": d.max_ms,
                "p50": d.p50_ms,
                "p95": d.p95_ms,
                "p99": d.p99_ms,
            }
            for fid, d in sorted(batch.fingerprints.items())
        ]
        requests = [
            {
                "service": service,
                "request_id": rid,
                "fid": fid,
                "calls": d.calls,
                "total_ms": d.total_ms,
                "max_ms": d.max_ms,
                "first_seen": to_datetime(d.first_seen),
                "last_seen": to_datetime(d.last_seen),
            }
            for (rid, fid), d in sorted(batch.requests.items())
        ]
        samples = [
            {
                "service": service,
                "fid": s.fingerprint_id,
                "request_id": s.request_id,
                "duration_ms": s.duration_ms,
                "sampled_at": to_datetime(s.sampled_at),
            }
            for s in batch.samples
        ]
        async with self.engine.begin() as conn:
            if fingerprints:
                await conn.execute(text(_SQL_UPSERT_FINGERPRINT), fingerprints)
            if requests:
                await conn.execute(text(_SQL_UPSERT_REQUEST), requests)
            if samples:
                await conn.execute(text(_SQL_INSERT_SAMPLE), samples)

    async def upsert_plan(
        self,
        fingerprint_id: str,
        plan_json: dict[str, JsonValue],
        cost: float | None,
        captured_at: datetime,
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(_SQL_UPSERT_PLAN),
                {
                    "fid": fingerprint_id,
                    "plan": json.dumps(plan_json),
                    "cost": cost,
                    "captured_at": captured_at,
                },
            )

    async def insert_suggestions(
        self, fingerprint_id: str, suggestions: Sequence[Suggestion]
    ) -> None:
        if not suggestions:
            return
        rows = [
            {
                "fid": fingerprint_id,
                "kind": s.kind,
                "source": s.source,
                "sql": s.sql or "",
                "rationale": s.rationale,
            }
            for s in suggestions
        ]
        async with self.engine.begin() as conn:
            await conn.execute(text(_SQL_INSERT_SUGGESTION), rows)

    async def sweep(self, service: str, cutoff: datetime) -> None:
        params = {"service": service, "cutoff": cutoff}
        async with self.engine.begin() as conn:
            await conn.execute(text(_SQL_SWEEP_SAMPLES), params)
            await conn.execute(text(_SQL_SWEEP_REQUESTS), params)

    # -- reads -----------------------------------------------------------------

    @staticmethod
    async def _suggestions(conn: AsyncConnection, ids: list[str]) -> dict[str, list[SuggestionOut]]:
        out: dict[str, list[SuggestionOut]] = {i: [] for i in ids}
        if not ids:
            return out
        for fid, kind, source, sql, rationale, applied_at in await conn.execute(
            text(_SQL_SUGGESTIONS_FOR), {"ids": ids}
        ):
            out[fid].append(
                SuggestionOut(
                    kind=kind,
                    source=source,
                    sql=sql or None,
                    rationale=rationale,
                    applied_at=applied_at,
                )
            )
        return out

    @staticmethod
    def _fingerprint_row(row: Any, suggestions: list[SuggestionOut]) -> QueryRow:
        fid, sql, first, last, calls, total, mx, p50, p95, p99 = row
        return QueryRow(
            id=fid,
            fingerprint=sql,
            first_seen=first,
            last_seen=last,
            call_count=calls,
            total_ms=total,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            max_ms=mx,
            suggestions=suggestions,
        )

    async def list_queries(
        self, service: str, *, since: datetime | None, limit: int
    ) -> list[QueryRow]:
        params: dict[str, Any] = {"service": service, "limit": limit}
        sql = _SQL_LIST
        if since is not None:
            sql, params["since"] = _SQL_LIST_SINCE, since
        async with self.engine.connect() as conn:
            rows = (await conn.execute(text(sql), params)).all()
            sugg = await self._suggestions(conn, [r[0] for r in rows])
        return [self._fingerprint_row(r, sugg[r[0]]) for r in rows]

    async def list_request_queries(
        self, service: str, request_id: str, *, since: datetime | None, limit: int
    ) -> list[QueryRow]:
        params: dict[str, Any] = {"service": service, "request_id": request_id, "limit": limit}
        sql = _SQL_LIST_REQUEST
        if since is not None:
            sql, params["since"] = _SQL_LIST_REQUEST_SINCE, since
        async with self.engine.connect() as conn:
            rows = (await conn.execute(text(sql), params)).all()
            sugg = await self._suggestions(conn, [r[0] for r in rows])
        return [
            QueryRow(
                id=fid,
                fingerprint=canonical,
                first_seen=first,
                last_seen=last,
                call_count=calls,
                total_ms=total,
                p50_ms=None,
                p95_ms=None,
                p99_ms=None,
                max_ms=mx,
                suggestions=sugg[fid],
            )
            for fid, canonical, first, last, calls, total, mx in rows
        ]

    async def get_query(self, service: str, fingerprint_id: str) -> QueryDetail | None:
        async with self.engine.connect() as conn:
            fp = (
                await conn.execute(
                    text(_SQL_GET_FINGERPRINT), {"service": service, "fid": fingerprint_id}
                )
            ).first()
            if fp is None:
                return None
            sugg = (await self._suggestions(conn, [fingerprint_id]))[fingerprint_id]
            plan = (await conn.execute(text(_SQL_GET_PLAN), {"fid": fingerprint_id})).first()
            samples = (
                await conn.execute(
                    text(_SQL_RECENT_SAMPLES),
                    {"service": service, "fid": fingerprint_id, "limit": RECENT_SAMPLES},
                )
            ).all()
        row = self._fingerprint_row(fp, sugg)
        return QueryDetail(
            fingerprint=row,
            canonical_sql=row.fingerprint,
            explain_plan=(
                None
                if plan is None
                else ExplainPlanOut(
                    plan_json=json.loads(plan[0]), cost=plan[1], captured_at=plan[2]
                )
            ),
            suggestions=sugg,
            recent_samples=[
                SampleOut(duration_ms=d, request_id=rid, sampled_at=at) for d, rid, at in samples
            ],
        )
