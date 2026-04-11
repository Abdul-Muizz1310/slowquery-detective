"""Async EXPLAIN worker — see ``docs/specs/06-explain-worker.md``.

Drains an in-process asyncio queue, runs ``EXPLAIN (ANALYZE, BUFFERS,
FORMAT JSON)`` against a dedicated engine, feeds the plan to the rules
engine and — on a miss — the LLM explainer, and persists everything to
the store. Per-fingerprint cooldown guards against a hot endpoint
re-running EXPLAIN every 100ms (which would double prod latency).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

_LOG = logging.getLogger("slowquery.worker")

# Type aliases for the rules / explainer callables.
RulesCallable = Callable[[dict[str, Any], str], list[Suggestion]]
ExplainerCallable = Callable[..., Awaitable[Suggestion | None]]


# Placeholder character in canonical SQL that the synthesizer replaces.
_PLACEHOLDER = "?"

# Substitution types the synthesizer knows how to generate. Order matters:
# context detection runs left-to-right per placeholder position.
_INT_LIKE_COLUMNS = re.compile(r"_id\b|\bid\b|\bcount\b|\bnum\b", re.IGNORECASE)
_BOOL_LIKE_COLUMNS = re.compile(r"\b(is|has|active|enabled|deleted)\w*\b", re.IGNORECASE)
_DATE_LIKE_COLUMNS = re.compile(r"_at\b|_date\b|_time\b", re.IGNORECASE)


@dataclass(frozen=True)
class ExplainJob:
    fingerprint_id: str
    canonical_sql: str
    observed_ms: float
    enqueued_at: float


@dataclass(frozen=True)
class CachedPlan:
    plan_json: dict[str, Any]
    plan_text: str
    cost: float
    captured_at: float
    suggestions: tuple[Suggestion, ...]


class ExplainWorker:
    """Background asyncio task that runs EXPLAIN and feeds the rules/LLM pipe."""

    def __init__(
        self,
        engine: Any,
        store: StoreWriter,
        rules: RulesCallable,
        explainer: ExplainerCallable | None,
        *,
        per_fingerprint_cooldown_seconds: float = 60.0,
        explain_timeout_seconds: float = 10.0,
        max_queue_size: int = 256,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._store = store
        self._rules = rules
        self._explainer = explainer
        self._cooldown = per_fingerprint_cooldown_seconds
        self._timeout = explain_timeout_seconds
        self._max_queue_size = max_queue_size
        self._now = now

        self._queue: asyncio.Queue[ExplainJob] = asyncio.Queue(maxsize=max_queue_size)
        self._task: asyncio.Task[None] | None = None
        self._cache: dict[str, CachedPlan] = {}
        # fingerprint_id -> timestamp at which EXPLAIN completed.
        self._cooldown_until: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._drain(), name="ExplainWorker")

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        if task.done():
            self._task = None
            return

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass
        except Exception:
            _LOG.debug("slowquery.worker.stop_error", exc_info=True)
        self._task = None

    def submit(self, job: ExplainJob) -> bool:
        try:
            self._queue.put_nowait(job)
            return True
        except asyncio.QueueFull:
            _LOG.debug("slowquery.worker.queue_full", extra={"fid": job.fingerprint_id})
            return False

    def plan_cache_get(self, fingerprint_id: str) -> CachedPlan | None:
        return self._cache.get(fingerprint_id)

    # ------------------------------------------------------------------
    # Queue drain loop
    # ------------------------------------------------------------------

    async def _drain(self) -> None:
        while True:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                return
            try:
                await self._process_one(item)
            except asyncio.CancelledError:
                return
            except Exception:
                _LOG.error("slowquery.worker.process_error", exc_info=True)

    async def _process_one(self, job: ExplainJob) -> None:
        # Cooldown gate.
        until = self._cooldown_until.get(job.fingerprint_id)
        if until is not None and self._now() < until:
            return

        plan = await self._run_explain(job)
        if plan is None:
            self._cooldown_until[job.fingerprint_id] = self._now() + self._cooldown
            return

        try:
            suggestions = self._rules(plan, job.canonical_sql)
        except Exception:
            _LOG.error("slowquery.worker.rules_error", exc_info=True)
            suggestions = []

        if not suggestions and self._explainer is not None:
            try:
                extra = await self._explainer(
                    job.canonical_sql,
                    plan,
                    fingerprint_id=job.fingerprint_id,
                )
            except Exception:
                _LOG.error("slowquery.worker.explainer_error", exc_info=True)
                extra = None
            if extra is not None:
                suggestions = [extra]

        cost = 0.0
        plan_root = plan.get("Plan") if isinstance(plan, dict) else None
        if isinstance(plan_root, dict):
            cost = float(plan_root.get("Total Cost") or 0.0)

        now = self._now()
        cached = CachedPlan(
            plan_json=plan,
            plan_text="",
            cost=cost,
            captured_at=now,
            suggestions=tuple(suggestions),
        )
        self._cache[job.fingerprint_id] = cached
        self._cooldown_until[job.fingerprint_id] = now + self._cooldown

        try:
            await self._store.upsert_plan(
                job.fingerprint_id,
                plan_json=plan,
                plan_text="",
                cost=cost,
            )
        except Exception:
            _LOG.error("slowquery.worker.store_upsert_plan_error", exc_info=True)

        if suggestions:
            try:
                await self._store.insert_suggestions(
                    job.fingerprint_id,
                    list(suggestions),
                )
            except Exception:
                _LOG.error("slowquery.worker.store_insert_suggestions_error", exc_info=True)

    # ------------------------------------------------------------------
    # EXPLAIN runner
    # ------------------------------------------------------------------

    async def _run_explain(self, job: ExplainJob) -> dict[str, Any] | None:
        substituted = synthesize_params(job.canonical_sql)
        if substituted is None:
            plan = await self._explain_statement(
                f"EXPLAIN (BUFFERS, FORMAT JSON) {job.canonical_sql}",
            )
            return plan

        plan = await self._explain_statement(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {substituted}",
        )
        if plan is None:
            # Retry once without ANALYZE — some canonical forms aren't
            # executable even with synthesized params.
            plan = await self._explain_statement(
                f"EXPLAIN (BUFFERS, FORMAT JSON) {substituted}",
            )
        return plan

    async def _explain_statement(self, statement: str) -> dict[str, Any] | None:
        try:
            async with self._engine.connect() as conn:
                result = await asyncio.wait_for(
                    conn.execute(text(statement)),
                    timeout=self._timeout,
                )
                plan_rows = result.scalar_one()
        except TimeoutError:
            _LOG.warning("slowquery.worker.explain_timeout")
            return None
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.debug("slowquery.worker.explain_error", exc_info=True)
            return None

        if isinstance(plan_rows, list) and plan_rows:
            first = plan_rows[0]
            if isinstance(first, dict):
                return first
        if isinstance(plan_rows, dict):
            return plan_rows
        return None


# --------------------------------------------------------------------------
# Parameter synthesizer (pure function, tested separately)
# --------------------------------------------------------------------------


def synthesize_params(canonical_sql: str) -> str | None:
    """Replace every ``?`` in a canonical SQL with a representative literal.

    Returns the substituted SQL, or ``None`` if the synthesizer can't
    produce a safe substitution (in which case the worker falls back to
    plain ``EXPLAIN`` without ANALYZE).

    The synthesizer inspects the small window of text preceding each ``?``
    to decide which literal type to emit. Integer default for unknown
    contexts; ``NULL`` for cases that look like expressions we can't
    safely guess (JSON path, array access).
    """
    if _PLACEHOLDER not in canonical_sql:
        return canonical_sql

    out: list[str] = []
    i = 0
    for match in re.finditer(r"\?", canonical_sql):
        out.append(canonical_sql[i : match.start()])
        context = canonical_sql[max(0, match.start() - 40) : match.start()]
        out.append(_literal_for_context(context))
        i = match.end()
    out.append(canonical_sql[i:])
    return "".join(out)


def _literal_for_context(context: str) -> str:
    """Return a representative literal based on the 40 chars before the ``?``."""
    lower = context.lower()
    # Give up on jsonb path / complex operators — the worker will fall
    # through to plain EXPLAIN.
    if "jsonb_path" in lower or "->>" in lower or "->" in lower:
        return "NULL"
    if _DATE_LIKE_COLUMNS.search(lower):
        return "now()"
    if _BOOL_LIKE_COLUMNS.search(lower):
        return "true"
    if _INT_LIKE_COLUMNS.search(lower):
        return "1"
    # Default to empty string — works for text columns and most comparisons.
    if "=" in lower or "like" in lower or "in (" in lower:
        return "''"
    return "1"
