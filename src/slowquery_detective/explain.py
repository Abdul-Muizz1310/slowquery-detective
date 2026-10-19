"""Background EXPLAIN worker — see ``docs/specs/06-explain-worker.md`` and spec 07.

Drains an in-process asyncio queue of slow statements. For each one it runs
``EXPLAIN (FORMAT JSON)`` on the host engine **with the statement's real,
driver-level parameters** — never with ``ANALYZE``, so the statement itself
is never executed again — feeds the plan to the rules engine and, on a miss,
the LLM explainer, and persists the plan and suggestions.

Upstreamed from slowquery-demo-backend's shim 4: the 0.1 parameter
synthesizer produced invalid SQL for typed parameters (``cast(1 as uuid)``),
so every such EXPLAIN failed silently.

Because a plan made with real parameters prints their values, everything that
leaves the worker — the stored plan and the plan sent to an LLM — is scrubbed
by :func:`~slowquery_detective.plan_scrub.scrub_plan`. Driver errors are
logged by class only: their messages can echo a parameter value.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Protocol

import sqlglot
from sqlglot import exp

from slowquery_detective.plan_scrub import scrub_plan
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter
from slowquery_detective.tagging import spawn_untagged

_LOG = logging.getLogger("slowquery.worker")

EXPLAIN_PREFIX: Final = "EXPLAIN (FORMAT JSON) "

# When the cooldown map grows past this many fingerprints, expired entries are
# dropped, so it is bounded by the number of fingerprints explained recently.
COOLDOWN_PRUNE_AT = 10_000

# Statement kinds that mutate data if they reach the server. EXPLAIN in
# ANALYZE mode *executes* its target; the worker no longer uses that mode, and
# still never sends any of these — including data-modifying CTEs like
# ``WITH x AS (INSERT ... RETURNING ...) SELECT ...`` — to EXPLAIN at all.
_WRITE_EXPRESSIONS: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
)

# Top-level statement types that are provably read-only. Anything else
# (DDL, ``exp.Command`` for DO/CALL/VACUUM/raw statements, etc.) is denied.
_READ_EXPRESSIONS: tuple[type[exp.Expression], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
    exp.Subquery,
    exp.Values,
)


def is_read_only_sql(canonical_sql: str, dialect: str = "postgres") -> bool:
    """Return ``True`` only if ``canonical_sql`` is a provably read-only query.

    Default-deny: any statement we cannot parse into a pure read (SELECT /
    VALUES / set-operation of SELECTs, including read-only CTEs) is rejected.
    This guard is why 0.1's ANALYZE-mode EXPLAIN could not re-execute a
    captured INSERT/UPDATE/DELETE/CALL/DO against the live database; 0.2 keeps
    it in front of a plain EXPLAIN as defense in depth.
    """
    try:
        tree = sqlglot.parse_one(canonical_sql, dialect=dialect)
    except Exception:
        return False
    if tree is None:
        return False
    # A data-modifying node anywhere in the tree disqualifies the statement,
    # even when it is nested inside a CTE that resolves to a SELECT.
    for write_type in _WRITE_EXPRESSIONS:
        if tree.find(write_type) is not None:
            return False
    return isinstance(tree, _READ_EXPRESSIONS)


class RulesCallable(Protocol):
    def __call__(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> list[Suggestion]: ...


class ExplainerCallable(Protocol):
    async def __call__(
        self, canonical_sql: str, plan_json: dict[str, Any], *, fingerprint_id: str
    ) -> Suggestion | None: ...


@dataclass(frozen=True)
class ExplainJob:
    """A slow statement to explain. ``parameters`` never leave the worker."""

    fingerprint_id: str
    canonical_sql: str
    statement: str
    parameters: Any
    observed_ms: float
    recent_call_count: int
    enqueued_at: float


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _driver_parameters(parameters: Any) -> Any:
    """Parameters as one execution: a tuple (positional) or a dict (named).

    A list would be read by ``exec_driver_sql`` as several parameter sets.
    """
    if parameters is None or isinstance(parameters, tuple | dict):
        return parameters
    if isinstance(parameters, Mapping):
        return dict(parameters)
    if isinstance(parameters, Sequence) and not isinstance(parameters, str | bytes):
        return tuple(parameters)
    return None


def _plan_from(raw: Any) -> dict[str, Any] | None:
    """The top-level plan object from an ``EXPLAIN (FORMAT JSON)`` result."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    return raw if isinstance(raw, dict) else None


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
        wall_clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._engine = engine
        self._store = store
        self._rules = rules
        self._explainer = explainer
        self._cooldown = per_fingerprint_cooldown_seconds
        self._timeout = explain_timeout_seconds
        self._now = now
        self._wall_clock = wall_clock

        self._queue: asyncio.Queue[ExplainJob] = asyncio.Queue(maxsize=max_queue_size)
        self._queued: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # fingerprint_id -> monotonic time until which it is not explained again.
        self._cooldown_until: dict[str, float] = {}
        # Jobs dropped because the queue was full; logged at WARNING.
        self._dropped_jobs = 0
        self._processed_jobs = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            # A new event loop (a host test suite re-entering its TestClient):
            # an asyncio.Queue stays bound to the loop it first waited on, so
            # start over with a fresh one. Jobs queued on the old loop are dropped.
            self._dropped_jobs += self._queue.qsize()
            self._queue = asyncio.Queue(maxsize=self._queue.maxsize)
            self._queued.clear()
        self._loop = loop
        # An empty context: nothing the worker runs is attributed to a request,
        # even when the worker is first started from inside one.
        self._task = spawn_untagged(self._drain(), name="ExplainWorker")

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._task = None
        if task.done():
            return
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass
        except Exception:
            _LOG.debug("slowquery.worker.stop_error", exc_info=True)

    def accepts(self, fingerprint_id: str) -> bool:
        """False while the fingerprint is queued or cooling down."""
        if fingerprint_id in self._queued:
            return False
        until = self._cooldown_until.get(fingerprint_id)
        return until is None or self._now() >= until

    def submit(self, job: ExplainJob) -> bool:
        """Queue ``job`` without blocking; safe to call from any thread."""
        if not self.accepts(job.fingerprint_id):
            return False
        loop = self._loop
        if loop is not None and not loop.is_closed() and not _running_on(loop):
            self._queued.add(job.fingerprint_id)
            loop.call_soon_threadsafe(self._enqueue, job)
            return True
        return self._enqueue(job)

    @property
    def queued(self) -> int:
        return self._queue.qsize()

    @property
    def cooling_down(self) -> int:
        """Fingerprints currently held in the cooldown map."""
        return len(self._cooldown_until)

    @property
    def processed_jobs(self) -> int:
        """Total jobs taken off the queue and handled (explained or refused)."""
        return self._processed_jobs

    @property
    def dropped_jobs(self) -> int:
        """Total jobs dropped due to a full queue (backpressure counter)."""
        return self._dropped_jobs

    @property
    def engine(self) -> Any:
        """The engine EXPLAIN runs on."""
        return self._engine

    async def process_pending(self, limit: int | None = None) -> int:
        """Process up to ``limit`` queued jobs now; return how many were processed."""
        processed = 0
        while limit is None or processed < limit:
            try:
                job = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            await self._run(job)
            processed += 1
        return processed

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _enqueue(self, job: ExplainJob) -> bool:
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            self._queued.discard(job.fingerprint_id)
            self._dropped_jobs += 1
            _LOG.warning(
                "slowquery.worker.queue_full",
                extra={"fid": job.fingerprint_id, "dropped_total": self._dropped_jobs},
            )
            return False
        self._queued.add(job.fingerprint_id)
        return True

    async def _drain(self) -> None:
        while True:
            job = await self._queue.get()
            await self._run(job)

    async def _run(self, job: ExplainJob) -> None:
        self._queued.discard(job.fingerprint_id)
        try:
            await self._process_one(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.error("slowquery.worker.process_error", exc_info=True)
        self._processed_jobs += 1

    def _cool_down(self, fingerprint_id: str) -> None:
        now = self._now()
        if len(self._cooldown_until) >= COOLDOWN_PRUNE_AT:
            self._cooldown_until = {k: v for k, v in self._cooldown_until.items() if v > now}
        self._cooldown_until[fingerprint_id] = now + self._cooldown

    async def _process_one(self, job: ExplainJob) -> None:
        until = self._cooldown_until.get(job.fingerprint_id)
        if until is not None and self._now() < until:
            return

        # Defense in depth: EXPLAIN without ANALYZE does not execute its target,
        # but a statement that is not provably read-only is still never sent.
        if not is_read_only_sql(job.canonical_sql):
            _LOG.debug("slowquery.worker.skip_non_read_only", extra={"fid": job.fingerprint_id})
            self._cool_down(job.fingerprint_id)
            return

        plan = await self._explain(job)
        if plan is None:
            self._cool_down(job.fingerprint_id)
            return

        try:
            suggestions = self._rules(
                plan,
                job.canonical_sql,
                fingerprint_id=job.fingerprint_id,
                recent_call_count=job.recent_call_count,
            )
        except Exception:
            _LOG.error("slowquery.worker.rules_error", exc_info=True)
            suggestions = []

        safe_plan = scrub_plan(plan)
        if not suggestions and self._explainer is not None:
            try:
                extra = await self._explainer(
                    job.canonical_sql, safe_plan, fingerprint_id=job.fingerprint_id
                )
            except Exception:
                _LOG.error("slowquery.worker.explainer_error", exc_info=True)
                extra = None
            if extra is not None:
                suggestions = [extra]

        self._cool_down(job.fingerprint_id)
        root = plan.get("Plan")
        cost = float(root.get("Total Cost") or 0.0) if isinstance(root, dict) else None

        try:
            await self._store.upsert_plan(job.fingerprint_id, safe_plan, cost, self._wall_clock())
        except Exception:
            _LOG.error("slowquery.worker.store_upsert_plan_error", exc_info=True)

        if suggestions:
            try:
                await self._store.insert_suggestions(job.fingerprint_id, suggestions)
            except Exception:
                _LOG.error("slowquery.worker.store_insert_suggestions_error", exc_info=True)

    async def _explain(self, job: ExplainJob) -> dict[str, Any] | None:
        statement = EXPLAIN_PREFIX + job.statement
        try:
            async with asyncio.timeout(self._timeout), self._engine.connect() as conn:
                result = await conn.exec_driver_sql(statement, _driver_parameters(job.parameters))
                raw = result.scalar_one()
        except TimeoutError:
            _LOG.warning("slowquery.worker.explain_timeout", extra={"fid": job.fingerprint_id})
            return None
        except Exception as exc:
            # Class only: a driver message can quote a parameter value.
            _LOG.debug(
                "slowquery.worker.explain_error",
                extra={"fid": job.fingerprint_id, "error": type(exc).__name__},
            )
            return None
        return _plan_from(raw)


def _running_on(loop: asyncio.AbstractEventLoop) -> bool:
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False
