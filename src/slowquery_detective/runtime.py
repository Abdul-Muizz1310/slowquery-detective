"""The imperative shell between hooks, worker and store — spec 07 §Lifecycle.

``Runtime.observe`` is the hook callback: it records the statement and, for a
slow one, submits an EXPLAIN job. A background task flushes the recorder to
the store every two seconds; the read routes flush before they read. The
retention sweep runs at startup and then at most hourly, and only after a
flush that wrote something, so an idle service never wakes its database.

If the store cannot start (database asleep or unreachable), the runtime logs
at ERROR, keeps serving the host, holds pending data (bounded by
:func:`merge_batches`) and retries on the next flush that has data or the next
read.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Final

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.explain import ExplainJob, ExplainWorker
from slowquery_detective.hooks import Observation
from slowquery_detective.recorder import (
    EMPTY_BATCH,
    Batch,
    Recorder,
    merge_batches,
    with_percentiles,
)
from slowquery_detective.store import StoreWriter, to_datetime
from slowquery_detective.tagging import spawn_untagged

_LOG = logging.getLogger("slowquery.runtime")

FLUSH_INTERVAL_S: Final = 2.0
SWEEP_INTERVAL_S: Final = 3_600.0
FINAL_FLUSH_TIMEOUT_S: Final = 5.0


class Runtime:
    """Owns the flush loop, store readiness and the retention sweep."""

    def __init__(
        self,
        *,
        service: str,
        store: StoreWriter,
        recorder: Recorder,
        buffer: RingBuffer,
        worker: ExplainWorker,
        retention_s: int,
        clock: Callable[[], float] = time.time,
        flush_interval_s: float = FLUSH_INTERVAL_S,
        sweep_interval_s: float = SWEEP_INTERVAL_S,
        on_start: Callable[[], None] | None = None,
        on_stop: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.service = service
        self.store = store
        self.recorder = recorder
        self.buffer = buffer
        self.worker = worker
        self._retention_s = retention_s
        self._clock = clock
        self._flush_interval_s = flush_interval_s
        self._sweep_interval_s = sweep_interval_s
        self._on_start = on_start
        self._on_stop = on_stop
        self._pending: Batch = EMPTY_BATCH
        self._flush_lock = asyncio.Lock()
        self._ready = False
        self._last_sweep: float | None = None
        self._task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def ready(self) -> bool:
        return self._ready

    # -- hook callback (sync, hot path) ---------------------------------------

    def observe(self, obs: Observation) -> None:
        try:
            self.recorder.observe(
                obs.fingerprint_id, obs.canonical_sql, obs.duration_ms, obs.request_id
            )
            if obs.executemany or obs.duration_ms < self.recorder.threshold_ms:
                return
            if not self.worker.accepts(obs.fingerprint_id):
                return
            self.worker.submit(
                ExplainJob(
                    fingerprint_id=obs.fingerprint_id,
                    canonical_sql=obs.canonical_sql,
                    statement=obs.statement,
                    parameters=obs.parameters,
                    observed_ms=obs.duration_ms,
                    recent_call_count=self.buffer.count(obs.fingerprint_id),
                    enqueued_at=time.monotonic(),
                )
            )
        except Exception:
            _LOG.error("slowquery.runtime.observe_failed", exc_info=True)

    # -- lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            self._flush_lock = asyncio.Lock()  # a lock stays bound to its first loop
        self._loop = loop
        if self._on_start is not None:
            self._on_start()
        if await self.ensure_ready():
            await self._sweep()
        await self.worker.start()
        if self._task is None or self._task.done():
            self._task = spawn_untagged(self._flush_loop(), name="slowquery-flush")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        try:
            await asyncio.wait_for(self.flush(), timeout=FINAL_FLUSH_TIMEOUT_S)
        except Exception:
            _LOG.error("slowquery.runtime.final_flush_failed", exc_info=True)
        await self.worker.stop()
        if self._on_stop is not None:
            try:
                await self._on_stop()
            except Exception:
                _LOG.error("slowquery.runtime.on_stop_failed", exc_info=True)
        try:
            await self.store.close()
        except Exception:
            _LOG.error("slowquery.runtime.store_close_failed", exc_info=True)
        self._ready = False

    async def ensure_ready(self) -> bool:
        """Start the store if it has not started yet; ``False`` if it cannot."""
        if self._ready:
            return True
        try:
            await self.store.start()
        except Exception:
            _LOG.error(
                "slowquery.runtime.store_unavailable",
                extra={"service": self.service},
                exc_info=True,
            )
            return False
        self._ready = True
        return True

    # -- flushing -------------------------------------------------------------

    def _percentiles(self, fingerprint_id: str) -> tuple[float, float, float] | None:
        p = self.buffer.percentiles(fingerprint_id)
        return None if p is None else (p.p50_ms, p.p95_ms, p.p99_ms)

    async def flush(self) -> None:
        """Write what the recorder holds. Touches the store only if there is data."""
        async with self._flush_lock:
            drained = with_percentiles(self.recorder.drain(), self._percentiles)
            batch = merge_batches(self._pending, drained)
            self._pending = EMPTY_BATCH
            if batch.dropped:
                _LOG.warning(
                    "slowquery.runtime.dropped",
                    extra={"service": self.service, "dropped": batch.dropped},
                )
                batch = replace(batch, dropped=0)
            if batch.is_empty():
                return
            try:
                if not await self.ensure_ready():
                    self._pending = batch
                    return
                await self.store.write_batch(self.service, batch)
            except Exception:
                _LOG.error("slowquery.runtime.flush_failed", exc_info=True)
                self._pending = batch
                return
            except BaseException:
                # Cancelled mid-write (shutdown, a cancelled read, the final
                # flush's timeout): keep the batch for the next flush. A write
                # that committed just before the cancellation is counted twice,
                # which is the safer side of the trade.
                self._pending = batch
                raise
            if (
                self._last_sweep is None
                or self._clock() - self._last_sweep >= self._sweep_interval_s
            ):
                await self._sweep()

    async def _sweep(self) -> None:
        now = self._clock()
        try:
            await self.store.sweep(self.service, to_datetime(now - self._retention_s))
        except Exception:
            _LOG.error("slowquery.runtime.sweep_failed", exc_info=True)
            return
        self._last_sweep = now

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval_s)
            try:
                await self.flush()
            except Exception:
                _LOG.error("slowquery.runtime.flush_loop_error", exc_info=True)
