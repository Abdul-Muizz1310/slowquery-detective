"""In-memory aggregation between flushes — spec 07 §Recording, cases 11-18.

The SQLAlchemy hook calls :meth:`Recorder.observe` once per recorded
statement. It must be cheap and must never block on I/O, so it only updates
three bounded in-memory aggregates under a lock:

- per fingerprint: calls, total/max duration, first/last seen;
- per ``(request_id, fingerprint_id)`` for tagged statements, regardless of
  the slow threshold;
- a list of slow samples (``duration_ms >= threshold_ms``).

The runtime's flush loop takes everything with :meth:`Recorder.drain` and
hands the resulting immutable :class:`Batch` to the store. Batches combine
with the pure :func:`merge_batches`, which is how a failed flush is retried
without losing or double-counting what it held.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Final

MAX_FINGERPRINTS: Final = 10_000
MAX_REQUESTS: Final = 10_000
MAX_SAMPLES: Final = 1_000

RequestKey = tuple[str, str]
"""``(request_id, fingerprint_id)``."""


@dataclass(frozen=True, slots=True)
class FingerprintDelta:
    canonical_sql: str
    calls: int
    total_ms: float
    max_ms: float
    first_seen: float
    last_seen: float
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None


@dataclass(frozen=True, slots=True)
class RequestDelta:
    calls: int
    total_ms: float
    max_ms: float
    first_seen: float
    last_seen: float


@dataclass(frozen=True, slots=True)
class SlowSample:
    fingerprint_id: str
    duration_ms: float
    request_id: str | None
    sampled_at: float


@dataclass(frozen=True)
class Batch:
    """Everything observed between two flushes. Times are Unix seconds."""

    fingerprints: Mapping[str, FingerprintDelta] = field(default_factory=dict)
    requests: Mapping[RequestKey, RequestDelta] = field(default_factory=dict)
    samples: tuple[SlowSample, ...] = ()
    dropped: int = 0

    def is_empty(self) -> bool:
        return not (self.fingerprints or self.requests or self.samples)


EMPTY_BATCH: Final = Batch()


def _merge_fp(a: FingerprintDelta, b: FingerprintDelta) -> FingerprintDelta:
    return FingerprintDelta(
        canonical_sql=a.canonical_sql,
        calls=a.calls + b.calls,
        total_ms=a.total_ms + b.total_ms,
        max_ms=max(a.max_ms, b.max_ms),
        first_seen=min(a.first_seen, b.first_seen),
        last_seen=max(a.last_seen, b.last_seen),
        p50_ms=b.p50_ms if b.p50_ms is not None else a.p50_ms,
        p95_ms=b.p95_ms if b.p95_ms is not None else a.p95_ms,
        p99_ms=b.p99_ms if b.p99_ms is not None else a.p99_ms,
    )


def _merge_req(a: RequestDelta, b: RequestDelta) -> RequestDelta:
    return RequestDelta(
        calls=a.calls + b.calls,
        total_ms=a.total_ms + b.total_ms,
        max_ms=max(a.max_ms, b.max_ms),
        first_seen=min(a.first_seen, b.first_seen),
        last_seen=max(a.last_seen, b.last_seen),
    )


def merge_batches(
    older: Batch,
    newer: Batch,
    *,
    max_fingerprints: int = MAX_FINGERPRINTS,
    max_requests: int = MAX_REQUESTS,
    max_samples: int = MAX_SAMPLES,
) -> Batch:
    """Combine two batches; pure. ``newer`` wins on percentiles.

    Keys already present always merge. New keys beyond a bound are dropped and
    counted, as are the oldest samples beyond ``max_samples``.
    """
    dropped = older.dropped + newer.dropped

    fps = dict(older.fingerprints)
    for key, delta in newer.fingerprints.items():
        existing = fps.get(key)
        if existing is not None:
            fps[key] = _merge_fp(existing, delta)
        elif len(fps) < max_fingerprints:
            fps[key] = delta
        else:
            dropped += 1

    reqs = dict(older.requests)
    for rkey, rdelta in newer.requests.items():
        rexisting = reqs.get(rkey)
        if rexisting is not None:
            reqs[rkey] = _merge_req(rexisting, rdelta)
        elif len(reqs) < max_requests:
            reqs[rkey] = rdelta
        else:
            dropped += 1

    samples = older.samples + newer.samples
    if len(samples) > max_samples:
        dropped += len(samples) - max_samples
        samples = samples[len(samples) - max_samples :]

    return Batch(fingerprints=fps, requests=reqs, samples=samples, dropped=dropped)


class Recorder:
    """Thread-safe accumulator fed by the hooks, drained by the flush loop."""

    def __init__(
        self,
        *,
        threshold_ms: float,
        clock: Callable[[], float] = time.time,
        max_fingerprints: int = MAX_FINGERPRINTS,
        max_requests: int = MAX_REQUESTS,
        max_samples: int = MAX_SAMPLES,
    ) -> None:
        if not threshold_ms > 0:
            raise ValueError("threshold_ms must be > 0")
        if min(max_fingerprints, max_requests, max_samples) <= 0:
            raise ValueError("bounds must be > 0")
        self.threshold_ms = float(threshold_ms)
        self._clock = clock
        self._max_fingerprints = max_fingerprints
        self._max_requests = max_requests
        self._max_samples = max_samples
        self._lock = threading.Lock()
        # Mutable accumulators: [calls, total, max, first, last] (+ sql for fps).
        self._fps: dict[str, list[float]] = {}
        self._sql: dict[str, str] = {}
        self._reqs: dict[RequestKey, list[float]] = {}
        self._samples: list[SlowSample] = []
        self._dropped = 0

    def observe(
        self,
        fingerprint_id: str,
        canonical_sql: str,
        duration_ms: float,
        request_id: str | None,
    ) -> None:
        """Record one statement. Never raises for bad input; counts it as dropped."""
        now = self._clock()
        with self._lock:
            if not (math.isfinite(duration_ms) and duration_ms >= 0):
                self._dropped += 1
                return
            fp = self._fps.get(fingerprint_id)
            if fp is None:
                if len(self._fps) >= self._max_fingerprints:
                    self._dropped += 1
                    return
                self._fps[fingerprint_id] = [1, duration_ms, duration_ms, now, now]
                self._sql[fingerprint_id] = canonical_sql
            else:
                fp[0] += 1
                fp[1] += duration_ms
                if duration_ms > fp[2]:
                    fp[2] = duration_ms
                fp[4] = now

            if request_id is not None:
                key = (request_id, fingerprint_id)
                req = self._reqs.get(key)
                if req is None:
                    if len(self._reqs) >= self._max_requests:
                        self._dropped += 1
                    else:
                        self._reqs[key] = [1, duration_ms, duration_ms, now, now]
                else:
                    req[0] += 1
                    req[1] += duration_ms
                    if duration_ms > req[2]:
                        req[2] = duration_ms
                    req[4] = now

            if duration_ms >= self.threshold_ms:
                if len(self._samples) >= self._max_samples:
                    self._dropped += 1
                else:
                    self._samples.append(SlowSample(fingerprint_id, duration_ms, request_id, now))

    def drain(self) -> Batch:
        """Take everything recorded so far, leaving the recorder empty."""
        with self._lock:
            fps, sql, reqs = self._fps, self._sql, self._reqs
            samples, dropped = self._samples, self._dropped
            self._fps, self._sql, self._reqs = {}, {}, {}
            self._samples, self._dropped = [], 0
        if not (fps or reqs or samples or dropped):
            return EMPTY_BATCH
        return Batch(
            fingerprints={
                k: FingerprintDelta(sql[k], int(v[0]), v[1], v[2], v[3], v[4])
                for k, v in fps.items()
            },
            requests={k: RequestDelta(int(v[0]), v[1], v[2], v[3], v[4]) for k, v in reqs.items()},
            samples=tuple(samples),
            dropped=dropped,
        )


def with_percentiles(
    batch: Batch, lookup: Callable[[str], tuple[float, float, float] | None]
) -> Batch:
    """Attach ``(p50, p95, p99)`` from ``lookup`` to each fingerprint delta."""
    if not batch.fingerprints:
        return batch
    fps: dict[str, FingerprintDelta] = {}
    for key, delta in batch.fingerprints.items():
        pct = lookup(key)
        fps[key] = (
            delta if pct is None else replace(delta, p50_ms=pct[0], p95_ms=pct[1], p99_ms=pct[2])
        )
    return replace(batch, fingerprints=fps)
