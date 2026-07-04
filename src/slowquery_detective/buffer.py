"""Ring buffer + percentile computation — see ``docs/specs/01-buffer.md``.

Sliding 60s window per fingerprint. Memory stays bounded by a fixed-size
``deque(maxlen=max_samples_per_key)`` that drops the *oldest* sample when
full — a recency-biased cap that is the correct shape for a sliding window
(the newest observations are exactly the ones a p95 spike lives in). We
deliberately do **not** use Algorithm-R reservoir sampling: a uniform
sample over all history keeps ancient timestamps alive and is fundamentally
incompatible with time-window eviction (see COR-1 in the audit).

Thread-safe via a single lock guarding per-key deques. All timing uses an
injected ``now`` for determinism in tests; production callers leave
``now=None`` and ``time.monotonic()`` is used.
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Iterator
from typing import NamedTuple


class Percentiles(NamedTuple):
    sample_count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


class RingBuffer:
    """Sliding-window sample buffer keyed by fingerprint id."""

    def __init__(
        self,
        window_seconds: float = 60.0,
        max_samples_per_key: int = 1024,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if max_samples_per_key <= 0:
            raise ValueError("max_samples_per_key must be > 0")

        self._window_seconds = float(window_seconds)
        self._max_samples_per_key = int(max_samples_per_key)
        self._lock = threading.Lock()
        self._samples: dict[str, deque[tuple[float, float]]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(
        self,
        fingerprint_id: str,
        duration_ms: float,
        now: float | None = None,
    ) -> None:
        if not fingerprint_id:
            raise ValueError("fingerprint_id must be non-empty")
        if not math.isfinite(duration_ms) or duration_ms < 0:
            raise ValueError("duration_ms must be a finite non-negative float")

        timestamp = self._clock(now)

        with self._lock:
            samples = self._samples.get(fingerprint_id)
            if samples is None:
                # ``maxlen`` gives us a fixed memory ceiling that drops the
                # oldest sample on overflow — the right eviction order for a
                # sliding window.
                samples = deque(maxlen=self._max_samples_per_key)
                self._samples[fingerprint_id] = samples

            samples.append((timestamp, float(duration_ms)))

    def percentiles(
        self,
        fingerprint_id: str,
        now: float | None = None,
    ) -> Percentiles | None:
        cutoff = self._clock(now) - self._window_seconds

        with self._lock:
            samples = self._samples.get(fingerprint_id)
            if samples is None:
                return None

            # Evict expired samples, preserving each survivor's *real*
            # arrival timestamp. Overwriting timestamps here (as an earlier
            # version did with a shared ``cutoff + 1e-9`` sentinel) collapses
            # the sliding window to "since the last read" — a sample recorded
            # at t=0 would vanish on the very next percentiles() call.
            live = [(t, d) for t, d in samples if t >= cutoff]
            if len(live) != len(samples):
                # Compact the deque so expired entries can't resurrect on a
                # later call with a backwards clock. ``clear`` keeps maxlen.
                samples.clear()
                samples.extend(live)

            if not live:
                return None

            return _compute_percentiles([d for _t, d in live])

    def keys(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._samples.keys())

    def clear(self, fingerprint_id: str | None = None) -> None:
        with self._lock:
            if fingerprint_id is None:
                self._samples.clear()
            else:
                self._samples.pop(fingerprint_id, None)

    def __contains__(self, fingerprint_id: object) -> bool:
        """Membership test used by the dashboard (``fid in buffer``)."""
        with self._lock:
            return fingerprint_id in self._samples

    def __iter__(self) -> Iterator[str]:
        """Iterate a *snapshot* of fingerprint ids (safe against mutation)."""
        return iter(self.keys())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clock(now: float | None) -> float:
        return time.monotonic() if now is None else float(now)

    def __repr__(self) -> str:
        # Deliberately terse: never include per-sample contents or keys so
        # sensitive fingerprint ids don't land in log output.
        with self._lock:
            n_keys = len(self._samples)
        return f"RingBuffer(keys={n_keys}, window_seconds={self._window_seconds})"


def _compute_percentiles(samples: list[float]) -> Percentiles:
    """Compute p50/p95/p99/max over a non-empty list of durations.

    Uses linear-interpolated percentiles (same definition as numpy's
    ``percentile`` with ``method='linear'``). The sample list is short
    (bounded by ``max_samples_per_key``, default 1024), so a plain
    ``sorted`` call is fine.
    """
    ordered = sorted(samples)
    n = len(ordered)

    def _pct(p: float) -> float:
        if n == 1:
            return ordered[0]
        rank = p * (n - 1)
        lo = math.floor(rank)
        hi = math.ceil(rank)
        if lo == hi:
            return ordered[lo]
        weight = rank - lo
        return ordered[lo] * (1 - weight) + ordered[hi] * weight

    return Percentiles(
        sample_count=n,
        p50_ms=_pct(0.50),
        p95_ms=_pct(0.95),
        p99_ms=_pct(0.99),
        max_ms=ordered[-1],
    )
