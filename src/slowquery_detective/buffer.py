"""Ring buffer + percentile computation — see ``docs/specs/01-buffer.md``.

Sliding 60s window per fingerprint with a reservoir cap for bounded memory
under sustained high QPS. Thread-safe via a single lock guarding per-key
deques. All timing uses an injected ``now`` for determinism in tests;
production callers leave ``now=None`` and ``time.monotonic()`` is used.
"""

from __future__ import annotations

import math
import random
import threading
import time
from collections import deque
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
        # Random source is seeded per-instance so tests running in parallel
        # don't see cross-test interference. ``random.Random()`` uses os
        # entropy by default; we accept that non-determinism for the
        # reservoir replacement policy.
        self._rng = random.Random()
        # Counter per key for reservoir sampling (Algorithm R).
        self._seen: dict[str, int] = {}

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
                samples = deque()
                self._samples[fingerprint_id] = samples
                self._seen[fingerprint_id] = 0

            seen = self._seen[fingerprint_id] + 1
            self._seen[fingerprint_id] = seen

            if len(samples) < self._max_samples_per_key:
                samples.append((timestamp, float(duration_ms)))
            else:
                # Reservoir replacement (Algorithm R): pick a random slot
                # in the first ``max_samples_per_key`` items to replace.
                idx = self._rng.randrange(seen)
                if idx < self._max_samples_per_key:
                    samples[idx] = (timestamp, float(duration_ms))

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

            # Evict expired samples from the front of the deque. Reservoir
            # replacement can leave old timestamps in the middle too, so
            # compact the survivors into a fresh list.
            live = [d for t, d in samples if t >= cutoff]
            # Keep the deque in sync so we don't re-iterate dead entries.
            samples.clear()
            for d in live:
                samples.append((cutoff + 1e-9, d))  # placeholder timestamp

            if not live:
                return None

            return _compute_percentiles(live)

    def keys(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._samples.keys())

    def clear(self, fingerprint_id: str | None = None) -> None:
        with self._lock:
            if fingerprint_id is None:
                self._samples.clear()
                self._seen.clear()
            else:
                self._samples.pop(fingerprint_id, None)
                self._seen.pop(fingerprint_id, None)

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
