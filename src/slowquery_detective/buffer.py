"""Ring buffer + percentile computation — see ``docs/specs/01-buffer.md``.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

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
        # S4 will validate; we store so tests can read the config back.
        self._window_seconds = window_seconds
        self._max_samples_per_key = max_samples_per_key

    def record(
        self,
        fingerprint_id: str,
        duration_ms: float,
        now: float | None = None,
    ) -> None:
        raise NotImplementedError("S4: implement RingBuffer.record per docs/specs/01-buffer.md")

    def percentiles(
        self,
        fingerprint_id: str,
        now: float | None = None,
    ) -> Percentiles | None:
        raise NotImplementedError(
            "S4: implement RingBuffer.percentiles per docs/specs/01-buffer.md"
        )

    def keys(self) -> frozenset[str]:
        raise NotImplementedError("S4: implement RingBuffer.keys per docs/specs/01-buffer.md")

    def clear(self, fingerprint_id: str | None = None) -> None:
        raise NotImplementedError("S4: implement RingBuffer.clear per docs/specs/01-buffer.md")
