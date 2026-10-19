"""The store interface and the in-process default — spec 07 §Stores.

A :class:`StoreWriter` persists what the runtime flushes (fingerprint, request
and sample aggregates), what the EXPLAIN worker produces (plans, suggestions),
and answers the two read routes. Two implementations ship:

- :class:`InMemoryStoreWriter` — process-local and bounded; the default when
  ``install`` gets neither ``store_url`` nor ``store``. Each process sees only
  its own traffic.
- :class:`~slowquery_detective.pg_store.PostgresStoreWriter` — the service's
  own Postgres, schema ``slowquery``; what ``store_url`` selects.

Traffic (fingerprints, request aggregates, samples) is kept per ``service``;
plans and suggestions describe a statement against a database and are keyed by
fingerprint only. Both implementations pass ``tests/store_contract.py``.
"""

from __future__ import annotations

import abc
from collections import OrderedDict, deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from pydantic import JsonValue

from slowquery_detective.recorder import Batch, FingerprintDelta, RequestDelta
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.schemas import (
    ExplainPlanOut,
    QueryDetail,
    QueryRow,
    SampleOut,
    SuggestionOut,
)

RECENT_SAMPLES: Final = 20


def to_datetime(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, UTC)


class StoreWriter(abc.ABC):
    """Where slowquery-detective keeps what it observes. Subclass all methods."""

    @abc.abstractmethod
    async def start(self) -> None:
        """Prepare the store (create schema, open pools). Idempotent; may raise."""

    @abc.abstractmethod
    async def write_batch(self, service: str, batch: Batch) -> None:
        """Add one flush's aggregates. Additive: counts and totals accumulate."""

    @abc.abstractmethod
    async def upsert_plan(
        self,
        fingerprint_id: str,
        plan_json: dict[str, JsonValue],
        cost: float | None,
        captured_at: datetime,
    ) -> None:
        """Store the (already scrubbed) latest plan for a fingerprint."""

    @abc.abstractmethod
    async def insert_suggestions(
        self, fingerprint_id: str, suggestions: Sequence[Suggestion]
    ) -> None:
        """Add suggestions; one row per ``(fingerprint_id, kind, sql)``."""

    @abc.abstractmethod
    async def sweep(self, service: str, cutoff: datetime) -> None:
        """Delete the service's samples and request aggregates older than ``cutoff``."""

    @abc.abstractmethod
    async def list_queries(
        self, service: str, *, since: datetime | None, limit: int
    ) -> list[QueryRow]:
        """The service's fingerprints, ``total_ms`` desc then ``id``."""

    @abc.abstractmethod
    async def list_request_queries(
        self, service: str, request_id: str, *, since: datetime | None, limit: int
    ) -> list[QueryRow]:
        """The fingerprints one request executed, with that request's numbers."""

    @abc.abstractmethod
    async def get_query(self, service: str, fingerprint_id: str) -> QueryDetail | None:
        """Detail for one fingerprint of the service, or ``None``."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Release resources. Idempotent."""


# ---------------------------------------------------------------------------
# In-memory implementation
# ---------------------------------------------------------------------------


@dataclass
class _Traffic:
    """Mutable call statistics (a fingerprint, or one request's use of one)."""

    first_seen: datetime
    last_seen: datetime
    calls: int
    total_ms: float
    max_ms: float

    def add(self, delta: FingerprintDelta | RequestDelta) -> None:
        self.first_seen = min(self.first_seen, to_datetime(delta.first_seen))
        self.last_seen = max(self.last_seen, to_datetime(delta.last_seen))
        self.calls += delta.calls
        self.total_ms += delta.total_ms
        self.max_ms = max(self.max_ms, delta.max_ms)

    @classmethod
    def of(cls, delta: FingerprintDelta | RequestDelta) -> _Traffic:
        return cls(
            first_seen=to_datetime(delta.first_seen),
            last_seen=to_datetime(delta.last_seen),
            calls=delta.calls,
            total_ms=delta.total_ms,
            max_ms=delta.max_ms,
        )


@dataclass
class _Fingerprint:
    canonical_sql: str
    traffic: _Traffic
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None


class InMemoryStoreWriter(StoreWriter):
    """Process-local store: bounded, swept, never shared between processes."""

    def __init__(
        self,
        *,
        max_samples_per_fingerprint: int = 100,
        max_request_rows: int = 50_000,
    ) -> None:
        if max_samples_per_fingerprint <= 0 or max_request_rows <= 0:
            raise ValueError("bounds must be > 0")
        self._max_samples = max_samples_per_fingerprint
        self._max_request_rows = max_request_rows
        self._fps: dict[tuple[str, str], _Fingerprint] = {}
        self._reqs: OrderedDict[tuple[str, str, str], _Traffic] = OrderedDict()
        self._samples: dict[tuple[str, str], deque[SampleOut]] = {}
        self._plans: dict[str, ExplainPlanOut] = {}
        self._suggestions: dict[str, dict[tuple[str, str], SuggestionOut]] = {}

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    # -- writes ----------------------------------------------------------------

    async def write_batch(self, service: str, batch: Batch) -> None:
        for fid, delta in batch.fingerprints.items():
            fp = self._fps.get((service, fid))
            if fp is None:
                self._fps[(service, fid)] = _Fingerprint(
                    delta.canonical_sql,
                    _Traffic.of(delta),
                    delta.p50_ms,
                    delta.p95_ms,
                    delta.p99_ms,
                )
                continue
            fp.traffic.add(delta)
            if delta.p50_ms is not None:
                fp.p50_ms, fp.p95_ms, fp.p99_ms = delta.p50_ms, delta.p95_ms, delta.p99_ms
        for (rid, fid), rdelta in batch.requests.items():
            key = (service, rid, fid)
            row = self._reqs.get(key)
            if row is None:
                self._reqs[key] = _Traffic.of(rdelta)
                while len(self._reqs) > self._max_request_rows:
                    self._reqs.popitem(last=False)
            else:
                row.add(rdelta)
                self._reqs.move_to_end(key)
        for sample in batch.samples:
            bucket = self._samples.setdefault(
                (service, sample.fingerprint_id), deque(maxlen=self._max_samples)
            )
            bucket.append(
                SampleOut(
                    duration_ms=sample.duration_ms,
                    request_id=sample.request_id,
                    sampled_at=to_datetime(sample.sampled_at),
                )
            )

    async def upsert_plan(
        self,
        fingerprint_id: str,
        plan_json: dict[str, JsonValue],
        cost: float | None,
        captured_at: datetime,
    ) -> None:
        current = self._plans.get(fingerprint_id)
        if current is not None and current.captured_at > captured_at:
            return  # an older plan never replaces a newer one
        self._plans[fingerprint_id] = ExplainPlanOut(
            plan_json=plan_json, cost=cost, captured_at=captured_at
        )

    async def insert_suggestions(
        self, fingerprint_id: str, suggestions: Sequence[Suggestion]
    ) -> None:
        bucket = self._suggestions.setdefault(fingerprint_id, {})
        for s in suggestions:
            bucket.setdefault(
                (s.kind, s.sql or ""),
                SuggestionOut(
                    kind=s.kind, source=s.source, sql=s.sql, rationale=s.rationale, applied_at=None
                ),
            )

    async def sweep(self, service: str, cutoff: datetime) -> None:
        for key in [
            k for k, row in self._reqs.items() if k[0] == service and row.last_seen < cutoff
        ]:
            del self._reqs[key]
        for (svc, _fid), bucket in self._samples.items():
            if svc != service:
                continue
            kept = [s for s in bucket if s.sampled_at >= cutoff]
            bucket.clear()
            bucket.extend(kept)

    # -- reads -----------------------------------------------------------------

    def _suggestions_for(self, fingerprint_id: str) -> list[SuggestionOut]:
        return list(self._suggestions.get(fingerprint_id, {}).values())

    def _row(self, fid: str, fp: _Fingerprint, traffic: _Traffic, *, per_request: bool) -> QueryRow:
        return QueryRow(
            id=fid,
            fingerprint=fp.canonical_sql,
            first_seen=traffic.first_seen,
            last_seen=traffic.last_seen,
            call_count=traffic.calls,
            total_ms=traffic.total_ms,
            p50_ms=None if per_request else fp.p50_ms,
            p95_ms=None if per_request else fp.p95_ms,
            p99_ms=None if per_request else fp.p99_ms,
            max_ms=traffic.max_ms,
            suggestions=self._suggestions_for(fid),
        )

    @staticmethod
    def _top(rows: list[QueryRow], since: datetime | None, limit: int) -> list[QueryRow]:
        kept = [r for r in rows if since is None or r.last_seen >= since]
        kept.sort(key=lambda r: (-r.total_ms, r.id))
        return kept[:limit]

    async def list_queries(
        self, service: str, *, since: datetime | None, limit: int
    ) -> list[QueryRow]:
        rows = [
            self._row(fid, fp, fp.traffic, per_request=False)
            for (svc, fid), fp in self._fps.items()
            if svc == service
        ]
        return self._top(rows, since, limit)

    async def list_request_queries(
        self, service: str, request_id: str, *, since: datetime | None, limit: int
    ) -> list[QueryRow]:
        rows: list[QueryRow] = []
        for (svc, rid, fid), traffic in self._reqs.items():
            if svc != service or rid != request_id:
                continue
            fp = self._fps.get((service, fid))
            if fp is not None:
                rows.append(self._row(fid, fp, traffic, per_request=True))
        return self._top(rows, since, limit)

    async def get_query(self, service: str, fingerprint_id: str) -> QueryDetail | None:
        fp = self._fps.get((service, fingerprint_id))
        if fp is None:
            return None
        row = self._row(fingerprint_id, fp, fp.traffic, per_request=False)
        # Newest first; among equal timestamps, the later-inserted first.
        samples = list(reversed(self._samples.get((service, fingerprint_id), ())))
        samples.sort(key=lambda s: s.sampled_at, reverse=True)
        return QueryDetail(
            fingerprint=row,
            canonical_sql=fp.canonical_sql,
            explain_plan=self._plans.get(fingerprint_id),
            suggestions=row.suggestions,
            recent_samples=samples[:RECENT_SAMPLES],
        )
