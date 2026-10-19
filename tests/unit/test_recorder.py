"""Spec 07 cases 11-18 — in-memory aggregation between flushes."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from hypothesis import given, settings
from hypothesis import strategies as st
from slowquery_detective.recorder import (
    EMPTY_BATCH,
    Batch,
    Recorder,
    merge_batches,
)

FP_A = "a" * 16
FP_B = "b" * 16
SQL_A = "select * from a where id = ?"
SQL_B = "select * from b where id = ?"


class _Clock:
    def __init__(self, t: float = 1_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _recorder(threshold_ms: float = 100.0, **kw: int) -> tuple[Recorder, _Clock]:
    clock = _Clock()
    return Recorder(threshold_ms=threshold_ms, clock=clock, **kw), clock


# ---------------------------------------------------------------------------
# 11-14
# ---------------------------------------------------------------------------


def test_11_one_observation_one_fingerprint_delta() -> None:
    rec, clock = _recorder()
    rec.observe(FP_A, SQL_A, 12.5, None)
    batch = rec.drain()
    delta = batch.fingerprints[FP_A]
    assert delta.canonical_sql == SQL_A
    assert (delta.calls, delta.total_ms, delta.max_ms) == (1, 12.5, 12.5)
    assert delta.first_seen == delta.last_seen == clock.t
    assert batch.requests == {}
    assert batch.samples == ()


def test_11_repeated_observations_accumulate() -> None:
    rec, clock = _recorder()
    rec.observe(FP_A, SQL_A, 10.0, None)
    clock.t += 5
    rec.observe(FP_A, SQL_A, 30.0, None)
    delta = rec.drain().fingerprints[FP_A]
    assert (delta.calls, delta.total_ms, delta.max_ms) == (2, 40.0, 30.0)
    assert delta.last_seen - delta.first_seen == 5


def test_12_tagged_observation_adds_request_delta() -> None:
    rec, _ = _recorder()
    rec.observe(FP_A, SQL_A, 2.0, "R1")
    rec.observe(FP_A, SQL_A, 4.0, "R1")
    rec.observe(FP_B, SQL_B, 1.0, "R1")
    rec.observe(FP_A, SQL_A, 8.0, "R2")
    rec.observe(FP_A, SQL_A, 16.0, None)
    batch = rec.drain()
    assert set(batch.requests) == {("R1", FP_A), ("R1", FP_B), ("R2", FP_A)}
    r1a = batch.requests[("R1", FP_A)]
    assert (r1a.calls, r1a.total_ms, r1a.max_ms) == (2, 6.0, 4.0)
    assert batch.fingerprints[FP_A].calls == 4


def test_12_untagged_observation_adds_no_request_delta() -> None:
    rec, _ = _recorder()
    rec.observe(FP_A, SQL_A, 2.0, None)
    assert rec.drain().requests == {}


def test_13_threshold_boundary_for_samples() -> None:
    rec, clock = _recorder(threshold_ms=100.0)
    rec.observe(FP_A, SQL_A, 99.999, "R1")
    rec.observe(FP_A, SQL_A, 100.0, "R1")
    rec.observe(FP_B, SQL_B, 250.0, None)
    samples = rec.drain().samples
    assert [(s.fingerprint_id, s.duration_ms, s.request_id) for s in samples] == [
        (FP_A, 100.0, "R1"),
        (FP_B, 250.0, None),
    ]
    assert all(s.sampled_at == clock.t for s in samples)


def test_14_drain_empties_the_recorder() -> None:
    rec, _ = _recorder()
    rec.observe(FP_A, SQL_A, 500.0, "R1")
    first = rec.drain()
    assert not first.is_empty()
    second = rec.drain()
    assert second.is_empty()
    assert second == EMPTY_BATCH


# ---------------------------------------------------------------------------
# 15 — bounds
# ---------------------------------------------------------------------------


def test_15_fingerprint_bound_drops_and_counts() -> None:
    rec, _ = _recorder(max_fingerprints=3)
    for i in range(4):
        rec.observe(f"{i:016x}", SQL_A, 1.0, None)
    rec.observe(f"{0:016x}", SQL_A, 1.0, None)  # existing key still accepted
    batch = rec.drain()
    assert len(batch.fingerprints) == 3
    assert batch.fingerprints[f"{0:016x}"].calls == 2
    assert batch.dropped == 1


def test_15_request_bound_drops_and_counts() -> None:
    rec, _ = _recorder(max_requests=2)
    rec.observe(FP_A, SQL_A, 1.0, "R1")
    rec.observe(FP_A, SQL_A, 1.0, "R2")
    rec.observe(FP_A, SQL_A, 1.0, "R3")
    batch = rec.drain()
    assert set(batch.requests) == {("R1", FP_A), ("R2", FP_A)}
    assert batch.dropped == 1
    # The fingerprint aggregate still counts the statement.
    assert batch.fingerprints[FP_A].calls == 3


def test_15_sample_bound_drops_and_counts() -> None:
    rec, _ = _recorder(threshold_ms=1.0, max_samples=2)
    for _ in range(3):
        rec.observe(FP_A, SQL_A, 5.0, None)
    batch = rec.drain()
    assert len(batch.samples) == 2
    assert batch.dropped == 1


def test_recorder_rejects_invalid_observations() -> None:
    rec, _ = _recorder()
    for bad in (-1.0, float("nan"), float("inf")):
        rec.observe(FP_A, SQL_A, bad, None)
    batch = rec.drain()
    assert batch.fingerprints == {}
    assert batch.dropped == 3


def test_recorder_rejects_invalid_construction() -> None:
    import pytest

    with pytest.raises(ValueError):
        Recorder(threshold_ms=0)
    with pytest.raises(ValueError):
        Recorder(threshold_ms=10, max_samples=0)


# ---------------------------------------------------------------------------
# 16-17 — merge properties
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Obs:
    fp: str
    duration: float
    request: str | None
    dt: float


_obs = st.builds(
    _Obs,
    fp=st.sampled_from([FP_A, FP_B, "c" * 16]),
    duration=st.floats(min_value=0.0, max_value=1_000.0, allow_nan=False),
    request=st.sampled_from([None, "R1", "R2"]),
    dt=st.floats(min_value=0.0, max_value=10.0, allow_nan=False),
)


def _batch_of(observations: list[_Obs], start: float = 0.0) -> Batch:
    clock = _Clock(start)
    rec = Recorder(threshold_ms=500.0, clock=clock)
    for o in observations:
        clock.t += o.dt
        rec.observe(o.fp, f"sql {o.fp}", o.duration, o.request)
    return rec.drain()


def _summary(batch: Batch) -> dict[str, tuple[int, float, float, float, float]]:
    return {
        k: (
            v.calls,
            round(v.total_ms, 6),
            v.max_ms,
            v.first_seen,
            v.last_seen,
        )
        for k, v in batch.fingerprints.items()
    }


def _req_summary(batch: Batch) -> dict[tuple[str, str], tuple[int, float, float]]:
    return {k: (v.calls, round(v.total_ms, 6), v.max_ms) for k, v in batch.requests.items()}


@settings(max_examples=150, deadline=None)
@given(st.lists(_obs, max_size=20), st.lists(_obs, max_size=20), st.lists(_obs, max_size=20))
def test_16_merge_is_associative(a: list[_Obs], b: list[_Obs], c: list[_Obs]) -> None:
    ba, bb, bc = _batch_of(a, 0.0), _batch_of(b, 1_000.0), _batch_of(c, 2_000.0)
    left = merge_batches(merge_batches(ba, bb), bc)
    right = merge_batches(ba, merge_batches(bb, bc))
    assert _summary(left) == _summary(right)
    assert _req_summary(left) == _req_summary(right)
    assert [s.sampled_at for s in left.samples] == [s.sampled_at for s in right.samples]


@settings(max_examples=150, deadline=None)
@given(st.lists(_obs, max_size=40), st.integers(min_value=0, max_value=40))
def test_17_split_drains_merge_to_one_drain(observations: list[_Obs], cut: int) -> None:
    whole = _batch_of(observations)
    clock = _Clock(0.0)
    rec = Recorder(threshold_ms=500.0, clock=clock)
    halves: list[Batch] = []
    for i, o in enumerate(observations):
        if i == cut:
            halves.append(rec.drain())
        clock.t += o.dt
        rec.observe(o.fp, f"sql {o.fp}", o.duration, o.request)
    halves.append(rec.drain())
    merged = EMPTY_BATCH
    for h in halves:
        merged = merge_batches(merged, h)
    assert _summary(merged) == _summary(whole)
    assert _req_summary(merged) == _req_summary(whole)
    assert len(merged.samples) == len(whole.samples)


def test_16_merge_prefers_newer_percentiles_and_keeps_older_when_missing() -> None:
    from dataclasses import replace

    older = _batch_of([_Obs(FP_A, 1.0, None, 0.0)])
    newer = _batch_of([_Obs(FP_A, 2.0, None, 0.0)], 10.0)
    older = replace(
        older,
        fingerprints={FP_A: replace(older.fingerprints[FP_A], p50_ms=1.0, p95_ms=1.0, p99_ms=1.0)},
    )
    merged = merge_batches(older, newer)
    assert merged.fingerprints[FP_A].p50_ms == 1.0
    newer = replace(
        newer,
        fingerprints={FP_A: replace(newer.fingerprints[FP_A], p50_ms=2.0, p95_ms=2.0, p99_ms=2.0)},
    )
    merged = merge_batches(older, newer)
    assert merged.fingerprints[FP_A].p95_ms == 2.0


def test_16_merge_respects_bounds_and_counts_drops() -> None:
    a = _batch_of([_Obs(FP_A, 600.0, "R1", 1.0), _Obs(FP_A, 700.0, "R1", 1.0)])
    b = _batch_of([_Obs(FP_B, 800.0, "R2", 1.0)], 100.0)
    merged = merge_batches(a, b, max_samples=2, max_requests=1, max_fingerprints=1)
    assert len(merged.samples) == 2
    # Newest samples are kept.
    assert [s.duration_ms for s in merged.samples] == [700.0, 800.0]
    assert len(merged.requests) == 1
    assert len(merged.fingerprints) == 1
    assert merged.dropped == 3


# ---------------------------------------------------------------------------
# 18 — thread safety
# ---------------------------------------------------------------------------


def test_18_concurrent_observations_are_all_counted() -> None:
    rec = Recorder(threshold_ms=1e9)

    def _hammer() -> None:
        for _ in range(1_000):
            rec.observe(FP_A, SQL_A, 1.0, "R1")

    threads = [threading.Thread(target=_hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    batch = rec.drain()
    assert batch.fingerprints[FP_A].calls == 8_000
    assert batch.requests[("R1", FP_A)].calls == 8_000
