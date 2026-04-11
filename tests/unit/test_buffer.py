"""Red tests for docs/specs/01-buffer.md.

25 enumerated cases covering the sliding-window ring buffer. Cases 21-22
(concurrency stress) and case 25 (memory sanity) are marked ``slow`` so CI's
default ``-m 'not slow and not integration'`` skips them.
"""

from __future__ import annotations

import math
import threading

import pytest

from slowquery_detective.buffer import Percentiles, RingBuffer

# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_01_percentiles_of_known_distribution() -> None:
    buf = RingBuffer()
    for i in range(1, 101):
        buf.record("fp", float(i), now=0.0)
    p = buf.percentiles("fp", now=0.0)
    assert p is not None
    assert math.isclose(p.p50_ms, 50.0, abs_tol=1.0)
    assert math.isclose(p.p95_ms, 95.0, abs_tol=1.0)
    assert math.isclose(p.p99_ms, 99.0, abs_tol=1.0)
    assert p.max_ms == 100.0


def test_02_p99_within_tolerance_on_uniform_1000_samples() -> None:
    import random

    rng = random.Random(42)
    buf = RingBuffer()
    for _ in range(1000):
        buf.record("fp", rng.uniform(0, 1000), now=0.0)
    p = buf.percentiles("fp", now=0.0)
    assert p is not None
    assert 950.0 <= p.p99_ms <= 1000.0


def test_03_fingerprints_isolated() -> None:
    buf = RingBuffer()
    for i in range(1, 11):
        buf.record("a", float(i), now=0.0)
    for i in range(100, 111):
        buf.record("b", float(i), now=0.0)
    pa = buf.percentiles("a", now=0.0)
    pb = buf.percentiles("b", now=0.0)
    assert pa is not None and pb is not None
    assert pa.max_ms == 10.0
    assert pb.max_ms == 110.0


def test_04_record_then_percentiles_same_tick() -> None:
    buf = RingBuffer()
    buf.record("fp", 5.0, now=10.0)
    buf.record("fp", 7.0, now=10.0)
    p = buf.percentiles("fp", now=10.0)
    assert p is not None and p.sample_count == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_05_sliding_window_just_inside() -> None:
    buf = RingBuffer(window_seconds=60.0)
    buf.record("fp", 1.0, now=0.0)
    buf.record("fp", 2.0, now=30.0)
    buf.record("fp", 3.0, now=59.0)
    p = buf.percentiles("fp", now=60.0)
    assert p is not None and p.sample_count == 3


def test_06_eviction_past_window() -> None:
    buf = RingBuffer(window_seconds=60.0)
    buf.record("fp", 1.0, now=0.0)
    buf.record("fp", 2.0, now=30.0)
    buf.record("fp", 3.0, now=60.0)
    assert buf.percentiles("fp", now=121.0) is None


def test_07_empty_buffer_returns_none() -> None:
    buf = RingBuffer()
    assert buf.percentiles("never-recorded") is None


def test_08_single_sample_all_percentiles_equal() -> None:
    buf = RingBuffer()
    buf.record("fp", 42.0, now=0.0)
    p = buf.percentiles("fp", now=0.0)
    assert p == Percentiles(sample_count=1, p50_ms=42.0, p95_ms=42.0, p99_ms=42.0, max_ms=42.0)


def test_09_reservoir_cap_bounds_memory() -> None:
    buf = RingBuffer(max_samples_per_key=128)
    for i in range(1, 10001):
        buf.record("fp", float(i), now=0.0)
    p = buf.percentiles("fp", now=0.0)
    assert p is not None
    assert p.sample_count <= 128
    # With a uniform 1..10000 distribution and a reservoir, p95 should still
    # be in the right ballpark (within 10% of the true 9500).
    assert 8550.0 <= p.p95_ms <= 10000.0


def test_10_max_samples_per_key_one() -> None:
    buf = RingBuffer(max_samples_per_key=1)
    for i in range(1, 101):
        buf.record("fp", float(i), now=0.0)
    p = buf.percentiles("fp", now=0.0)
    assert p is not None and p.sample_count == 1


def test_11_clear_specific_key() -> None:
    buf = RingBuffer()
    buf.record("a", 1.0, now=0.0)
    buf.record("b", 2.0, now=0.0)
    buf.clear("a")
    assert buf.percentiles("a", now=0.0) is None
    assert buf.percentiles("b", now=0.0) is not None


def test_12_clear_all_keys() -> None:
    buf = RingBuffer()
    buf.record("a", 1.0, now=0.0)
    buf.record("b", 2.0, now=0.0)
    buf.clear()
    assert buf.percentiles("a", now=0.0) is None
    assert buf.percentiles("b", now=0.0) is None


def test_13_keys_snapshot_is_immutable() -> None:
    buf = RingBuffer()
    buf.record("a", 1.0, now=0.0)
    snap = buf.keys()
    buf.record("b", 2.0, now=0.0)
    assert "a" in snap
    assert "b" not in snap


# ---------------------------------------------------------------------------
# Failure cases
# ---------------------------------------------------------------------------


def test_14_negative_duration_rejected() -> None:
    buf = RingBuffer()
    with pytest.raises(ValueError):
        buf.record("fp", -0.1)


def test_15_nan_duration_rejected() -> None:
    buf = RingBuffer()
    with pytest.raises(ValueError):
        buf.record("fp", float("nan"))


def test_16_inf_duration_rejected() -> None:
    buf = RingBuffer()
    with pytest.raises(ValueError):
        buf.record("fp", float("inf"))


def test_17_empty_fingerprint_rejected() -> None:
    buf = RingBuffer()
    with pytest.raises(ValueError):
        buf.record("", 1.0)


def test_18_zero_window_rejected() -> None:
    with pytest.raises(ValueError):
        RingBuffer(window_seconds=0)


def test_19_zero_reservoir_rejected() -> None:
    with pytest.raises(ValueError):
        RingBuffer(max_samples_per_key=0)


def test_20_expired_key_returns_none() -> None:
    buf = RingBuffer(window_seconds=60.0)
    buf.record("fp", 1.0, now=0.0)
    assert buf.percentiles("fp", now=121.0) is None


# ---------------------------------------------------------------------------
# Concurrency / stress
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_21_concurrent_record_no_loss() -> None:
    buf = RingBuffer(max_samples_per_key=200_000)

    def worker() -> None:
        for _ in range(10_000):
            buf.record("fp", 1.0, now=0.0)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    p = buf.percentiles("fp", now=0.0)
    assert p is not None
    assert p.sample_count in (80_000, 200_000)  # either full count or capped


@pytest.mark.slow
def test_22_concurrent_record_percentiles_clear_no_crash() -> None:
    buf = RingBuffer()
    stop = threading.Event()

    def writer() -> None:
        while not stop.is_set():
            buf.record("fp", 1.0, now=0.0)

    def reader() -> None:
        while not stop.is_set():
            p = buf.percentiles("fp", now=0.0)
            if p is not None:
                assert not math.isnan(p.p95_ms)
                assert not math.isinf(p.p95_ms)

    def clearer() -> None:
        while not stop.is_set():
            buf.clear("fp")

    threads = [threading.Thread(target=f) for f in (writer, reader, clearer)]
    for t in threads:
        t.start()
    import time

    time.sleep(0.5)
    stop.set()
    for t in threads:
        t.join()


def test_23_injected_clock_monotonicity_does_not_resurrect() -> None:
    buf = RingBuffer(window_seconds=10.0)
    buf.record("fp", 1.0, now=100.0)
    assert buf.percentiles("fp", now=200.0) is None
    # A later query with a backwards clock must not make the sample reappear.
    assert buf.percentiles("fp", now=105.0) is None


# ---------------------------------------------------------------------------
# Security / privacy
# ---------------------------------------------------------------------------


def test_24_repr_contains_no_sql_or_params() -> None:
    buf = RingBuffer()
    buf.record("fp_abc", 1.0, now=0.0)
    text = repr(buf)
    assert "SELECT" not in text.upper()
    assert "fp_abc" not in text or "sample" not in text  # Not load-bearing


@pytest.mark.slow
def test_25_memory_sanity_10k_fingerprints() -> None:
    buf = RingBuffer(max_samples_per_key=1024)
    for i in range(10_000):
        for j in range(100):
            buf.record(f"fp_{i}", float(j), now=0.0)
    assert len(buf.keys()) == 10_000
