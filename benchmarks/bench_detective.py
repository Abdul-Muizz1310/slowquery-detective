"""Micro-benchmarks for the slowquery-detective hot path (no database).

Only two things run synchronously on every query — ``fingerprint()`` and
``RingBuffer.record()`` — and both are measured here. The rules engine and
``RingBuffer.percentiles`` are measured too, but they are *not* per-query costs:
``run_rules`` runs on the background ``ExplainWorker`` and ``percentiles`` only
when the dashboard is rendered. ``EXPLAIN`` itself needs a database and is out of
scope for this harness.

Run with::

    uv run python benchmarks/bench_detective.py

Two methodology notes, both of which exist because a published number silently
went stale once already (see ``benchmarks/report.md``):

*Fingerprint is memoized.* ``fingerprint()`` caches on ``(sql, dialect)`` via a
bounded LRU. Because this harness re-times a fixed set of SQL strings, timing
``fingerprint()`` directly measures a **dict lookup**, not a parse. So each
query shape is measured twice: ``(cold)`` calls the uncached core so the sqlglot
parse actually runs on every iteration, and ``(cached)`` measures the LRU hit.
Only the cold figure describes the work; the cached figure describes the win.

*The percentile window is clock-pinned.* ``RingBuffer`` evicts samples outside
its 60 s window, so a benchmark that filled the buffer and then spent a minute
on other cases would silently end up timing an empty-window ``None`` return.
The percentile case therefore records and reads at a fixed injected timestamp.

The published figure per case is its fastest timed burst, with the median burst
printed next to it so a contended host is visible rather than silently inflating
the number (see :func:`summarize`). Burst length is per-case (``BenchCase.batch``)
so that one burst of an expensive op is still short enough to fit inside a clean
scheduling window.

Numbers are machine-dependent; commit the host + Python version alongside any
recorded result (see ``benchmarks/report.md``).
"""

from __future__ import annotations

import platform
import statistics
import sys
import time
from collections.abc import Callable
from typing import NamedTuple

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.fingerprint import _fingerprint_cached, fingerprint
from slowquery_detective.rules.base import run_rules

# The uncached core of ``fingerprint()``. Reaching for the LRU wrapper's
# ``__wrapped__`` is deliberate: it is the only way to time the real parse
# without either paying ``cache_clear()`` inside the timed loop or fabricating
# new SQL strings (which would measure string building too). It computes exactly
# what ``fingerprint()`` computes, minus the two input guards.
_fingerprint_uncached = _fingerprint_cached.__wrapped__

QUERIES = {
    "simple_select": "SELECT * FROM orders WHERE user_id = 42 LIMIT 20",
    "join": (
        "SELECT o.id, u.email FROM orders o JOIN users u ON u.id = o.user_id "
        "WHERE o.total > 100 ORDER BY o.created_at DESC LIMIT 50"
    ),
    "in_list": "SELECT * FROM products WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
    "parse_fallback": "SELECT * FROM t WHERE x = 'a' AND }}}broken sql(((",
}

_SEQ_SCAN_PLAN = {
    "Plan": {
        "Node Type": "Seq Scan",
        "Relation Name": "orders",
        "Plan Rows": 1_000_000,
        "Total Cost": 14209.0,
        "Output": ["id", "user_id", "total", "created_at"],
    }
}

# Samples pre-loaded into the percentile window: the buffer's default per-key
# cap, i.e. the worst case the sort path ever sees.
PERCENTILE_WINDOW_SAMPLES = 1024

# Fixed timestamp used for both the fill and the read of the percentile window,
# so the measurement cannot depend on how long earlier cases took.
_PINNED_CLOCK = 1_000.0

# Default calls per timed burst, for sub-microsecond operations.
BATCH_SIZE = 1000

# A cold sqlglot parse costs upwards of 1500x a cached lookup, so the cold cases
# get fewer iterations and much shorter bursts (3-7 ms each, 200 samples).
_COLD_ITERATIONS = 2_000
_COLD_BATCH = 10
_CACHED_ITERATIONS = 20_000

# ~9 ms bursts for the ~85 µs percentile read, 200 samples.
_PERCENTILE_ITERATIONS = 20_000
_PERCENTILE_BATCH = 100


class BenchCase(NamedTuple):
    """One thing to time: a name, a zero-arg callable, an iteration count.

    ``batch`` is how many calls make up one timed burst. It should be sized so a
    burst lasts a millisecond or two: long bursts of an expensive op are certain
    to be preempted on a busy host, which destroys the min-of-batches estimator.
    """

    name: str
    fn: Callable[[], object]
    iterations: int
    batch: int = BATCH_SIZE


class BenchResult(NamedTuple):
    """Per-op cost for one :class:`BenchCase`, in microseconds.

    ``us_per_op`` is the *fastest* batch and is the figure the report quotes;
    ``us_per_op_median`` is carried alongside it so a reader can see how noisy
    the host was. See :func:`summarize`.
    """

    operation: str
    iterations: int
    us_per_op: float
    us_per_op_median: float
    ops_per_sec: float


def build_cases() -> list[BenchCase]:
    """Enumerate every case the report publishes, in table order."""
    cases: list[BenchCase] = []

    # Cold: the sqlglot parse runs on every iteration.
    for label, sql in QUERIES.items():
        cases.append(
            BenchCase(
                name=f"fingerprint:{label} (cold)",
                fn=lambda sql=sql: _fingerprint_uncached(sql, "postgres"),  # type: ignore[misc]
                iterations=_COLD_ITERATIONS,
                batch=_COLD_BATCH,
            )
        )

    # Cached: one representative shape is enough — an LRU hit is a dict lookup
    # keyed on the SQL string, so the cost barely varies across shapes.
    simple = QUERIES["simple_select"]
    cases.append(
        BenchCase(
            name="fingerprint:simple_select (cached)",
            fn=lambda: fingerprint(simple),
            iterations=_CACHED_ITERATIONS,
        )
    )

    cases.append(
        BenchCase(
            name="run_rules:6_rules",
            fn=lambda: run_rules(
                _SEQ_SCAN_PLAN,
                "select * from orders where user_id = ?",
                fingerprint_id="fp",
                recent_call_count=0,
            ),
            iterations=50_000,
            batch=100,
        )
    )

    record_buffer = RingBuffer()
    cases.append(
        BenchCase(
            name="ringbuffer:record",
            fn=lambda: record_buffer.record("hot", 12.5),
            iterations=200_000,
        )
    )

    percentile_buffer = RingBuffer()
    for i in range(PERCENTILE_WINDOW_SAMPLES):
        percentile_buffer.record("warm", float(i % 200), now=_PINNED_CLOCK)
    cases.append(
        BenchCase(
            name="ringbuffer:percentiles",
            fn=lambda: percentile_buffer.percentiles("warm", now=_PINNED_CLOCK),
            iterations=_PERCENTILE_ITERATIONS,
            batch=_PERCENTILE_BATCH,
        )
    )

    return cases


def summarize(operation: str, iterations: int, per_op_samples: list[float]) -> BenchResult:
    """Reduce per-batch seconds-per-op samples to one published figure.

    The headline is the **fastest** batch, which is what ``timeit``'s own docs
    recommend: batches slower than the minimum are almost never this code being
    slower, they are other processes on the host getting scheduled. The median
    is reported next to it so a contended run is visible rather than silently
    inflating the number the README quotes.

    Raises:
        ValueError: if ``per_op_samples`` is empty (nothing was measured).
    """
    if not per_op_samples:
        raise ValueError("per_op_samples must not be empty")

    fastest = min(per_op_samples)
    return BenchResult(
        operation=operation,
        iterations=iterations,
        us_per_op=fastest * 1e6,
        us_per_op_median=statistics.median(per_op_samples) * 1e6,
        ops_per_sec=1.0 / fastest,
    )


def run_case(case: BenchCase) -> BenchResult:
    """Time ``case`` in bursts of ``case.batch`` calls and summarize the bursts."""
    warmup = min(1000, max(1, case.iterations // 10))
    for _ in range(warmup):
        case.fn()

    batch = case.batch
    rounds = max(1, case.iterations // batch)
    per_op_samples: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        for _ in range(batch):
            case.fn()
        per_op_samples.append((time.perf_counter() - start) / batch)

    return summarize(case.name, rounds * batch, per_op_samples)


def format_table(results: list[BenchResult]) -> str:
    """Render ``results`` as the fixed-width table the report quotes."""
    width = max(len(r.operation) for r in results)
    lines = [
        "# slowquery-detective hot-path microbenchmark",
        f"# {platform.platform()} | Python {sys.version.split()[0]}",
        f"{'operation':<{width}}  {'iters':>8}  {'us/op min':>11}  "
        f"{'us/op p50':>11}  {'ops/sec':>12}",
    ]
    for r in results:
        lines.append(
            f"{r.operation:<{width}}  {r.iterations:>8}  "
            f"{round(r.us_per_op, 2)!s:>11}  {round(r.us_per_op_median, 2)!s:>11}  "
            f"{r.ops_per_sec:>12,.0f}"
        )
    return "\n".join(lines)


def main() -> None:
    print(format_table([run_case(case) for case in build_cases()]))


if __name__ == "__main__":
    main()
