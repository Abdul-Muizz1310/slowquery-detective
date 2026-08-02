"""Tests for the hot-path microbenchmark harness (``benchmarks/bench_detective.py``).

The harness is dev tooling, not shipped library code, but it publishes numbers
into ``benchmarks/report.md`` and the README, so it needs the same guardrails as
anything else that makes a factual claim. Two past defects motivate these cases:

* ``fingerprint()`` gained a bounded LRU cache (commit ``ec112df``). The
  harness benchmarks a fixed set of SQL strings, so every timed call became a
  cache hit and the published "fingerprint costs ~334 µs" number silently
  turned into "a dict lookup costs 0.18 µs" — three orders of magnitude, with
  no test failure anywhere to notice it.
* ``RingBuffer.percentiles`` used to expire its whole window on the first read.
  The harness therefore timed a ``None`` short-circuit and published 0.5 µs for
  work that actually costs ~85 µs.

Enumerated cases:

1. Every case has a unique name and a positive iteration count.
2. ``build_cases()`` reports both a cold (uncached) and a cached fingerprint
   path, so the memoization win is visible instead of hiding the real cost.
3. The cold fingerprint case does not touch the LRU cache at all (neither hit
   nor miss), i.e. it measures a real sqlglot parse every call.
4. The cached fingerprint case does register LRU hits, i.e. it measures the
   memoized path it claims to measure.
5. The percentiles case measures a full 1024-sample window, never ``None``.
6. The percentiles case is independent of how long the rest of the benchmark
   took: it still measures a full window when the wall clock has advanced far
   past the buffer's sliding window.
7. ``run_case`` reports the iteration count it actually executed and a positive
   per-op cost.
8. ``format_table`` renders one row per result with the operation name, both
   µs/op columns and ops/sec, and does not truncate long operation names.
9. ``summarize`` publishes the *fastest* batch as the headline per-op cost (the
   estimator ``timeit`` recommends, because slower batches measure other
   processes rather than this code) and carries the median alongside it so the
   spread stays visible. ``ops_per_sec`` is derived from the headline cost.
10. ``run_case`` honours a per-case batch size, so a case whose single op costs
    hundreds of microseconds can still be timed in bursts short enough for one
    burst to land inside a clean scheduling window.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from slowquery_detective.buffer import Percentiles
from slowquery_detective.fingerprint import _fingerprint_cached, fingerprint

_BENCH_PATH = Path(__file__).resolve().parents[2] / "benchmarks" / "bench_detective.py"


def _load_bench() -> ModuleType:
    """Import ``benchmarks/bench_detective.py`` (not a package) by path."""
    spec = importlib.util.spec_from_file_location("bench_detective_under_test", _BENCH_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def bench() -> ModuleType:
    return _load_bench()


def _case(bench: ModuleType, needle: str) -> Any:
    matches = [c for c in bench.build_cases() if needle in c.name]
    assert matches, f"no benchmark case matching {needle!r}"
    assert len(matches) == 1, f"{needle!r} matched {[c.name for c in matches]}"
    return matches[0]


# ---------------------------------------------------------------------------
# Case 1
# ---------------------------------------------------------------------------


def test_01_cases_have_unique_names_and_positive_iterations(bench: ModuleType) -> None:
    cases = bench.build_cases()
    assert cases, "harness reported no cases"
    names = [c.name for c in cases]
    assert len(names) == len(set(names)), f"duplicate case names: {names}"
    for case in cases:
        assert case.iterations > 0, case.name
        assert callable(case.fn), case.name
        # A batch that does not divide the iteration count means the harness
        # silently runs fewer ops than the table's "iters" column claims.
        assert case.batch >= 1, case.name
        assert case.batch <= case.iterations, case.name
        assert case.iterations % case.batch == 0, case.name


# ---------------------------------------------------------------------------
# Case 2
# ---------------------------------------------------------------------------


def test_02_reports_both_cold_and_cached_fingerprint_paths(bench: ModuleType) -> None:
    names = [c.name for c in bench.build_cases()]
    cold = [n for n in names if n.startswith("fingerprint") and "cold" in n]
    cached = [n for n in names if n.startswith("fingerprint") and "cached" in n]
    assert cold, f"no cold (uncached) fingerprint case in {names}"
    assert cached, f"no cached fingerprint case in {names}"


# ---------------------------------------------------------------------------
# Case 3 — the regression that made the published number meaningless
# ---------------------------------------------------------------------------


def test_03_cold_fingerprint_case_never_touches_the_lru_cache(bench: ModuleType) -> None:
    case = _case(bench, "fingerprint:simple_select (cold)")
    # Prime the cache so a memoized call would be a *hit*, not a miss.
    case.fn()
    before = _fingerprint_cached.cache_info()
    for _ in range(5):
        case.fn()
    after = _fingerprint_cached.cache_info()
    assert after.hits == before.hits, "cold benchmark is being served by the LRU cache"
    assert after.misses == before.misses, "cold benchmark is populating the LRU cache"


def test_03b_cold_fingerprint_case_returns_the_same_result_as_the_public_api(
    bench: ModuleType,
) -> None:
    """The cold path must be the same computation, not a cheaper stand-in."""
    case = _case(bench, "fingerprint:simple_select (cold)")
    sql = bench.QUERIES["simple_select"]
    assert case.fn() == fingerprint(sql)


# ---------------------------------------------------------------------------
# Case 4
# ---------------------------------------------------------------------------


def test_04_cached_fingerprint_case_registers_lru_hits(bench: ModuleType) -> None:
    case = _case(bench, "fingerprint:simple_select (cached)")
    case.fn()  # ensure the entry exists
    before = _fingerprint_cached.cache_info()
    for _ in range(5):
        case.fn()
    after = _fingerprint_cached.cache_info()
    assert after.hits == before.hits + 5
    assert after.misses == before.misses


# ---------------------------------------------------------------------------
# Cases 5-6 — the regression that published 0.5 µs for a None short-circuit
# ---------------------------------------------------------------------------


def test_05_percentiles_case_measures_a_full_window(bench: ModuleType) -> None:
    case = _case(bench, "ringbuffer:percentiles")
    result = case.fn()
    assert isinstance(result, Percentiles), "percentiles benchmark timed a None short-circuit"
    assert result.sample_count == bench.PERCENTILE_WINDOW_SAMPLES


def test_06_percentiles_case_is_independent_of_elapsed_wall_clock(
    bench: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _case(bench, "ringbuffer:percentiles")
    # Pretend the earlier cases took an hour: a wall-clock-dependent benchmark
    # would now see every sample as expired and time a None return instead.
    monkeypatch.setattr("time.monotonic", lambda: 1e9)
    result = case.fn()
    assert isinstance(result, Percentiles)
    assert result.sample_count == bench.PERCENTILE_WINDOW_SAMPLES


# ---------------------------------------------------------------------------
# Case 7
# ---------------------------------------------------------------------------


def test_07_run_case_reports_executed_iterations_and_positive_cost(bench: ModuleType) -> None:
    case = bench.BenchCase(name="noop", fn=lambda: None, iterations=2_000)
    result = bench.run_case(case)
    assert result.operation == "noop"
    assert result.iterations == 2_000
    assert result.us_per_op > 0
    assert result.ops_per_sec > 0
    assert result.us_per_op <= result.us_per_op_median


# ---------------------------------------------------------------------------
# Case 8
# ---------------------------------------------------------------------------


def test_08_format_table_renders_every_row_without_truncating_names(bench: ModuleType) -> None:
    results = [
        bench.BenchResult(
            operation="a:short",
            iterations=10,
            us_per_op=1.5,
            us_per_op_median=1.75,
            ops_per_sec=666_667,
        ),
        bench.BenchResult(
            operation="fingerprint:parse_fallback (cold)",
            iterations=5_000,
            us_per_op=188.0,
            us_per_op_median=203.5,
            ops_per_sec=5_319,
        ),
    ]
    table = bench.format_table(results)
    lines = table.splitlines()
    header = [line for line in lines if "ops/sec" in line]
    assert header, table
    assert header[0].count("us/op") == 2, f"both estimators must be shown: {header[0]!r}"
    for result in results:
        row = [line for line in lines if line.startswith(result.operation)]
        assert row, f"missing row for {result.operation}: {table}"
        assert str(result.us_per_op) in row[0]
        assert str(result.us_per_op_median) in row[0]
    assert "fingerprint:parse_fallback (cold)" in table


# ---------------------------------------------------------------------------
# Case 9
# ---------------------------------------------------------------------------


def test_09_summarize_publishes_the_fastest_batch_and_carries_the_median(
    bench: ModuleType,
) -> None:
    # Batches in seconds-per-op: a contended host produced two slow batches.
    samples = [3e-6, 1e-6, 2e-6, 9e-6, 4e-6]
    result = bench.summarize("op", iterations=5_000, per_op_samples=samples)
    assert result.operation == "op"
    assert result.iterations == 5_000
    assert result.us_per_op == pytest.approx(1.0), "headline must be the fastest batch"
    assert result.us_per_op_median == pytest.approx(3.0)
    assert result.ops_per_sec == pytest.approx(1_000_000), "ops/sec derives from the headline"


def test_09b_summarize_rejects_an_empty_sample_list(bench: ModuleType) -> None:
    with pytest.raises(ValueError):
        bench.summarize("op", iterations=0, per_op_samples=[])


# ---------------------------------------------------------------------------
# Case 10
# ---------------------------------------------------------------------------


def test_10_run_case_honours_a_per_case_batch_size(bench: ModuleType) -> None:
    # 500 iterations is below the default batch, so only a case-level batch can
    # produce the 5 short bursts (and the exact 500 executed ops) asserted here.
    case = bench.BenchCase(name="noop", fn=lambda: None, iterations=500, batch=100)
    result = bench.run_case(case)
    assert result.iterations == 500


def test_10b_expensive_cases_are_timed_in_short_bursts(bench: ModuleType) -> None:
    """A cold sqlglot parse is ~1000x a dict lookup; its bursts must be shorter."""
    cold = _case(bench, "fingerprint:simple_select (cold)")
    cached = _case(bench, "fingerprint:simple_select (cached)")
    assert cold.batch < cached.batch, (
        f"cold batch {cold.batch} is not shorter than cached batch {cached.batch}"
    )
