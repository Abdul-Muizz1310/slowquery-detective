"""Micro-benchmarks for the slowquery-detective hot path (no database).

Measures the per-query overhead the middleware adds on the request path:
fingerprinting (sqlglot parameterization), the rules engine, and the ring
buffer. These are the only pieces that run synchronously on every query;
EXPLAIN runs off the hot path on a background worker, so it is not measured
here.

Run with::

    uv run python benchmarks/bench_detective.py

Numbers are machine-dependent; commit the host + Python version alongside any
recorded result (see benchmarks/report.md).
"""

from __future__ import annotations

import platform
import statistics
import sys
import time
from collections.abc import Callable

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.fingerprint import fingerprint
from slowquery_detective.rules.base import run_rules

_QUERIES = {
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


def _bench(name: str, fn: Callable[[], object], iterations: int) -> dict[str, object]:
    """Time ``fn`` over ``iterations`` calls; return median per-op cost."""
    for _ in range(min(1000, iterations)):  # warm up
        fn()
    batch = 1000
    rounds = max(1, iterations // batch)
    per_op_samples: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        for _ in range(batch):
            fn()
        per_op_samples.append((time.perf_counter() - start) / batch)
    per_op = statistics.median(per_op_samples)
    return {
        "operation": name,
        "iterations": rounds * batch,
        "us_per_op": round(per_op * 1e6, 2),
        "ops_per_sec": round(1.0 / per_op),
    }


def main() -> None:
    rb = RingBuffer()
    for i in range(1024):  # fill one key to the reservoir cap for the sort path
        rb.record("warm", float(i % 200))

    results: list[dict[str, object]] = []
    for label, sql in _QUERIES.items():
        results.append(_bench(f"fingerprint:{label}", lambda sql=sql: fingerprint(sql), 20_000))
    results.append(
        _bench(
            "run_rules:6_rules",
            lambda: run_rules(
                _SEQ_SCAN_PLAN,
                "select * from orders where user_id = ?",
                fingerprint_id="fp",
                recent_call_count=0,
            ),
            50_000,
        )
    )
    results.append(_bench("ringbuffer:record", lambda: rb.record("hot", 12.5), 200_000))
    results.append(_bench("ringbuffer:percentiles", lambda: rb.percentiles("warm"), 50_000))

    print("# slowquery-detective hot-path microbenchmark")
    print(f"# {platform.platform()} | Python {sys.version.split()[0]}")
    width = max(len(str(r["operation"])) for r in results)
    print(f"{'operation':<{width}}  {'iters':>8}  {'us/op':>9}  {'ops/sec':>12}")
    for r in results:
        print(
            f"{r['operation']:<{width}}  {r['iterations']:>8}  "
            f"{r['us_per_op']:>9}  {r['ops_per_sec']:>12,}"
        )


if __name__ == "__main__":
    main()
