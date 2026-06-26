# Benchmarks — hot-path overhead

`slowquery-detective` runs three things synchronously on the request path:
fingerprint the SQL, record the duration in the ring buffer, and (for a query
that crosses the slow threshold) run the rules engine over its plan. EXPLAIN
itself runs **off** the hot path on a background worker, so it is excluded here.

Reproduce with:

```bash
uv run python benchmarks/bench_detective.py
```

## Measured (median per operation)

Host: `Windows-11-10.0.26200`, Python 3.12.12. Numbers are machine-dependent;
re-run on your target to get comparable figures.

| Operation | µs/op | ops/sec |
|---|--:|--:|
| `fingerprint` — simple `SELECT … WHERE` | 334 | ~3.0k |
| `fingerprint` — 2-table JOIN + ORDER BY | 743 | ~1.3k |
| `fingerprint` — 10-element `IN (…)` list | 447 | ~2.2k |
| `fingerprint` — parse failure → regex fallback | 188 | ~5.3k |
| `run_rules` — all 6 rules over a plan | 10 | ~100k |
| `RingBuffer.record` | 0.8 | ~1.2M |
| `RingBuffer.percentiles` (1024-sample window) | 0.5 | ~1.9M |

## What this means

- **Per-query bookkeeping is effectively free.** The ring buffer (record +
  percentile read) costs ~1 µs, and the rules engine costs ~10 µs — together
  about **11 µs/query**, well under any request budget.
- **Fingerprinting is the real cost** at ~0.2–0.7 ms, dominated by the
  `sqlglot` parse; it scales with query complexity (a 2-table join is ~2× a
  flat `SELECT`). It stays under the library's ≤1 ms/statement overhead budget
  (`tests/integration/test_hooks.py`), and the parse-failure path is cheaper
  because it skips straight to the regex fallback.

These are the figures the README's "negligible overhead" framing should be
read against: cheap accounting on every query, with the heavier parse bounded
and only triggered where the middleware is configured to fingerprint.
