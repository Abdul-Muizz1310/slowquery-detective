# Benchmarks — hot-path overhead

Exactly two things run synchronously on the request path, both inside the
SQLAlchemy `after_cursor_execute` listener in `hooks.py`:

1. `fingerprint(statement)` — memoized on `(sql, dialect)` via a bounded LRU
2. `RingBuffer.record(...)`

plus, only for a statement that crossed `threshold_ms`, a non-blocking
`ExplainWorker.submit(...)` queue put (budgeted separately by test case 33 in
`tests/integration/test_explain_worker.py`).

Everything else is **off** the hot path and is measured here only to bound what
it costs elsewhere: `EXPLAIN` and the rules engine run on the background worker
(`ExplainWorker._process_one`), and `RingBuffer.percentiles` is called only by
the dashboard (`dashboard.py`), never per query.

Reproduce with:

```bash
uv run python benchmarks/bench_detective.py
```

## Measured 2026-08-02

Host: `Windows-11-10.0.26200-SP0`, AMD Ryzen 7 PRO 5850U (8 cores / 16 threads),
Python 3.12.12.

Estimator: the **fastest** timed burst per case — the estimator `timeit`'s own
docs recommend, since bursts slower than the minimum measure other processes
rather than this code. The median burst is printed beside it so a noisy host is
visible instead of silently inflating the published figure.

One caveat, stated because it matters for these particular numbers: this is a
shared laptop that was running other builds and test suites while the benchmark
ran. The command was executed **eight consecutive times**; the last column is the
full min-to-max range of the µs/op figure across those eight runs, and it spans
up to 3.9x. Every figure here is therefore an upper bound for an idle machine,
and the shape of the table — not the third significant digit of any one cell —
is what to read.

| Operation | µs/op | median burst | ops/sec | µs/op across 8 runs |
|---|--:|--:|--:|--:|
| `fingerprint` **cold** — simple `SELECT … WHERE` | 302.57 | 326.18 | ~3.3k | 303–1033 |
| `fingerprint` **cold** — 2-table JOIN + ORDER BY | 696.69 | 748.21 | ~1.4k | 666–2540 |
| `fingerprint` **cold** — 10-element `IN (…)` list | 388.83 | 428.02 | ~2.6k | 389–1429 |
| `fingerprint` **cold** — parse failure → regex fallback | 172.18 | 185.62 | ~5.8k | 172–565 |
| `fingerprint` **cached** — LRU hit, simple `SELECT` | 0.18 | 0.18 | ~5.5M | 0.18–0.65 |
| `run_rules` — all 6 rules over a plan *(worker)* | 9.82 | 10.54 | ~102k | 9.8–35.8 |
| `RingBuffer.record` | 0.46 | 0.49 | ~2.2M | 0.46–1.78 |
| `RingBuffer.percentiles`, 1024-sample window *(dashboard)* | 85.15 | 96.21 | ~11.7k | 85–296 |

The µs/op, median-burst and ops/sec columns all come from one single run — the
quietest of the eight, which holds the eight-run minimum on every row but the
JOIN (another run reached 666 µs there). Quoting one run rather than a per-row
best-of keeps the table internally consistent. Reproduced verbatim:

```
# slowquery-detective hot-path microbenchmark
# Windows-11-10.0.26200-SP0 | Python 3.12.12
operation                              iters    us/op min    us/op p50       ops/sec
fingerprint:simple_select (cold)        2000       302.57       326.18         3,305
fingerprint:join (cold)                 2000       696.69       748.21         1,435
fingerprint:in_list (cold)              2000       388.83       428.02         2,572
fingerprint:parse_fallback (cold)       2000       172.18       185.62         5,808
fingerprint:simple_select (cached)     20000         0.18         0.18     5,527,916
run_rules:6_rules                      50000         9.82        10.54       101,874
ringbuffer:record                     200000         0.46         0.49     2,151,000
ringbuffer:percentiles                 20000        85.15        96.21        11,743
```

## What changed since the previous recorded figures (2026-06-26)

Two of the seven rows in the previous version of this file had drifted far
enough that they described something other than the work they named. Both drifts
came from code landing after the numbers were captured, with the benchmark
artifacts never re-run.

**`fingerprint` was memoized** on 2026-07-04 (commit `ec112df`: a bounded LRU on
`(sql, dialect)`). The harness re-times a fixed set of four SQL strings, so from
that commit onward every timed call was a cache **hit**: this file said 334 µs
and the documented command printed 0.18 µs — three orders of magnitude apart,
and the benchmark had stopped measuring a parse at all. The harness now measures
both paths: `(cold)` calls the uncached core so the sqlglot parse runs on every
iteration, `(cached)` measures the LRU hit. The cold figures land close to the
pre-memoization ones (303 vs 334, 697 vs 743, 389 vs 447, 172 vs 188 µs), which
is the expected outcome — memoization did not make parsing cheaper, it made it
rarer.

**`RingBuffer.percentiles` was recorded at 0.5 µs**, which was an artifact of a
since-fixed correctness bug rather than a real measurement. `percentiles()` used
to overwrite each survivor's timestamp with a shared sentinel, collapsing the
sliding window to "since the last read"; every call after the first therefore
found an empty window and returned `None` immediately. The benchmark was timing
that short-circuit. With the window intact, reading percentiles over a full
1024-sample window costs ~85 µs (a scan of the deque plus a `sorted`). The
harness now pins the clock for both the fill and the read so this case cannot
silently degenerate into an empty-window measurement again, and
`tests/unit/test_benchmarks.py` asserts the window really is full when timed.

`run_rules` and `RingBuffer.record` were consistent with the previous figures.

## What this means

- **On the statement shapes an app actually emits, per-query overhead is
  effectively free.** SQLAlchemy emits a small, stable set of parameterized
  templates, so the fingerprint LRU hits: 0.18 µs for the fingerprint plus
  0.46 µs for the buffer record is ~0.64 µs per query.
- **The first sighting of a new statement shape pays a real sqlglot parse** —
  ~0.3 ms for a flat `SELECT` up to ~0.7 ms for a 2-table join, scaling with
  query complexity. It stays under the library's ≤1 ms/statement overhead budget
  (`tests/integration/test_hooks.py`), and the parse-failure path is the cheapest
  of the four because it skips straight to the regex fallback. The number of
  distinct shapes that can pay it is bounded by the LRU's 2048 entries.
- **Neither the rules engine nor the percentile read is on the request path.**
  `run_rules` (~10 µs) runs on the background `ExplainWorker`; `percentiles`
  (~85 µs over a full window) runs only when the dashboard is rendered. Earlier
  revisions of this file and of the README summed a percentile read into a
  "~11 µs/query" per-query total; that was wrong twice over — the read is not
  per-query, and it does not cost 0.5 µs.
- Numbers are machine-dependent and were taken on a loaded host. Re-run on your
  target before quoting them anywhere.
