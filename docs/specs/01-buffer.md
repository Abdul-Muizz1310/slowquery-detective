# 01 — Ring buffer & percentile computation

## Goal

Keep a sliding 60-second window of query durations per fingerprint so the middleware can compute p50/p95/p99 on demand without any external state store. Memory must stay bounded even under sustained high QPS per fingerprint. Threading must be safe because SQLAlchemy event hooks fire on whichever worker thread executed the cursor.

## Module

`package/src/slowquery_detective/buffer.py`

## Public API

```python
class RingBuffer:
    def __init__(self, window_seconds: float = 60.0, max_samples_per_key: int = 1024) -> None: ...
    def record(self, fingerprint_id: str, duration_ms: float, now: float | None = None) -> None: ...
    def percentiles(self, fingerprint_id: str, now: float | None = None) -> Percentiles | None: ...
    def keys(self) -> frozenset[str]: ...
    def clear(self, fingerprint_id: str | None = None) -> None: ...

class Percentiles(NamedTuple):
    sample_count: int                  # ``count`` would shadow tuple.count
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
```

- `record` evicts in-place on call; `percentiles` is a pure read that also evicts expired samples before computing.
- `now` is injected for deterministic tests; production code leaves it `None` (uses `time.monotonic()`).
- `percentiles` returns `None` when the fingerprint has zero live samples.

## Inputs

- `fingerprint_id: str` — opaque; pass-through from `fingerprint.py`. Not validated beyond non-empty.
- `duration_ms: float` — **must be ≥ 0**. Negative values raise `ValueError`.
- `window_seconds: float` — constructor-only. Default 60.0.
- `max_samples_per_key: int` — constructor-only. Default 1024 (reservoir cap).

## Outputs / Invariants

1. **Sliding window** — samples older than `window_seconds` are never counted in `percentiles(...)`.
2. **Bounded memory** — at most `max_samples_per_key` samples live per fingerprint; beyond that, a random-index reservoir replacement is used so the p95 stays statistically representative.
3. **Thread-safe** — concurrent `record` calls on the same fingerprint from multiple threads never lose updates or raise. A single `threading.Lock` guards the per-key deque.
4. **Isolation** — different fingerprints are independent; percentiles on one never reflect samples from another.
5. **Empty-state** — `percentiles("unknown")` returns `None`, never raises.
6. **Single-sample** — `p50 == p95 == p99 == max == sample`.
7. **Determinism** — given injected `now`, `percentiles` returns the same tuple across repeated calls with the same in-memory state.
8. **No SQL stored** — the buffer never holds the original SQL, parameter values, or anything beyond `(fingerprint_id, duration_ms, timestamp)`.
9. **Clear semantics** — `clear()` wipes all keys; `clear("abc")` wipes only one.

## Enumerated test cases

### Happy path

1. Record 100 samples with durations `[1..100]`; `p50==50`, `p95==95`, `p99==99`, `max==100` (±1 for interpolation).
2. Record 1000 samples at uniform random `[0, 1000)`; `p99` is within `[950, 1000]`.
3. Two fingerprints recorded in interleaved order; each returns the percentiles of its own samples only.
4. `record` then `percentiles` without advancing time → all samples counted.

### Edge cases

5. Eviction — record at `now=0, 30, 59`; query at `now=60` → all still counted. Query at `now=61` → sample from `now=0` evicted.
6. Hard eviction — record at `now=0, 30, 60`, query at `now=121` → all three evicted, returns `None`.
7. Empty buffer — `percentiles("never-recorded")` → `None`.
8. Single sample of 42.0 → `Percentiles(sample_count=1, p50_ms=42, p95_ms=42, p99_ms=42, max_ms=42)`.
9. Reservoir cap — record 10,000 samples (window not expired) with increasing values; `len(internal_samples) <= max_samples_per_key`; `p95` is still representative (within 10% of expected).
10. `max_samples_per_key=1` with 100 samples → always holds exactly one; percentiles reflect the reservoir-chosen sample.
11. `clear("fp1")` then `percentiles("fp1")` → `None`; other fingerprints untouched.
12. `clear()` with no argument wipes every key.
13. `keys()` returns a `frozenset` snapshot; mutation of the buffer after does not mutate the returned set.

### Failure cases

14. `record("fp", -0.1)` → `ValueError`.
15. `record("fp", float("nan"))` → `ValueError`.
16. `record("fp", float("inf"))` → `ValueError`.
17. `record("", 1.0)` → `ValueError` (empty fingerprint).
18. `window_seconds=0` at construction → `ValueError`.
19. `max_samples_per_key=0` at construction → `ValueError`.
20. `percentiles` called on an expired-but-not-yet-swept key returns `None` and leaves internal state empty.

### Concurrency / stress

21. 8 threads × 10,000 `record` calls on the same fingerprint. Post-condition: no exceptions, `sample_count == 80,000` *or* `sample_count == max_samples_per_key`, no deadlock.
22. Interleaved `record` + `percentiles` + `clear` across threads. Invariant: `percentiles` never raises and never returns a tuple containing NaN/Inf.
23. Injected-clock monotonicity — passing a `now` that goes backwards does not cause samples to resurrect. (Implementation must snapshot `now` and not rely on wall time.)

### Security / privacy

24. The buffer's `__repr__` and any serialization helpers never include SQL text or parameter values (only counts and percentiles).
25. Memory profile: 10,000 fingerprints × `max_samples_per_key=1024` stays under 200 MB RSS in a stress fixture. (Sanity check, not a hard CI gate.)

## Acceptance criteria

- [ ] `RingBuffer` + `Percentiles` named-tuple exported from `buffer.py`.
- [ ] Tests 1–25 pass; tests 21–22 marked `@pytest.mark.slow` if runtime exceeds 2s.
- [ ] Property test: reservoir p95 within 10% of exact p95 on a 10,000-sample run.
- [ ] No `time.time()` calls inside the module — only `time.monotonic()` or the injected `now`.
- [ ] mypy-strict clean.
