# 06 — Explain worker

## Goal

An async background task that pulls slow-fingerprint events off an in-process queue, runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` against a dedicated engine (read replica or the app engine in single-DB mode), caches the resulting plan per fingerprint, feeds it to the rules engine and — on a miss — the LLM explainer, and persists everything to the store. Rate-limited per fingerprint so a noisy endpoint can't run `EXPLAIN ANALYZE` in a tight loop (which would double the real query's latency every time).

## Module

`package/src/slowquery_detective/explain.py`

## Public API

```python
@dataclass(frozen=True)
class ExplainJob:
    fingerprint_id: str
    canonical_sql: str
    observed_ms: float
    enqueued_at: float              # time.monotonic()

class ExplainWorker:
    def __init__(
        self,
        engine: AsyncEngine,                         # where EXPLAIN runs (may be a replica)
        store: StoreWriter,
        rules: Callable[[dict, str, str, int], list[Suggestion]],
        explainer: Callable[..., Awaitable[Suggestion | None]] | None,
        *,
        per_fingerprint_cooldown_seconds: float = 60.0,
        explain_timeout_seconds: float = 10.0,
        max_queue_size: int = 256,
        now: Callable[[], float] = time.monotonic,
    ) -> None: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def submit(self, job: ExplainJob) -> bool: ...
    def plan_cache_get(self, fingerprint_id: str) -> CachedPlan | None: ...

@dataclass(frozen=True)
class CachedPlan:
    plan_json: dict
    plan_text: str
    cost: float
    captured_at: float
    suggestions: tuple[Suggestion, ...]
```

- `submit` is **non-blocking** and **synchronous** — called from the SQLAlchemy hook, which executes in sync context inside SQLAlchemy's cursor handler. Returns `True` if the job was queued, `False` if the queue was full (drop, log at `DEBUG`, don't block the request).
- `start` / `stop` are called from the middleware's lifespan handlers.
- The worker drains the queue in a single asyncio task; concurrent `EXPLAIN` execution is not a goal (the point is visibility, not throughput).

## Inputs / Outputs / Invariants

1. **Off the request path** — nothing the worker does blocks a request. The hook only calls `submit(job)`, which is a `queue.put_nowait` equivalent.
2. **Async context** — `ExplainWorker` runs inside the app's asyncio loop (started in the `install()` lifespan). It uses `asyncio.Queue`, not `queue.Queue`, but `submit` uses `put_nowait` so it can be called from the sync SQLAlchemy hook via `loop.call_soon_threadsafe`.
3. **Per-fingerprint cooldown** — after an `EXPLAIN` completes for a fingerprint, any further `submit` for that fingerprint is dropped until `cooldown` elapses. This prevents a hot fingerprint from re-running `EXPLAIN ANALYZE` every 100 ms and doubling prod latency.
4. **Timeout** — an `EXPLAIN` call is wrapped in `asyncio.wait_for(..., timeout=explain_timeout_seconds)`. Timeouts are logged, the fingerprint is still marked "cooling down", and no plan is cached.
5. **Parameter hack** — `EXPLAIN ANALYZE` needs real values for bound parameters. Because the fingerprint has already stripped literals, the worker runs `EXPLAIN` against the **canonical** SQL with `?` replaced by representative samples *only for types it can synthesize* (integers = `1`, text = `''`, booleans = `true`, dates = `now()`, unknowns = `NULL`). If substitution would produce invalid SQL, the worker falls back to `EXPLAIN` (without `ANALYZE`) — plan is less accurate but never crashes. Documented in the spec; not pretty, but necessary.
6. **Plan cache** — keyed by `fingerprint_id`. Only the most recent plan is kept per fingerprint. The cache is in-process (a `dict`), not Redis; survives across requests but not across process restarts.
7. **Rules-first, LLM-second** — the worker calls `rules(plan, canonical_sql, fingerprint_id, recent_call_count)` first. If the result is empty *and* `explainer` is not `None`, it awaits `explainer(canonical_sql, plan_json, ...)`. The LLM call has its own cooldown (see `04-explainer.md`); the worker does not duplicate it.
8. **Persistence** — every plan and every suggestion is written to the store via `store.upsert_plan(...)` and `store.insert_suggestions(...)`. Failures to persist are logged and swallowed; the plan cache still holds the result.
9. **Backpressure** — if `queue.qsize() == max_queue_size`, `submit` returns `False` immediately. No blocking, no dropping an older job to make room. Log at `DEBUG` with a counter metric.
10. **Graceful shutdown** — `stop()` cancels the worker task, waits up to 5 s for in-flight `EXPLAIN` to finish, then hard-cancels. No outstanding `asyncio.Task` after `stop()` returns.
11. **No hook access** — the worker never touches the ring buffer directly. The cooldown map is the worker's own state.
12. **Idempotent stop** — calling `stop()` twice is a no-op.

## Enumerated test cases

### Happy path

1. Start the worker, submit one `ExplainJob`, advance the event loop → plan is fetched (mock engine returns a fixture plan), rules called, suggestions written to the store, `plan_cache_get(fingerprint_id)` returns the `CachedPlan`.
2. Submit three jobs for three different fingerprints → all three processed; all three present in the plan cache.
3. The plan JSON returned by the mock engine appears verbatim in `store.upsert_plan` (asserted via a spy).
4. When rules return an empty list and `explainer` is provided, `explainer` is awaited exactly once with `(canonical_sql, plan_json, ...)`; its result is persisted.
5. When rules return a non-empty list, `explainer` is **not** called.
6. `explainer=None` + empty rules result → no suggestion written, but the plan itself is still cached and stored.

### Cooldown

7. Submit the same fingerprint twice in 1 s → first is processed, second is dropped (`submit` returns `True` for queuing but the worker skips it on dequeue because cooldown is active).
8. Advance the injected `now` by 61 s → a subsequent submit is processed.
9. Different fingerprints do not share cooldown.
10. The cooldown starts at the moment `EXPLAIN` **finishes**, not when the job was enqueued (a slow EXPLAIN must not cause back-to-back re-runs).

### Backpressure / queue

11. Fill the queue to `max_queue_size`, then call `submit(...)` once more → returns `False`, logs at `DEBUG`, queue size unchanged.
12. Drop events never affect already-queued events (FIFO).
13. `submit` never blocks for more than 1 ms under any circumstance (asserted with a monotonic clock around the call).
14. Queue is drained in FIFO order.

### Timeout / errors

15. `EXPLAIN` hangs (mock takes 30 s) + `explain_timeout_seconds=1` → `asyncio.TimeoutError` caught, logged, fingerprint enters cooldown, no plan cached, worker continues processing the next job.
16. `EXPLAIN` raises a DB error (`InvalidTextRepresentation` from a failed param substitution) → worker retries once with `EXPLAIN` (no `ANALYZE`). If that also fails, logs and cools down.
17. `store.upsert_plan` raises → error logged with `exc_info=True`, plan cache still updated, worker continues.
18. `rules(...)` raises → worker logs, treats as empty result, still calls `explainer` if configured.
19. `explainer(...)` raises → worker logs, persists the plan without suggestions, does not crash.

### Shutdown

20. Call `stop()` while a job is in flight → the in-flight `EXPLAIN` completes (up to `explain_timeout_seconds`), then the worker task exits cleanly; `asyncio.all_tasks()` in the worker's loop contains no worker tasks after `stop()` returns.
21. Call `stop()` when the queue has 10 pending jobs → the remaining jobs are **not** processed (drain-on-shutdown is off by design); the drop count is logged.
22. Call `stop()` twice → no exception, second call is a no-op.
23. Call `start()` after `stop()` → worker restarts cleanly.

### Parameter substitution

24. Canonical SQL `SELECT * FROM users WHERE id = ?` → substituted as `SELECT * FROM users WHERE id = 1` before `EXPLAIN`. The test asserts the exact substituted SQL via an engine spy.
25. Canonical SQL `WHERE email = ?` → substituted as `WHERE email = ''`; if this produces an empty-result plan, the worker still caches it (a fast empty plan is still "the plan for this shape").
26. Canonical SQL with an IN list `WHERE id IN (?)` → substituted as `WHERE id IN (1)`.
27. Canonical SQL with an unknown parameter position (e.g., JSON path access) → substitution gives up, worker runs plain `EXPLAIN` (no `ANALYZE`); test asserts the fallback path was taken.
28. `EXPLAIN`'s own canonical SQL never contains a raw literal from the *original* query (only the synthesized ones).

### Security

29. The canonical SQL passed to `EXPLAIN` has already been parameter-scrubbed by `fingerprint.py`; the worker never sees, logs, or stores the literal values that the user's original query ran with. Verified by a test that submits a job whose *original* SQL contained `'sk-live-secret'` — the string never appears in any log record, the plan cache, or the store writer's arguments.
30. The parameter synthesizer never produces DDL-looking SQL. Asserted by a parametrized test over ~20 canonical SQL shapes.
31. `EXPLAIN` is run on the `engine` passed to the worker — which is the same one the app uses by default. When a host app wants to run it against a read replica, they pass a different engine to `ExplainWorker(engine=...)`; nothing in the worker hard-codes the primary.
32. No user-controlled string is ever format-concatenated into a `text()` SQL call. All substitutions go through a small state machine with a fixed set of placeholder types.

### Performance

33. Overhead budget: with the worker running but the queue empty, the hook's `submit` call adds ≤ 10 µs per statement (benchmarked; `@pytest.mark.slow`).
34. A 100-job burst (all unique fingerprints, mock engine with 10 ms EXPLAIN latency) completes in ≤ 1.2 s wall-clock — i.e., processing is effectively sequential with no additional queueing overhead beyond the mock's own latency.

## Acceptance criteria

- [ ] `ExplainWorker`, `ExplainJob`, `CachedPlan` exported from `explain.py`.
- [ ] Tests 1–34 pass. Integration tests (24–28, 33) use testcontainers Postgres and are gated `@pytest.mark.integration`.
- [ ] `submit` is sync and never blocks > 1 ms (test 13).
- [ ] Per-fingerprint cooldown is enforced (tests 7–10).
- [ ] Shutdown leaks no tasks (test 20).
- [ ] Security test 29 grep-asserts that no literal from the original statement survives to any sink.
- [ ] Parameter substitution is implemented as a small pure function with its own unit tests (tests 24–27, 30).
- [ ] mypy-strict clean.
