# 02 — SQLAlchemy event hooks

## Goal

Wire `before_cursor_execute` and `after_cursor_execute` listeners onto a SQLAlchemy `Engine` (or `AsyncEngine`) so every statement is timed, fingerprinted, and pushed into the ring buffer. The hooks must add negligible overhead on the happy path, catch and log any failure without poisoning the host request, and work identically for sync and async engines.

## Module

`package/src/slowquery_detective/hooks.py`

## Public API

```python
def attach(engine: Engine | AsyncEngine, buffer: RingBuffer, *, sample_rate: float = 1.0) -> None: ...
def detach(engine: Engine | AsyncEngine) -> None: ...
```

- `attach` is idempotent: calling it twice on the same engine registers listeners once and logs a warning.
- `detach` removes the listeners and is a no-op if nothing is attached.
- `sample_rate` — `0.0`..`1.0`; statements that lose the coin flip are skipped before fingerprinting (optimization).

## Inputs

- `engine` — any SQLAlchemy 2.x `Engine` or `AsyncEngine`. Must not be `None`.
- `buffer` — an instance of `RingBuffer` from `01-buffer.md`.
- `sample_rate` — float in `[0.0, 1.0]`; default 1.0.

## Outputs / Invariants

1. **Per-statement timing** — duration measured with `time.perf_counter()` in `before_cursor_execute` / `after_cursor_execute`. The delta is recorded in ms via `buffer.record(fingerprint_id, duration_ms)`.
2. **Async support** — for `AsyncEngine`, listeners attach to `engine.sync_engine` (SQLAlchemy's published pattern); no separate async listener API is exercised.
3. **Zero propagation** — any exception inside a hook is caught, logged via `structlog`, and swallowed. The host statement still completes normally.
4. **Connection-local state** — the start time is attached to the `cursor.info` dict, never to a global, so concurrent connections do not race.
5. **Nested-transaction safety** — statements executed inside SAVEPOINTs are counted once, at the leaf.
6. **Sampling** — when `sample_rate < 1.0`, the coin flip happens *before* fingerprinting so we don't spend sqlglot cycles on dropped statements. The fingerprint skip must be deterministic per-statement (uses `random.Random` seeded once at attach, not `random.random()`).
7. **Idempotent attach** — a second `attach(engine, ...)` call logs `"slowquery.hooks.already_attached"` and returns without re-registering.
8. **Detach cleanliness** — `detach` unregisters both listeners and removes the internal registration marker, so a subsequent `attach` succeeds silently.
9. **No params logged** — bound parameters are never read by the hook; only the statement text is fingerprinted.
10. **Overhead budget** — added latency is ≤ 50 µs per statement on a modern laptop (measured, not just asserted).

## Enumerated test cases

### Happy path (integration — testcontainers Postgres)

1. Attach to a sync `Engine`, run `SELECT 1`, assert the fingerprint lands in the buffer with a non-zero duration.
2. Attach to an `AsyncEngine` over asyncpg, run `await conn.execute(text("SELECT 1"))`, assert the fingerprint lands in the buffer.
3. Run 1000 varied `SELECT` statements; `buffer.keys()` size matches the number of distinct fingerprints, not 1000.
4. Run the same parameterized query 50 times (via bound parameters); exactly one fingerprint is tracked with `count=50`.
5. A `BEGIN; INSERT; COMMIT;` sequence records only the `INSERT` fingerprint, not the transaction control statements *unless* they're explicitly executed as text (document the chosen behavior).

### Edge cases

6. Attaching to the same engine twice → one warning log, no duplicated records (run a query after; `count=1`, not `2`).
7. `detach` then `attach` again → hooks work normally; no stale listeners.
8. `sample_rate=0.0` → run 100 queries, `buffer.keys()` is empty.
9. `sample_rate=0.5` over 10,000 queries → buffer count is within 5% of 5,000 (binomial tolerance).
10. DDL — `CREATE INDEX ...` is fingerprinted and recorded like any other statement.
11. A statement that raises at the DB (`SELECT 1/0`) still fires `after_cursor_execute` *via* SQLAlchemy's handler path; the hook records a duration and the error propagates to the caller.
12. Hooks fire on `engine.begin()` context manager usage, `engine.connect()`, and `session.execute()` uniformly.

### Failure cases

13. `attach(None, buffer)` → `ValueError`.
14. `attach(engine, None)` → `ValueError`.
15. `sample_rate=-0.1` or `sample_rate=1.1` → `ValueError`.
16. Hook raises internally (monkeypatched `buffer.record` to throw) → host query still returns the row; error logged with `exc_info=True`.
17. `fingerprint()` raises on a pathological statement — hook catches, logs, and skips the sample rather than propagating.

### Concurrency

18. 8 parallel worker threads hammering the same engine. Post-condition: no `KeyError` on `cursor.info`, no negative durations, `buffer.percentiles(fp)` returns a coherent tuple.
19. Async: 100 concurrent `asyncio.gather(...)` calls on an `AsyncEngine`. Same invariants.

### Security

20. Hook never calls `str(parameters)` or otherwise touches parameter values. Asserted via a monkeypatch that raises if parameters are read.
21. Hook never logs the raw SQL at `INFO` — only fingerprint id + duration. (SQL may be logged at `DEBUG` in dev mode via an explicit flag.)
22. Connection-string secrets never appear in any log record emitted by the hook module.

### Performance

23. Overhead benchmark: 10,000 `SELECT 1` calls with the hook attached vs without → average added latency ≤ 50 µs/statement. Marked `@pytest.mark.slow`.

## Acceptance criteria

- [ ] `attach` / `detach` exported from `hooks.py`.
- [ ] Tests 1–23 pass; integration tests gated behind `@pytest.mark.integration` and use testcontainers Postgres.
- [ ] Idempotency covered (test 6).
- [ ] Both sync and async `Engine` covered (tests 1, 2, 19).
- [ ] Hook exception containment covered (test 16).
- [ ] mypy-strict clean, no `# type: ignore` in this module.
