# 05 — Middleware & dashboard router

## Goal

A 3-line public integration that wires every previous slice — fingerprinting, ring buffer, SQLAlchemy hooks, rules engine, LLM fallback, and store writer — onto a FastAPI app. Optionally mount a read-only dashboard API router for the Next.js frontend to consume. Must refuse to execute arbitrary DDL; the "apply" action is strictly scoped to a hard-coded allowlist.

## Module

`package/src/slowquery_detective/middleware.py` + `package/src/slowquery_detective/dashboard.py`

## Public API

```python
def install(
    app: FastAPI,
    engine: AsyncEngine,
    *,
    threshold_ms: int = 100,
    sample_rate: float = 1.0,
    store_url: str | None = None,
    enable_llm: bool = False,
    llm_config: LlmConfig | None = None,
) -> None:
    """Attach slowquery-detective to a FastAPI app + SQLAlchemy engine."""

dashboard_router: APIRouter          # read-only; mounted by the caller under any prefix
```

Usage — the 3-line integration:

```python
from slowquery_detective import install
install(app, engine)
```

With the dashboard:

```python
from slowquery_detective import install, dashboard_router
install(app, engine)
app.include_router(dashboard_router, prefix="/_slowquery")
```

## Dashboard router routes

```
GET  /api/queries                          # list fingerprints sorted by total_ms desc
GET  /api/queries/{fingerprint_id}         # detail: plan + suggestions + recent samples
POST /api/queries/{fingerprint_id}/apply   # run the suggested DDL (allowlisted only)
GET  /api/stream                           # SSE: live p95 per fingerprint
```

## Inputs / Outputs / Invariants

1. **`install` side effects** — in order: construct a `RingBuffer`, call `hooks.attach(engine, buffer, sample_rate=sample_rate)`, construct and `start()` the `ExplainWorker` (see [`06-explain-worker.md`](06-explain-worker.md) for its contract and tests), wire the hook's slow-threshold check to call `worker.submit(ExplainJob(...))` when `duration_ms >= threshold_ms`, and register `app.on_event("shutdown")` / lifespan handlers to detach the hook and `await worker.stop()`.
2. **Idempotent** — a second `install(app, engine)` call is a no-op with a warning (`"slowquery.middleware.already_installed"`).
3. **Store URL** — if `store_url is None`, uses the engine's own URL. The store always writes to a dedicated schema (`slowquery`) so it never collides with application tables.
4. **DEMO_MODE compatibility** — when the host app's env has `DEMO_MODE=true`, the dashboard router's `/apply` endpoint is enabled; otherwise it returns `403` (applying DDL in production via a web UI is off by default).
5. **DDL allowlist** — `/apply` accepts only suggestions whose `sql` matches the regex `^CREATE INDEX( CONCURRENTLY)? IF NOT EXISTS ix_[A-Za-z0-9_]+ ON [A-Za-z0-9_"]+\s*\([A-Za-z0-9_,\s()]+\);?$`. Anything else → `400`.
6. **Shutdown cleanliness** — on `app.shutdown`, `hooks.detach(engine)` is called, `await worker.stop()` is awaited, and the ring buffer is cleared. The worker's own shutdown contract (5 s in-flight grace, no drain, idempotent stop) lives in [`06-explain-worker.md`](06-explain-worker.md) §shutdown.
7. **LLM integration** — `enable_llm=True` requires `llm_config`; otherwise `ValueError`. The middleware passes the `llm_config`-constructed `explain` callable into `ExplainWorker(explainer=...)`. Rules-first / LLM-second ordering is the worker's responsibility, not the middleware's.
8. **Threshold propagation** — the middleware owns the decision of whether a fingerprint crosses `threshold_ms`. The hook's `after_cursor_execute` path compares `duration_ms >= threshold_ms` and, only on true, calls `worker.submit(ExplainJob(...))`. The worker itself trusts the caller and does not re-check the threshold.
9. **No implicit migrations** — `install` does not create tables. The caller runs Alembic (or the slowquery-demo-backend's migration does it). Documented in README.
10. **Read-only dashboard by default** — `GET` endpoints never trigger database mutations; they read from the store only.

## Scope boundary

The middleware is a **wiring layer**. It constructs components and connects them; it does not re-test their internals. In particular:

- Fingerprinting rules and edge cases → [`00-fingerprint.md`](00-fingerprint.md)
- Buffer eviction, percentiles, concurrency → [`01-buffer.md`](01-buffer.md)
- Hook attach/detach semantics, sync vs. async engines → [`02-hooks.md`](02-hooks.md)
- Rules engine behavior, DDL generation safety → [`03-rules.md`](03-rules.md)
- LLM cascade, cooldown, abstention → [`04-explainer.md`](04-explainer.md)
- **Explain worker rate-limit, plan cache, param substitution, shutdown drain → [`06-explain-worker.md`](06-explain-worker.md)**

The tests below cover the middleware's *own* responsibilities: argument validation, idempotent install, the HTTP surface, the DDL allowlist, and the lifespan-scoped start/stop handshake with the worker.

## Enumerated test cases

### Happy path

1. `install(app, engine)` on a toy FastAPI app + in-memory store → no exceptions, `hooks.attach` was called exactly once, background task started.
2. Issue a slow query (mocked `EXPLAIN` returns a seq-scan plan) → `GET /api/queries` eventually includes the fingerprint with `p95_ms >= threshold_ms`.
3. `GET /api/queries/{id}` returns the fingerprint detail, the cached plan, and at least one suggestion.
4. `dashboard_router` mounted at custom prefix (`/_slowq`) responds; routes under `/api/...` are reachable.

### Idempotency & wiring

5. Calling `install(app, engine)` twice emits a warning and does not double-register hooks.
6. `install` without `store_url` uses the engine URL; a test verifies the store writer connects to the same DB.
7. `install(app, None)` → `ValueError`.
8. `install(None, engine)` → `ValueError`.
9. `enable_llm=True, llm_config=None` → `ValueError`.
10. `threshold_ms=0` → `ValueError`.
11. `sample_rate` out of `[0, 1]` → `ValueError`.

### DDL allowlist

12. `POST /api/queries/{id}/apply` with a `CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);` suggestion → 200 OK, DDL executed against the store engine.
13. Same call with `DROP TABLE users;` → 400, error body `{"error": "ddl_not_allowed"}`, nothing executed.
14. Same call with `CREATE OR REPLACE FUNCTION ...` → 400.
15. Same call with `ALTER TABLE orders ADD COLUMN ...` → 400.
16. Same call with `CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_orders_user_id ON orders(user_id);` → 200.
17. Same call with SQL comments injected (`CREATE INDEX -- harmless ;DROP TABLE ...`) → 400 (regex requires no semicolons in the middle).
18. `/apply` with a fingerprint that has no suggestion → 404.
19. `/apply` in non-demo mode (`DEMO_MODE=false`) → 403.

### Shutdown

20. App shutdown calls `hooks.detach(engine)` and `await worker.stop()` in that order. The middleware test asserts the *call order* via a spy; the worker's own in-flight drain behavior is owned by [`06-explain-worker.md`](06-explain-worker.md) tests 20–22.
21. After shutdown, a further DB call on the engine does not raise from the detached hooks (clean detach).
22. Repeated install/shutdown cycles (3×) do not leak listeners, background tasks, or `app.state` entries.

### SSE stream

23. `GET /api/stream` returns `text/event-stream`, emits one event within 1s of a new recorded query, and closes cleanly on client disconnect.
24. The SSE stream never emits raw SQL text — only fingerprint ids, percentiles, and counts.

### Security

25. `/api/queries*` endpoints never return raw parameter values from `query_samples` — the `params` column is redacted in the response schema.
26. `/api/queries*` endpoints reject unauthenticated requests when `DEMO_MODE=false` (require a valid `X-Platform-Token` per the global §3.4 middleware contract).
27. The allowlist regex is unit-tested separately with at least 20 adversarial strings (injection attempts, unicode lookalikes, whitespace tricks).
28. `/apply` requests are rate-limited to 1 per fingerprint per 10 seconds (in-memory token bucket) to prevent hammering the database with index creations.
29. CORS on the dashboard router is scoped to the known frontend origin (`slowquery-dashboard-frontend.vercel.app` + `http://localhost:3000`); other origins get no `Access-Control-Allow-Origin`.
30. `/apply`'s response includes the exact DDL that was executed so the audit trail is explicit.

## Acceptance criteria

- [ ] `install` + `dashboard_router` exported from `slowquery_detective/__init__.py`.
- [ ] Tests 1–30 pass; FastAPI tests use `httpx.AsyncClient` against the app via `TestClient` or lifespan-aware client.
- [ ] DDL allowlist regex lives in `middleware.py` as a module-level constant, not inlined into an endpoint.
- [ ] Shutdown leaves zero background tasks (asserted with `asyncio.all_tasks()`).
- [ ] README "3-line integration" snippet is copy-pastable and verified by a doctest or CI-run snippet script.
- [ ] mypy-strict clean.
