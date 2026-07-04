# Architecture

## Overview

```mermaid
flowchart TD
    App["Host FastAPI app"] --> MW["install(app, engine)<br/>middleware.py"]
    MW --> Hook["SQLAlchemy event listeners<br/>before_cursor_execute / after_cursor_execute"]
    Hook --> FP["fingerprint.py<br/>sqlglot parameterize → SHA-1"]
    FP --> Ring["RingBuffer<br/>60s sliding window, maxlen cap"]
    Ring --> Detector{"duration_ms<br/>≥ threshold?"}
    Detector -- "fast" --> Discard["No action"]
    Detector -- "slow" --> Queue["asyncio.Queue<br/>non-blocking put_nowait"]
    Queue --> Worker["ExplainWorker<br/>async drain loop"]
    Worker --> Cooldown{"per-fingerprint<br/>cooldown active?"}
    Cooldown -- "yes" --> Skip["Abstain"]
    Cooldown -- "no" --> Synth["synthesize_params<br/>? → typed literals<br/>state machine"]
    Synth --> Explain["EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)<br/>with asyncio.wait_for timeout"]
    Explain --> Rules["run_rules<br/>6 pure rules"]
    Rules -- "matched" --> Cache["In-process plan cache"]
    Rules -- "empty" --> LLM["llm_explainer.explain<br/>OpenRouter cascade"]
    LLM --> Cache
    Cache --> Store["StoreWriter<br/>upsert_plan + insert_suggestions"]
    Store --> DB[("Postgres")]
    DB --> Dash["dashboard_router<br/>APIRouter"]
    Dash --> FE["Next.js frontend"]
```

## Rules dispatch

`run_rules` iterates all six rules in registration order, collects non-`None` results, sorts by confidence descending, and returns. On empty, the worker falls back to the LLM explainer.

```mermaid
flowchart LR
    Plan["EXPLAIN plan JSON +<br/>canonical SQL"] --> R1["seq_scan"]
    R1 --> R2["missing_fk_index"]
    R2 --> R3["sort_without_index"]
    R3 --> R4["select_star"]
    R4 --> R5["n_plus_one"]
    R5 --> R6["function_in_where"]
    R6 --> Collect{"any Suggestions?"}
    Collect -- "yes" --> Sort["Sort by confidence DESC<br/>return list"]
    Collect -- "no" --> LLM["llm_explainer.explain<br/>OpenRouter cascade<br/>(per-fingerprint cooldown)"]
```

## Ring buffer architecture

Each fingerprint gets a sliding-window buffer that retains samples from the last 60 seconds. Memory is capped by a fixed-size `deque(maxlen=max_samples_per_key)` that drops the **oldest** sample on overflow — the correct eviction order for a sliding window (a uniform reservoir over all history keeps ancient timestamps alive and is incompatible with time-window eviction). `percentiles()` filters expired samples in place, preserving each survivor's real arrival time.

```mermaid
flowchart TD
    Record["buffer.record(fingerprint_id, duration_ms)"]
    Record --> Append["Append (timestamp, duration)<br/>deque drops oldest at maxlen"]
    Append --> Stats["On-demand percentiles<br/>evict &lt; now()−60s, then p50/p95/p99"]
    Detect["hook after_cursor_execute:<br/>duration_ms ≥ threshold_ms?"]
    Detect -- "yes" --> Submit["worker.submit(ExplainJob)"]
    Detect -- "no" --> Wait["Track only; no EXPLAIN"]
```

Slow-query detection is a per-observation `duration_ms >= threshold_ms` check (owned by the middleware, spec item 8), not a percentile check — the ring buffer supplies percentiles to the dashboard, it does not gate EXPLAIN.

## Security boundary diagram

PII scrubbing, DDL allowlisting, and identifier validation form three defense layers at different points in the pipeline.

```mermaid
flowchart TD
    Raw["Raw SQL from cursor"] --> FP["fingerprint.py<br/>sqlglot parameterize<br/>ALL literals → ?"]
    FP -->|"canonical SQL only"| Buffer["Ring buffer"]
    FP -->|"canonical SQL only"| Worker["ExplainWorker"]
    Worker --> Rules["run_rules"]
    Rules --> IdCheck{"Identifier validation<br/>^[A-Za-z_][A-Za-z0-9_]*$<br/>+ quote_if_reserved()"}
    IdCheck -- "pass" --> Suggestion["Suggestion with SQL"]
    IdCheck -- "fail" --> Abstain["Rule abstains (None)"]
    Suggestion --> DDL{"DDL_ALLOWLIST_REGEX<br/>CREATE INDEX<br/>[CONCURRENTLY]<br/>IF NOT EXISTS ix_..."}
    DDL -- "match" --> Executable["Executable suggestion"]
    DDL -- "no match" --> Prose["sql field nulled out<br/>diagnostic prose only"]
    Worker --> LLM["LLM explainer"]
    LLM --> Filter{"Response filter:<br/>only CREATE INDEX<br/>passes"}
    Filter -- "pass" --> DDL
    Filter -- "fail" --> Prose
```

## Component map

Every component lives in its own module. Each row references the feature spec it implements.

| Module | Responsibility | Spec |
|---|---|---|
| [`fingerprint.py`](../src/slowquery_detective/fingerprint.py) | Parse via sqlglot, scrub literals + parameters, SHA-1 the canonical form | [`00-fingerprint.md`](specs/00-fingerprint.md) |
| [`buffer.py`](../src/slowquery_detective/buffer.py) | 60s sliding-window ring buffer with a fixed maxlen cap (drops oldest) and p50/p95/p99 on demand | [`01-buffer.md`](specs/01-buffer.md) |
| [`hooks.py`](../src/slowquery_detective/hooks.py) | SQLAlchemy `before_cursor_execute` / `after_cursor_execute` listeners | [`02-hooks.md`](specs/02-hooks.md) |
| [`rules/*.py`](../src/slowquery_detective/rules/) | Six pure rules + `run_rules` dispatcher with confidence-desc ordering | [`03-rules.md`](specs/03-rules.md) |
| [`llm_explainer.py`](../src/slowquery_detective/llm_explainer.py) | OpenRouter cascade with per-fingerprint cooldown and destructive-DDL scrub | [`04-explainer.md`](specs/04-explainer.md) |
| [`middleware.py`](../src/slowquery_detective/middleware.py) | `install()` wires everything onto a FastAPI app + SQLAlchemy engine | [`05-middleware.md`](specs/05-middleware.md) |
| [`explain.py`](../src/slowquery_detective/explain.py) | Async EXPLAIN runner, per-fingerprint rate limit, parameter synthesizer, plan cache | [`06-explain-worker.md`](specs/06-explain-worker.md) |
| [`dashboard.py`](../src/slowquery_detective/dashboard.py) | APIRouter surface + `DDL_ALLOWLIST_REGEX` lockdown constant | [`05-middleware.md`](specs/05-middleware.md) |
| [`store.py`](../src/slowquery_detective/store.py) | Async writer for fingerprints, samples, plans, suggestions | -- (lands with Phase 4b schema) |

## Layering discipline

```
middleware → (hooks | buffer | explain_worker | store | dashboard)
                         ↓
                  rules → llm_explainer
                  ↓
              fingerprint
```

- **`middleware.py` never imports a rule directly.** Rules are registered via `rules/__init__.py` and dispatched through `run_rules`.
- **`hooks.py` never touches the DB.** It only fingerprints + records into the ring buffer. Slow-query detection (threshold comparison + enqueue to the worker) is the middleware's job.
- **`llm_explainer.py` is only called on rules miss.** The explain worker owns the ordering; rules run first and the LLM is consulted only when `run_rules(...) == []`. No rule calls the LLM directly.
- **Rules are pure.** No I/O, no clock, no database. They receive `(plan_json, canonical_sql, fingerprint_id, recent_call_count)` and return a `Suggestion` or `None`.
- **`submit()` is sync and non-blocking.** Called from the SQLAlchemy hook (which runs in sync context even on async engines), it does one `put_nowait` and returns `True`/`False`. The overhead budget is <= 10 us per call; anything heavier starves the request.

## Data flow for a slow query

1. FastAPI handler runs `await session.execute(stmt)`.
2. SQLAlchemy `before_cursor_execute` fires -- `time.perf_counter()` stashed in `cursor.info`.
3. Cursor executes the statement.
4. `after_cursor_execute` fires -- duration calculated -- `fingerprint()` produces `(fingerprint_id, canonical_sql)` -- `buffer.record(fingerprint_id, duration_ms)`.
5. Middleware checks `duration_ms >= threshold_ms`. If yes, `worker.submit(ExplainJob(...))` -- non-blocking.
6. The worker's drain loop dequeues the job, checks its per-fingerprint cooldown (abstains if active), then applies the **read-only guard**: it parses the canonical SQL and drops anything that is not a provably read-only query (see Security invariant 7) before any EXPLAIN. For surviving reads it synthesizes representative parameter values for the canonical SQL (`?` -> `1` / `''` / `true` / `now()` / `NULL` based on column-context heuristics).
7. Worker runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` against the engine, wrapped in `asyncio.wait_for(..., timeout=explain_timeout_seconds)`. On timeout or invalid-literal errors, the worker retries once with plain `EXPLAIN` (no `ANALYZE`), preserving visibility even when the synthesized literal doesn't execute.
8. Plan feeds into `run_rules(plan, canonical_sql)`. Matching rules return `Suggestion` objects sorted by confidence desc. On empty, the worker calls `llm_explainer.explain(...)` if configured (a single deadline bounds the whole PRIMARY->FAST->FALLBACK cascade).
9. The plan + suggestions are cached in-process (`plan_cache_get(fingerprint_id)`) and written to the store via `StoreWriter.upsert_plan` + `StoreWriter.insert_suggestions` (the default store is process-local `InMemoryStoreWriter`).
10. The per-fingerprint cooldown is set to `now() + per_fingerprint_cooldown_seconds` so a hot query can't re-trigger EXPLAIN in a tight loop.
11. Dashboard `GET /api/queries/{id}` reads the cached plan + suggestions and the ring-buffer percentile summary; it never re-runs EXPLAIN. `POST /apply` may flush queued jobs via the worker's public `process_pending()` before reading suggestions.

## Security invariants

1. **No literal content survives anywhere.** `fingerprint()` parameterizes before anything else sees the SQL. Logs, ring buffer, plan cache, store writes, and LLM requests all operate on canonical (`?`-only) forms. A hypothesis property test in [`test_fingerprint.py`](../tests/unit/test_fingerprint.py) asserts this across 200+ random literal shapes per test run.
2. **DDL execution is allowlist-gated *and* fingerprint-bound.** `DDL_ALLOWLIST_REGEX` in [`dashboard.py`](../src/slowquery_detective/dashboard.py) accepts exactly `CREATE INDEX [CONCURRENTLY] IF NOT EXISTS ix_<table>_<col(s)>`. An adversarial unit suite of 27 parametrized cases -- including Cyrillic homograph attacks, whitespace tricks, and injection attempts with embedded `DROP TABLE` -- locks the regex in place as a permanent regression guard. Beyond the regex, a body-supplied DDL is rejected unless its target table and columns actually appear in the fingerprint's canonical SQL, so an anonymous caller can't force `CREATE INDEX ... ON <any_table>` for a table unrelated to the fingerprint.
2b. **Per-request auth on the dashboard.** When `SLOWQUERY_PLATFORM_TOKEN` is set, every dashboard request must carry a matching `X-Platform-Token` header (constant-time compared) — this holds even in `DEMO_MODE`, so a public demo can still refuse anonymous callers. With no token configured, the endpoints fall back to the `DEMO_MODE` gate (403 outside demo mode) for local development.
3. **Rule-generated identifiers are whitelist-checked.** Every table/column name a rule formats into SQL must match `^[A-Za-z_][A-Za-z0-9_]*$` or the rule abstains. Reserved-word collisions (`"user"`, `"order"`) are double-quoted via `quote_if_reserved()`.
4. **No rule source contains `DROP`, `ALTER`, `TRUNCATE`, `GRANT`, `REVOKE`, `UPDATE`, or `DELETE`.** A grep-based regression guard runs on every CI build.
5. **LLM responses are filtered.** If the model proposes anything other than `CREATE INDEX ...`, the suggestion is retained as diagnostic prose but the executable `sql` field is nulled out. API keys are held in `SecretStr` and never appear in any log record.
6. **Per-fingerprint cooldown on the LLM call.** 60s default. Stops a hot fingerprint from burning OpenRouter credits or leaking timing information to a third-party service more than once per cooldown window.
7. **EXPLAIN runs only against read-only statements.** `EXPLAIN (ANALYZE)` *executes* its target, so the worker parses each captured statement (sqlglot AST, default-deny) and refuses to run EXPLAIN on anything that is not a provably read-only query — INSERT/UPDATE/DELETE/MERGE, data-modifying CTEs, and `DO`/`CALL`/DDL are dropped before any execution. Statements are still fingerprinted and tracked in the ring buffer; they just never reach the database via EXPLAIN.

## Test layout

| Layer | Count | Runs in CI? |
|---|---|---|
| Unit (`tests/unit/`) | 265 | Yes (default filter: `-m "not slow and not integration"`) |
| Integration (`tests/integration/`) | 52 | No -- needs testcontainers Postgres, run locally via `uv run pytest -m integration` |
| Slow (`@pytest.mark.slow`) | 6 | No -- benchmark-style, run via `uv run pytest -m slow` |
| **Total** | **317** | |

The critical-path HTTP surface (dashboard auth, DDL binding, detail shape) is exercised in CI by ASGI-level tests (`tests/unit/test_dashboard_asgi.py`) that drive the composed router against a real `RingBuffer` and a fake worker/engine — no Postgres required — so these paths are no longer only covered by the Docker-gated integration suite. The testcontainers integration tests (`tests/integration/`) remain the end-to-end layer against a real database and are run locally via `uv run pytest -m integration` (or in CI with a Postgres service).

## Deferred components

The data model in [`50-slowquery-detective.md`](../../docs/projects/50-slowquery-detective.md) defines four tables -- `query_fingerprints`, `query_samples`, `explain_plans`, `suggestions`. The abstract `StoreWriter` in `store.py` has typed interfaces for all four; the concrete Alembic migrations + asyncpg implementation land with `slowquery-demo-backend` in Phase 4b. For a working default, the package ships `InMemoryStoreWriter` (process-local) which `install()` uses unless a custom `StoreWriter` is injected via `install(app, engine, store=...)`. Library users who need cross-process/durable persistence subclass `StoreWriter` against their own DB. See the README "Scope & limitations" note on single-process state.
