# 07 — Fleet observability (0.2.0)

## Goal

Make slowquery-detective installable in every backend of the fleet (paper-trail, magpie, inkprint,
inkthread, the slowquery demo) with one call, persist what it sees in the service's own Postgres, and
answer one question for any request id: **which queries did this request cause, how many times, and how
long did they take?** bastion's dossier "measure" stage asks every service it called
`GET /_slowquery/queries?request_id=<id>` and hashes the answers into a perf receipt.

0.2.0 upstreams the four shims slowquery-demo-backend carries in `core/observability.py`
(its `docs/DEVIATIONS.md` §8): lifespan instead of `add_event_handler`, an injectable store,
per-execution state on the `ExecutionContext` instead of `cursor.info`, and a bridge from the sync hook
to an async store with `EXPLAIN` run on the statement's real parameters.

## Module map

| Module | Responsibility |
|---|---|
| `tagging.py` | `X-Request-Id` parsing, the request-id context variable, the ASGI middleware |
| `recorder.py` | Thread-safe in-memory aggregation between flushes; `Batch` and its pure `merge_batches` |
| `plan_scrub.py` | Pure: remove literal values from an `EXPLAIN` plan before it is stored or sent to an LLM |
| `store.py` | The `StoreWriter` interface and `InMemoryStoreWriter` |
| `pg_store.py` | `PostgresStoreWriter`: schema `slowquery`, idempotent DDL, batched upserts, reads, sweep |
| `store_url.py` | Pure: validate and normalise a Postgres URL for the async store engine |
| `schemas.py` | Response models: `QueryRow`, `SuggestionOut`, `ExplainPlanOut`, `SampleOut`, `QueryDetail` |
| `runtime.py` | The imperative shell: start/stop, the flush loop, readiness, the retention sweep |
| `router.py` | The two read routes behind `authorize` |
| `middleware.py` | `install()`: validation and wiring |
| `hooks.py`, `explain.py` | Changed: context-attribute timing, observations carry the raw statement; real-parameter `EXPLAIN` |

## Public API (interface I-2 of the bastion v2 rollout plan)

```python
from slowquery_detective import install

install(app, engine, *, service: str, threshold_ms: int = 100, sample_rate: float = 1.0,
        store_url: str | None = None,            # async SQLAlchemy URL; set => Postgres store
        store: StoreWriter | None = None,
        authorize: Callable[[Request], Awaitable[bool]] | None = None,  # None => routes answer 403
        mount_path: str = "/_slowquery", retention_s: int = 604_800,
        enable_llm: bool = False, llm_config: LlmConfig | None = None) -> None
```

Exported from `slowquery_detective`: `install`, `StoreWriter`, `InMemoryStoreWriter`,
`PostgresStoreWriter`, `LlmConfig`, `QueryRow`, `QueryDetail`, `SuggestionOut`, `current_request_id`,
`__version__`. The 0.1 `dashboard_router` is removed (see "Breaking changes").

### Argument contract (rejected at call time)

| Input | Result |
|---|---|
| `app` is `None` / not a `FastAPI` | `ValueError` / `TypeError` |
| `engine` is `None` / not an `AsyncEngine` | `ValueError` / `TypeError` |
| `service` empty or not matching `^[a-z0-9][a-z0-9_-]{0,63}$` | `ValueError` |
| `threshold_ms` not an `int` (a `bool` is not an int here) / `<= 0` | `TypeError` / `ValueError` |
| `sample_rate` outside `[0.0, 1.0]` | `ValueError` |
| `retention_s` not an `int` / `<= 0` | `TypeError` / `ValueError` |
| both `store_url` and `store` | `ValueError` (ambiguous) |
| `store_url` not `postgres://`, `postgresql://`, `postgresql+asyncpg://` or `postgresql+psycopg://` | `ValueError` |
| `mount_path` not matching `^(/[A-Za-z0-9._~-]+)+$` (leading `/`, no trailing `/`) | `ValueError` |
| `authorize` neither `None` nor callable | `TypeError` |
| `enable_llm=True` without `llm_config` | `ValueError` |
| `install` after the app has started serving | `RuntimeError` |
| a second `install` on the same app | warning `slowquery.middleware.already_installed`, no-op |

## Behaviour

### Request tagging (`tagging.py`)

1. `install` adds a pure ASGI middleware. For each `http` scope it reads `X-Request-Id`; a value is
   accepted only if it is a single header matching `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`. Anything else
   (absent, empty, too long, illegal characters, two headers) leaves the request **untagged**; the
   request is served normally either way.
2. The accepted value is set in a `ContextVar` for the duration of the ASGI call and reset afterwards.
   It is therefore visible in the endpoint, in dependencies, in Starlette `BackgroundTasks` (they run
   inside the same call, after the response is sent) and in `asyncio` tasks created during the request
   (they copy the context at creation).
3. The library's own background tasks (the flush loop and the `EXPLAIN` worker) run in a fresh, empty
   context, so nothing they execute is attributed to a request, whenever they were started.
3b. Requests to the library's own routes (`mount_path` and below, after `root_path`) are never tagged:
   a caller measuring request R sends `X-Request-Id: R` on the measure call too, and what runs during
   that call (the `authorize` hook's own queries, say) must not change R's numbers.
3c. Middleware the host adds *after* `install` wraps outside the tagger, so its statements are not
   attributed; call `install` after the host's own `add_middleware` calls.

### Recording (`hooks.py`, `recorder.py`)

4. The start time of a statement is stored as an attribute of its SQLAlchemy `ExecutionContext`
   (asyncpg's cursor has no `.info`); there is no global per-cursor map.
5. A statement executed while a request id is set is **always** recorded; `sample_rate` applies only to
   untagged statements. `EXPLAIN` statements are never recorded.
6. Every recorded statement adds to the ring buffer (live percentiles, as in 0.1) and to the pending
   fingerprint aggregate (`calls`, `total_ms`, `max_ms`, `first_seen`, `last_seen`).
7. A tagged statement also adds to the pending `(request_id, fingerprint_id)` aggregate (`calls`,
   `total_ms`, `max_ms`, `first_seen`, `last_seen`) **regardless of `threshold_ms`**.
8. A statement with `duration_ms >= threshold_ms` adds a slow sample `(fingerprint_id, duration_ms,
   request_id | null, sampled_at)` and, unless it was an `executemany`, submits an `EXPLAIN` job.
9. Pending state is bounded per flush interval: 10,000 fingerprints, 10,000 request aggregates and
   1,000 slow samples. Beyond a bound the observation is dropped and counted; the flush logs the drop
   count at WARNING. A failed flush keeps its batch and merges it into the next one under the same
   bounds.
9b. Canonical SQL is persisted and served, so the fingerprinter replaces every literal form: plain,
   dollar-quoted (`$$..$$`, `$tag$..$tag$`), escape (`E'..'`), hex, bit, unicode and national strings,
   on the sqlglot path and on the regex fallback.
10. `merge_batches(a, b)` is pure: calls and totals add, `max_ms` is the larger, `first_seen` the earlier,
    `last_seen` the later, percentiles come from the newer batch when it has them, samples keep the
    newest up to the bound.

### EXPLAIN (`explain.py`, `plan_scrub.py`)

11. The worker runs `EXPLAIN (FORMAT JSON) <statement>` on the host engine **with the statement's real
    parameters** (driver-level statement and parameters captured by the hook). There is no `ANALYZE`:
    the statement itself is never executed again. The 0.1 parameter synthesizer is removed (it produced
    invalid SQL for typed parameters such as `$1::UUID`).
12. The read-only guard stays in front of it: a canonical statement that is not provably read-only is
    never explained.
13. A job is not queued when its fingerprint is cooling down or already queued, so a hot slow query
    cannot fill the queue. Submissions from a thread other than the worker's loop go through
    `call_soon_threadsafe`.
14. A plan explained with real parameters contains their values (`Index Cond: (email = 'a@b.c'::text)`).
    The rules run on that plan in memory; everything that leaves the worker — the stored plan, the plan
    sent to the LLM — is **scrubbed**: quoted literals and numeric literals inside every expression
    string become `?`; identifiers, node types and the planner's own numbers (costs, rows, widths) are
    kept. No parameter value is logged, stored or served.

### Stores (`store.py`, `pg_store.py`)

15. `store_url=None, store=None` uses `InMemoryStoreWriter` (process-local, bounded, swept);
    `store=<StoreWriter>` uses it as given; `store_url=<url>` uses `PostgresStoreWriter(url)`.
16. `PostgresStoreWriter` opens its **own** engine (pool of 2 + 2 overflow, `pool_pre_ping`,
    `pool_recycle=300`, a 10 s connect timeout so a read never waits a minute on an unreachable
    database; asyncpg's statement cache off for PgBouncer `-pooler` hosts). The hooks are never
    attached to it, so the store's own traffic is never observed.
17. URL normalisation: `postgres://` and `postgresql://` become `postgresql+asyncpg://`; for asyncpg,
    `sslmode=X` becomes `ssl=X` and `channel_binding` is dropped (asyncpg rejects both).
18. Startup DDL runs in one transaction under `pg_advisory_xact_lock`, every statement
    `CREATE … IF NOT EXISTS`, so it is safe on every boot and when two processes boot at once:

    | Table (schema `slowquery`) | Key | Columns |
    |---|---|---|
    | `query_fingerprints` | `(service, id)` | `canonical_sql`, `first_seen`, `last_seen`, `call_count`, `total_ms`, `max_ms`, `p50_ms`, `p95_ms`, `p99_ms` |
    | `request_stats` | `(service, request_id, fingerprint_id)` | `calls`, `total_ms`, `max_ms`, `first_seen`, `last_seen` |
    | `query_samples` | `id` | `service`, `fingerprint_id`, `request_id`, `duration_ms`, `sampled_at` |
    | `explain_plans` | `fingerprint_id` | `plan_json` (jsonb, scrubbed), `cost`, `captured_at` |
    | `suggestions` | `id`; **unique `(fingerprint_id, kind, sql)`** (`uq_suggestions_fp_kind_sql`) | `kind`, `source`, `sql` (`''` = none), `rationale`, `applied_at`, `created_at` |
    | `schema_version` | `version` | `applied_at` (row `1`) |

    Traffic tables carry `service`, so two services sharing one database keep their numbers apart.
    Plans and suggestions describe a statement against a database, so they are keyed by fingerprint
    only. There are no foreign keys: a plan may be written before its fingerprint's first flush.
19. A flush writes fingerprints, request aggregates and samples in **one transaction**, rows sorted by
    key (consistent lock order between processes). Upserts add: `call_count + n`, `total_ms + t`,
    `GREATEST(max_ms)`, `LEAST(first_seen)`, `GREATEST(last_seen)`, percentiles replaced when present.
20. Suggestions insert with `ON CONFLICT (fingerprint_id, kind, sql) DO NOTHING`; the same suggestion
    found twice is stored once.
21. Retention: samples with `sampled_at` and request aggregates with `last_seen` older than
    `retention_s` are deleted for the service. The sweep runs at startup and then at most hourly, and
    only after a flush that wrote something — an idle service never wakes its database.
22. If the store cannot start (database asleep or unreachable), the error is logged at ERROR, the app
    keeps serving, pending data is held (bounded) and the start is retried on the next flush.
22b. A flush that is cancelled mid-write (shutdown, a cancelled read, the final flush's timeout) puts
    its batch back for the next flush (at-least-once: a write that committed just before the
    cancellation is counted twice).
22c. The in-memory store keeps at most 10,000 fingerprints (least recently seen evicted with their
    samples and request rows), 100 samples per fingerprint and 50,000 request rows; the worker's
    cooldown map drops expired entries once it holds 10,000.

### Routes (`router.py`), mounted at `mount_path`

23. Every route depends on `authorize`. `None` ⇒ 403. The hook returning anything but `True` ⇒ 403.
    An `HTTPException` (FastAPI's or Starlette's) it raises propagates (so a host may answer 401); any other exception ⇒ logged,
    403. Authorization runs before query-parameter validation, so an unauthorized caller learns nothing
    from a 422.
24. `GET /queries?request_id=&since=&limit=` → `list[QueryRow]`, local pending data flushed first.
    `QueryRow = {id, fingerprint, first_seen, last_seen, call_count, total_ms, p50_ms, p95_ms, p99_ms,
    max_ms, suggestions: [{kind, source, sql, rationale, applied_at}]}`, `fingerprint` being the
    canonical SQL. Ordered by `total_ms` desc, then `id`. `limit` 1–500 (default 50). `since` must be
    timezone-aware and filters on `last_seen`. `request_id` must match the header pattern.
    - Without `request_id`: the service's fingerprints; lifetime `call_count`/`total_ms`/`max_ms`;
      `p50/p95/p99` are the rolling 60-second percentiles of the process that last flushed them.
    - With `request_id`: only fingerprints that request executed; `call_count`, `total_ms`, `max_ms`,
      `first_seen`, `last_seen` over that request; `p50/p95/p99` are `null`. Unknown id ⇒ `[]`.
25. `GET /queries/{id}` → `{fingerprint: QueryRow, canonical_sql, explain_plan: {plan_json, cost,
    captured_at} | null, suggestions, recent_samples: [{duration_ms, request_id, sampled_at}]}`
    (20 newest samples). An id that is not 16 lowercase hex characters, or is unknown, ⇒ 404.
26. Store not ready ⇒ 503. The routes never write to the host database and never return parameter
    values.

### Lifecycle (`runtime.py`)

27. `install` wraps the app's lifespan: after the host's startup the store starts, the worker and the
    flush loop (every 2 s) start; on shutdown the loop stops, a final flush runs, the worker stops, the
    hooks detach, the store and the LLM client close. The host's engine is not disposed. A later
    lifespan on the same app, including one on a new event loop (a test suite re-entering
    `TestClient`), re-attaches the hooks and gives the worker a fresh queue and the runtime a fresh
    lock (asyncio primitives stay bound to the loop they first waited on).

## Breaking changes from 0.1

- `service` is required (keyword-only).
- The static-secret check (`SLOWQUERY_PLATFORM_TOKEN` / `X-Platform-Token`) is removed; access is
  whatever `authorize` decides, and no hook means 403.
- `dashboard_router` and its routes (`/api/queries`, `/api/queries/{id}`, `/api/queries/{id}/apply`,
  `/api/stream`) are removed. `install` mounts `GET {mount_path}/queries` and
  `GET {mount_path}/queries/{id}`. Applying DDL over HTTP is no longer part of the library.
- `store_url` now means "persist to this Postgres"; it no longer defaults to the engine's URL.
- `StoreWriter` is an abstract base with a new read/write interface; 0.1 subclasses must be rewritten.
- `ExplainJob` carries the statement and parameters; `ExplainWorker.plan_cache_get`, `CachedPlan` and
  `synthesize_params` are removed; `EXPLAIN` no longer uses `ANALYZE`.
- `LlmConfig` model slots default to the free OpenRouter models (primary
  `google/gemma-4-26b-a4b-it:free`, fast `minimax/minimax-m3.1-flash:free`, fallback
  `google/gemma-4-31b-it:free`) instead of empty strings.

## Enumerated test cases

### install contract (unit)

1. Each row of the argument table raises the stated exception (parametrized).
2. Valid call: middleware added once, routes mounted at `mount_path`, `app.state.slowquery_installed`.
3. Second call warns and changes nothing (one middleware, one set of routes, hooks attached once).
4. `store_url=None, store=None` ⇒ `InMemoryStoreWriter`; `store=` ⇒ that object; `store_url=` ⇒
   `PostgresStoreWriter` (no connection made at install).
5. `install` after the app started ⇒ `RuntimeError`.
6. `LlmConfig()` model slots equal the three free models.

### Request id parsing and propagation (unit, ASGI)

7. Valid ids (UUID, ULID, `a`, 128 chars, `.`/`_`/`:`/`-`) are accepted.
8. Rejected: empty, 129 chars, leading `-`, space, `/`, non-ASCII, newline; two `X-Request-Id`
   headers.
9. The id is visible in the endpoint, in a dependency, in a `BackgroundTasks` function and in an
   `asyncio` task created by the endpoint; it is `None` in a request without the header and after the
   request finishes.
10. A task created by the library at startup sees no request id even when started during a request.

### Recorder and batches (unit + property)

11. One observation ⇒ one fingerprint delta (calls 1, total = max = duration, first = last = now).
12. Tagged observation ⇒ request delta; untagged ⇒ none.
13. Duration `< threshold` ⇒ no sample; `== threshold` ⇒ sample (boundary); sample keeps request id.
14. `drain()` returns everything and leaves the recorder empty; a second `drain()` is empty.
15. Bounds: the 10,001st fingerprint, request key and the 1,001st sample are dropped and counted.
16. Property: `merge_batches` is associative and preserves the sum of calls and totals, the max of
    maxima, the min/max of first/last seen (Hypothesis).
17. Property: a stream of observations split at any point into two drains, merged, equals one drain.
18. Thread safety: 8 threads × 1,000 observations ⇒ calls sum to 8,000.

### Hooks (unit + integration)

19. Start time lives on the execution context; an asyncpg-backed `AsyncEngine` records statements
    (integration, real Postgres).
20. `sample_rate=0.0`: untagged statements are not recorded; tagged statements are (integration).
21. `EXPLAIN …` statements are not recorded.
22. A failing statement is still recorded and its error reaches the caller (integration).
23. A crash inside the observation callback never reaches the host query.

### Plan scrubbing (unit + property)

24. Quoted literals (`'a@b.c'::text`, `'{1,2}'::integer[]`, escaped `''`) and numbers (`42`, `-3.5`)
    in `Filter`, `Index Cond`, `Recheck Cond`, `Join Filter`, `Hash Cond`, `Sort Key` and `Output`
    become `?`.
25. Kept: `Node Type`, `Relation Name`, `Index Name`, `Alias`, `Schema`, numeric fields (`Total Cost`,
    `Plan Rows`), identifiers containing digits (`col1`), parameter symbols (`$1`).
26. Nested `Plans` are scrubbed; the input is not mutated.
27. Property: for any literal text embedded as `'<text>'::text` in a filter, the scrubbed plan's JSON
    does not contain it (Hypothesis, texts of 4+ chars).

### EXPLAIN worker (unit + integration)

28. A job's statement is explained with its parameters (integration: a `$1::uuid` statement, which the
    0.1 synthesizer could not explain, yields a plan).
29. The statement is not executed: explaining an `UPDATE` never happens (guard), and explaining a
    `SELECT` that calls a volatile counter function leaves the counter unchanged (integration).
30. Secret parameter: a query filtered on `'sk-live-…'` stores a plan without the secret, sends the LLM a
    plan without it, and logs nothing containing it (integration + caplog).
31. Cooling down or already-queued fingerprints are not queued; a full queue returns `False`.
32. A submit from another thread is delivered to the worker's loop.
33. Rules see the unscrubbed plan (a rule keyed on a filter column still fires).

### Postgres store (integration, Testcontainers Postgres 16)

34. `start()` creates schema `slowquery` and the six tables; a second `start()` (same or new
    instance) succeeds and changes nothing (boot twice).
35. Two stores starting concurrently against an empty database both succeed (advisory lock).
36. `write_batch` then `list_queries` round-trips every QueryRow field; a second batch adds counts
    (calls, totals), keeps the max, the earliest first and the latest last seen.
37. Request aggregates: `list_request_queries(R)` returns only R's fingerprints with R's numbers and
    null percentiles; another request's numbers and another service's rows are not included.
38. `since` filters on `last_seen` (inclusive boundary); `limit` caps; order is `total_ms` desc, `id`.
39. Suggestions: inserting the same suggestion twice stores one row; `sql=None` is stored as `''` and
    served as `null`; a different kind with the same SQL is a second row.
40. `upsert_plan` before any fingerprint row works (no foreign key); the latest plan wins.
41. `get_query`: unknown id ⇒ `None`; known ⇒ detail with plan, suggestions, newest 20 samples first.
42. Sweep deletes samples and request aggregates older than the cutoff for this service only, and keeps
    newer ones and other services' rows.
43. URL normalisation: `postgres://`, `sslmode=require` ⇒ `ssl=require`, `channel_binding` dropped,
    `postgresql+psycopg://` kept; `mysql://`, `sqlite+aiosqlite://`, `postgresql+psycopg2://` rejected
    (unit).
44. A store whose database is unreachable: `start()` raises; the runtime logs, stays not-ready, serves
    503, and becomes ready once the database answers (unit with a failing fake, integration with a
    stopped-then-started container is out of scope).

### Routes (unit ASGI with the in-memory store, and integration with Postgres)

45. `authorize=None` ⇒ 403 on both routes; hook returns `False` ⇒ 403; returns a truthy non-bool ⇒ 403;
    raises `HTTPException(401)` ⇒ 401; raises `RuntimeError` ⇒ 403.
46. Unauthorized + invalid `limit` ⇒ 403, not 422.
47. `limit` 0 or 501 ⇒ 422; naive `since` ⇒ 422; malformed `request_id` ⇒ 422.
48. `GET /queries/{id}` with `../x`, uppercase hex, 15 chars or an unknown id ⇒ 404.
49. Composed flow (the WP-8 acceptance shape): a FastAPI app with a real asyncpg engine and
    `store_url` pointing at Postgres; request with `X-Request-Id: R` running three statements (one twice)
    ⇒ `GET /_slowquery/queries?request_id=R` lists exactly those fingerprints with the right call counts;
    a request without the header adds nothing to R (integration).
50. The same flow with a `BackgroundTasks` query and an `asyncio` task query, both attributed to R
    (integration).
51. A slow statement (threshold 1 ms, `pg_sleep`) ⇒ a sample with request id R, a stored scrubbed plan,
    and the detail route returns them (integration).
52. Restarting the app (new install over the same database) keeps the data and does not fail on the
    existing schema (integration).
53. Shutdown flushes what is pending; no library task is left running; the host engine still works.
54. Two lifespans on one app: the hooks are attached in both; three `TestClient` blocks (three event
    loops) each process an EXPLAIN job.
55. A measure call carrying `X-Request-Id: R` is not itself attributed to R; the `/_slowquery` prefix
    is excluded from tagging, with and without a `root_path` (unit).
56. Dollar-quoted, escape, hex, bit, unicode and national string literals never reach the canonical
    SQL (unit, both fingerprint paths).
57. A cancelled flush keeps its batch; `stop()` during a slow write still writes it in the final flush.
58. The LLM fallback still works in a second lifespan (integration, OpenRouter mocked).
59. In-memory fingerprint eviction takes the fingerprint's samples and request rows with it; the
    cooldown map is pruned.

## Acceptance criteria

- [ ] Cases 1–59 pass; integration cases run in CI (`pytest -m integration`, Testcontainers).
- [ ] ruff, ruff format, mypy `--strict` clean.
- [ ] Branch coverage on in CI; no module under 85 %.
- [ ] README: fleet install in three lines with an `authorize` hook, migration notes from 0.1, every
      number re-measured.
- [ ] CHANGELOG 0.2.0 lists every breaking change above.
