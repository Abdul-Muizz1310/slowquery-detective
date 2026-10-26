# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- `LlmConfig.model_fast` defaults to `xingchen-agi/xing4.0-29b-a4b:free`: the previous default,
  `minimax/minimax-m3.1-flash:free`, left OpenRouter on 2026-10-26, so the fast tier answered 404
  and the cascade skipped to the fallback.

## [0.2.1] - 2026-10-20

### Added

- `request_scope(request_id)`, exported from the package root: a context manager (`with` or
  `async with`) that attributes the statements run inside it to `request_id`, for work that runs
  outside the request that caused it, such as a background job processing an upload. It restores the
  previous id on exit, also on an exception, so scopes nest; tasks created inside it keep the id. The
  id must pass the `X-Request-Id` rule: an invalid id raises `ValueError`, a non-`str` raises
  `TypeError`. This replaces setting the library's private context variable.

## [0.2.0] - 2026-10-20

Fleet observability: install the library in every backend, keep what it sees in the service's own
Postgres, and ask any service which queries one request caused. Spec:
[`docs/specs/07-fleet-observability.md`](docs/specs/07-fleet-observability.md). Upstreams the four
shims slowquery-demo-backend carried (its `docs/DEVIATIONS.md` §8).

### Breaking

- `install()` requires a keyword-only `service` (`[a-z0-9][a-z0-9_-]{0,63}`), and `engine` must be
  an `AsyncEngine`. Every argument is validated before anything is wired.
- The static shared secret is gone: `SLOWQUERY_PLATFORM_TOKEN` and the `X-Platform-Token` check are
  no longer read. Access to the routes is whatever the new `authorize(request) -> bool` hook decides;
  with no hook, every route answers 403.
- `dashboard_router` is removed, and with it `GET /api/queries`, `GET /api/queries/{id}`,
  `POST /api/queries/{id}/apply` and `GET /api/stream`. `install()` now mounts two read routes itself,
  `GET {mount_path}/queries` and `GET {mount_path}/queries/{id}` (`mount_path` defaults to
  `/_slowquery`). Applying DDL over HTTP is no longer part of the library, so `DEMO_MODE` is not read
  either.
- `store_url` means "persist to this Postgres" (schema `slowquery`); it no longer defaults to the
  engine's URL. Without `store_url` or `store`, the in-memory store is used.
- `StoreWriter` is an abstract base with a new interface (`start`, `write_batch`, `upsert_plan`,
  `insert_suggestions`, `sweep`, `list_queries`, `list_request_queries`, `get_query`, `close`);
  0.1 subclasses must be rewritten.
- `ExplainJob` carries the statement, its parameters and the recent call count;
  `ExplainWorker.plan_cache_get`, `CachedPlan` and `synthesize_params` are removed. The `rules`
  callable is called with `fingerprint_id` and `recent_call_count` keywords.
- `hooks.attach(on_record=...)` passes one `Observation` instead of three positional values.
- `LlmConfig` model slots default to the free OpenRouter models (`google/gemma-4-26b-a4b-it:free`,
  `minimax/minimax-m3.1-flash:free`, `google/gemma-4-31b-it:free`) instead of empty strings.

### Added

- Request tagging: an ASGI middleware copies a valid `X-Request-Id` into a context variable. Every
  statement a tagged request runs — in the endpoint, a dependency, a `BackgroundTasks` function or an
  `asyncio` task it spawned — adds to a per-`(request_id, fingerprint)` aggregate (`calls`,
  `total_ms`, `max_ms`) regardless of `threshold_ms` and of `sample_rate`.
- `GET {mount_path}/queries?request_id=&since=&limit=` returns `QueryRow`s
  (`id, fingerprint, first_seen, last_seen, call_count, total_ms, p50_ms, p95_ms, p99_ms, max_ms,
  suggestions[kind, source, sql, rationale, applied_at]`); with `request_id`, only that request's
  fingerprints and numbers, percentiles `null`. `GET {mount_path}/queries/{id}` returns
  `{fingerprint, canonical_sql, explain_plan, suggestions, recent_samples}`.
- `PostgresStoreWriter`: its own small engine (never instrumented), tables `query_fingerprints`,
  `request_stats`, `query_samples`, `explain_plans`, `suggestions` (unique
  `(fingerprint_id, kind, sql)`) and `schema_version` in schema `slowquery`, created at startup by
  `CREATE … IF NOT EXISTS` under an advisory lock (safe on every boot and on concurrent boots),
  batched additive upserts, Neon-style URLs normalised for asyncpg.
- Retention: `retention_s` (default 7 days) for samples and request aggregates; the sweep runs at
  startup and then at most hourly, and only after a flush that wrote something.
- A `postgres` extra (`asyncpg`).
- `ExplainWorker.processed_jobs` and `queued`, beside `dropped_jobs`.
- `current_request_id()`, `QueryRow`, `QueryDetail`, `SuggestionOut`, `StoreWriter`,
  `InMemoryStoreWriter`, `PostgresStoreWriter` and `LlmConfig` are exported.

### Changed

- `EXPLAIN` runs with the statement's real parameters and **without `ANALYZE`**, so the statement is
  never executed again. Typed parameters such as `$1::UUID`, which the 0.1 synthesizer turned into
  invalid SQL, now produce plans.
- Plans are scrubbed of literal values before they are stored or sent to the LLM; driver errors are
  logged by class only, since their messages can quote a parameter.
- Statement start times live on the SQLAlchemy `ExecutionContext`; the global per-cursor map is
  gone.
- `install()` wraps the app's lifespan instead of calling `add_event_handler` (removed in
  Starlette 1.x). A later lifespan on the same app — also on a new event loop, as when a test suite
  re-enters `TestClient(app)` — re-attaches the hooks and gives the worker a fresh queue.
- A slow fingerprint that is already queued or cooling down is not queued again; submissions from
  another thread go through `call_soon_threadsafe`.
- The library's background tasks run in an empty context, so nothing they execute is attributed to a
  request.
- Requests to the library's own routes are not tagged, so a measure call carrying `X-Request-Id: R`
  never adds to R's numbers.
- The fingerprinter also replaces dollar-quoted (`$$..$$`, `$tag$..$tag$`), escape (`E'..'`), hex,
  bit, unicode and national string literals, on both of its paths, since canonical SQL is now
  stored and served. Fingerprint ids of statements with such literals change.
- A flush cancelled mid-write (shutdown, a cancelled read) keeps its batch for the next flush.
- The in-memory store is bounded in fingerprints too (10,000, least recently seen evicted), and the
  worker's cooldown map drops expired entries.
- An `authorize` hook may raise Starlette's `HTTPException` as well as FastAPI's.
- CI measures branch coverage, runs the unit and Postgres tiers together, and fails any module under
  85 %.

## [0.1.1] - 2026-10-20

The fixes made on `main` since 0.1.0, released. 0.1.0 shipped the dashboard router and the store as
stubs that raised `NotImplementedError`; 0.1.1 is the first release in which both work.

### Security

- `EXPLAIN (ANALYZE)` runs only on statements that parse as provably read-only. `INSERT`, `UPDATE`,
  `DELETE`, `MERGE`, DDL, `CALL`/`DO` and data-modifying CTEs are never sent to `EXPLAIN`, so a
  captured write is never executed again against the database.
- The DDL allowlist is anchored with `\Z` instead of `$` and statements with embedded newlines are
  rejected before it is checked, closing a newline-injection bypass of
  `POST /api/queries/{id}/apply`.
- DDL supplied in the request body must target a fingerprint that has been observed, and every table
  and column it names must appear in that fingerprint's canonical SQL. The free-form regex fallback
  that could bind an index to the wrong table of a join is removed.
- The dashboard router requires a matching `X-Platform-Token` header (constant-time comparison)
  whenever `SLOWQUERY_PLATFORM_TOKEN` is set; without it, the router answers 403 outside
  `DEMO_MODE=true`.

### Added

- `install(..., store=...)` accepts a `StoreWriter`; without one, `InMemoryStoreWriter` (a working,
  process-local store) is used instead of the abstract base.
- The dashboard router: `GET /api/queries`, `GET /api/queries/{id}`,
  `POST /api/queries/{id}/apply`, `GET /api/stream`.
- `ExplainWorker.engine`, `process_pending()` and `dropped_jobs`; a full queue is logged at WARNING.

### Fixed

- The ring buffer is a true 60-second sliding window. It used to rewrite surviving timestamps on
  every read, so the window collapsed to "since the last read".
- Background `EXPLAIN` runs only for statements at or above `threshold_ms`; faster statements are
  still fingerprinted for percentiles.
- The N+1 rule receives the fingerprint id and recent call count, so it can fire.
- Hooks work with asyncpg cursors, which have no `cursor.info`, and clean up after failed
  statements instead of leaking per-cursor state.
- The LLM cascade has a total deadline across its three models and moves to the next model on any
  non-2xx except 401 (an unknown slug or a refused `response_format` no longer ends the cascade).
- One keep-alive HTTP client is reused for LLM calls and closed with the store on shutdown.

### Changed

- `fingerprint()` is memoized with a bounded LRU cache.
- The unused `structlog` dependency is dropped.

## [0.1.0] - 2026-04-11

### Added

- First release: SQL fingerprinting, a sliding-window ring buffer, SQLAlchemy hooks, a background
  `EXPLAIN` worker, a six-rule engine, an OpenRouter fallback and `install()`.

[Unreleased]: https://github.com/Abdul-Muizz1310/slowquery-detective/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/Abdul-Muizz1310/slowquery-detective/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Abdul-Muizz1310/slowquery-detective/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Abdul-Muizz1310/slowquery-detective/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Abdul-Muizz1310/slowquery-detective/releases/tag/v0.1.0
