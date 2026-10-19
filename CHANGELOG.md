# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/Abdul-Muizz1310/slowquery-detective/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/Abdul-Muizz1310/slowquery-detective/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Abdul-Muizz1310/slowquery-detective/releases/tag/v0.1.0
