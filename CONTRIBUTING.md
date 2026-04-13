# Contributing to slowquery-detective

Welcome! `slowquery-detective` is a drop-in FastAPI + SQLAlchemy middleware for catching slow Postgres queries, fingerprinting them, running async EXPLAIN, and suggesting indexes via a deterministic rules engine with LLM fallback. It ships as a PyPI package.

This guide covers everything you need to contribute.

---

## Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** — used for dependency management, locking, and running tools
- **Docker** — required for integration tests (testcontainers spins up a real Postgres instance)

## Development setup

```bash
git clone https://github.com/Abdul-Muizz1310/slowquery-detective
cd slowquery-detective
uv sync --all-extras          # installs all dependencies + dev group
uv run pytest                 # verify unit tests pass
uv run mypy src/              # verify types
uv run ruff check .           # verify lint
```

---

## Project architecture

The package lives under `src/slowquery_detective/` and is organized into 9 modules, each with a single responsibility:

| Module | Responsibility |
|---|---|
| `__init__.py` | Public surface — exports `install()` and `dashboard_router` |
| `fingerprint.py` | SQL fingerprinting via sqlglot. Scrubs literals (PII/secrets never leave the process), collapses query patterns into stable fingerprint IDs |
| `buffer.py` | Ring buffer with a 60-second sliding window. Tracks per-fingerprint latency and computes p50/p95/p99 in constant memory |
| `hooks.py` | SQLAlchemy event listeners. Hooks into `before_cursor_execute` / `after_cursor_execute` to time every statement |
| `explain.py` | Async EXPLAIN worker. Runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` off the request path with per-fingerprint rate limiting |
| `rules/` | 6-rule deterministic engine. Each rule is a pure function of `(plan, canonical_sql, fingerprint_id, recent_call_count)` |
| `llm_explainer.py` | OpenRouter LLM fallback with a 3-model cascade (primary, fast, fallback). Per-fingerprint cooldown prevents credit burn |
| `store.py` | Persistence layer for fingerprints, EXPLAIN plans, and suggestions |
| `dashboard.py` | Optional FastAPI `APIRouter` — query list, detail, DDL apply (allowlist-gated), SSE stream |
| `middleware.py` | `install()` function — wires all components onto the FastAPI app and SQLAlchemy `AsyncEngine` |

---

## How to add a new rule

The rules engine is the core value prop. Adding a rule is the most common contribution path.

### Step 1: Write the spec

Add test cases to `docs/specs/03-rules.md` or create a new spec file. Enumerate:
- What EXPLAIN plan shape triggers the rule
- What suggestion it produces (kind, SQL, rationale, confidence)
- Edge cases where it should **not** fire (abstain cases)

### Step 2: Create the rule file

Create `src/slowquery_detective/rules/your_rule_name.py`:

```python
"""Short description — see docs/specs/03-rules.md."""

from __future__ import annotations

from typing import Any

from slowquery_detective.rules.base import Rule, Suggestion, walk_nodes, IDENTIFIER_RE


class YourRuleName:
    """Docstring explaining what this rule detects."""

    name: str = "your_rule_name"

    def apply(
        self,
        plan: dict[str, Any],
        canonical_sql: str,
        *,
        fingerprint_id: str,
        recent_call_count: int,
    ) -> Suggestion | None:
        # Walk EXPLAIN plan nodes, inspect canonical SQL, return Suggestion or None.
        # Use walk_nodes(plan) to iterate all Plan sub-nodes.
        # Use IDENTIFIER_RE to validate any identifiers before interpolating into DDL.
        # Return None to abstain.
        ...
```

Your rule must implement the `Rule` protocol defined in `rules/base.py`:
- A `name: str` class attribute
- An `apply()` method that returns `Suggestion | None`

Rules must be **pure** — no I/O, no database access, no network calls. They receive the EXPLAIN plan dict and canonical SQL string and return a suggestion or `None`.

### Step 3: Register in `rules/base.py`

Add your import and instance to the `_registered_rules()` function:

```python
def _registered_rules() -> tuple[Rule, ...]:
    from slowquery_detective.rules.your_rule_name import YourRuleName
    # ... existing imports ...

    return (
        # ... existing rules ...
        YourRuleName(),
    )
```

### Step 4: Write tests

Add unit tests in `tests/unit/` that cover:
- The happy path (rule fires on a matching plan)
- Abstain cases (rule returns `None` when it shouldn't fire)
- Edge cases from your spec (malformed plans, missing keys, identifier validation)

---

## How to modify the fingerprinter

The fingerprinter (`src/slowquery_detective/fingerprint.py`) uses sqlglot to normalize SQL and scrub literals. Changes here affect every downstream component.

Before modifying:
1. Read `docs/specs/00-fingerprint.md` — it defines the contract
2. Check that existing unit tests in `tests/unit/` still pass after your change
3. Verify literal scrubbing behavior — **PII/secrets must never appear in fingerprint output**
4. Ensure fingerprint stability — the same logical query must always produce the same fingerprint ID

---

## Testing guidelines

This project follows **Spec-TDD**:

1. **Write the spec first** — describe behavior, inputs, outputs, invariants in `docs/specs/`
2. **Enumerate test cases** — explicit pass conditions AND failure conditions (edge cases, invalid input, boundary values)
3. **Write failing tests** that encode those conditions
4. **Implement** until all tests pass
5. Never write production code without a failing test that justifies it

### Test categories

| Marker | What it covers | Requires | Runs in CI |
|---|---|---|---|
| *(none)* | Unit tests — pure logic, mocked I/O | Nothing | Yes |
| `integration` | Real Postgres via testcontainers | Docker | No (local only) |
| `slow` | Benchmark-style, end-to-end | Docker | No (local only) |

### Running tests locally

```bash
# Unit tests only (fast, no Docker needed)
uv run pytest

# Unit tests with coverage report
uv run pytest --cov=slowquery_detective --cov-report=term-missing

# Integration tests (Docker must be running — testcontainers spins up Postgres)
uv run pytest -m integration

# Slow / benchmark tests
uv run pytest -m slow

# Everything
uv run pytest -m "" 

# Single test file
uv run pytest tests/unit/test_fingerprint.py -v
```

---

## Code style

### Linting: ruff

```bash
uv run ruff check .            # lint
uv run ruff format --check .   # format check
uv run ruff format .           # auto-format
```

Config is in `pyproject.toml`:
- Line length: 100
- Target: Python 3.12
- Rule sets: E, F, I, B, UP, N, SIM, RUF

### Types: mypy strict

```bash
uv run mypy src/
```

- `--strict` mode is enforced
- No `Any` crossing module boundaries
- All public functions must have full type annotations
- Pydantic models use `ConfigDict(extra="forbid", strict=True)`

---

## Security considerations

These are non-negotiable. PRs that weaken these guarantees will not be merged.

### Literal scrubbing

The fingerprinter replaces all SQL literals with placeholders before they enter the system. PII and secrets **never leave the process boundary**. If you modify the fingerprinter, verify this invariant with explicit test cases.

### DDL allowlist

The dashboard's `/apply` endpoint gates DDL execution behind a strict regex. Only statements matching `CREATE INDEX [CONCURRENTLY] IF NOT EXISTS ix_<table>_<col>` are allowed. The allowlist regex lives in `dashboard.py`. Do not widen it without a spec and thorough review.

### Identifier validation

Rules that interpolate identifiers (table names, column names) into generated DDL must validate them against `IDENTIFIER_RE` (`^[A-Za-z_][A-Za-z0-9_]*$`). If validation fails, the rule abstains rather than emitting potentially-injectable SQL. See `rules/base.py`.

### DEMO_MODE gate

DDL application via the dashboard API requires `DEMO_MODE=true` as an environment variable. This is an intentional safety net for production deployments.

---

## PR process

1. **Fork and branch** — create a feature branch from `main`
2. **Write specs and tests first** — follow Spec-TDD (see above)
3. **Implement** — keep changes scoped to one concern per PR
4. **Run the full quality gate locally:**
   ```bash
   uv run ruff check .
   uv run ruff format --check .
   uv run mypy src/
   uv run pytest --cov=slowquery_detective
   ```
5. **Push and open a PR** — CI will run lint, types, unit tests, coverage, and build
6. Coverage must stay at or above 80% on `src/slowquery_detective`

---

## Release process

Releases are automated via GitHub Actions based on git tags:

| Tag pattern | Target | Action |
|---|---|---|
| `testpypi-v*` | TestPyPI | Builds wheel + sdist, publishes to [TestPyPI](https://test.pypi.org/project/slowquery-detective/) |
| `v*.*.*` | PyPI | Builds wheel + sdist, publishes to [PyPI](https://pypi.org/project/slowquery-detective/) |

### To release a new version:

1. Update the version in `pyproject.toml` and `src/slowquery_detective/__init__.py`
2. Commit and push to `main`
3. Test on TestPyPI first:
   ```bash
   git tag testpypi-v0.2.0
   git push origin testpypi-v0.2.0
   ```
4. Verify the TestPyPI package installs correctly in a fresh venv
5. Release to PyPI:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```

---

Thank you for contributing! If you have questions, open an issue or start a discussion on the repository.
