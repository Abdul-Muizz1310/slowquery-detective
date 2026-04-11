# slowquery-detective

> Catch slow Postgres queries live. Fingerprint the pattern. Run `EXPLAIN` asynchronously. Suggest the index. A drop-in FastAPI + SQLAlchemy middleware.

[![PyPI](https://img.shields.io/pypi/v/slowquery-detective?style=flat-square)](https://pypi.org/project/slowquery-detective/)
[![ci](https://github.com/Abdul-Muizz1310/slowquery-detective/actions/workflows/ci.yml/badge.svg)](https://github.com/Abdul-Muizz1310/slowquery-detective/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.12+-3776ab?style=flat-square&logo=python&logoColor=white)
![sqlalchemy](https://img.shields.io/badge/SQLAlchemy-2.0-d71f00?style=flat-square)
![fastapi](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![mypy](https://img.shields.io/badge/mypy-strict-blue?style=flat-square)
![coverage](https://img.shields.io/badge/coverage-84%25-green?style=flat-square)
![license](https://img.shields.io/badge/license-MIT-lightgrey?style=flat-square)

---

## What it does

1. **Fingerprints** every query via `sqlglot`. `WHERE id=1` and `WHERE id=2` collapse into one row, so you see the *pattern* that's slow, not 10,000 per-literal samples. Literals are scrubbed before they hit the log — PII and secrets never leave the process boundary.
2. **Runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`** asynchronously for queries that cross the p95 threshold. The EXPLAIN is executed off the request path with per-fingerprint rate limiting, so a hot endpoint can't double its own latency.
3. **Suggests fixes** via a deterministic rules engine with six rules (seq scan on large tables, missing FK indexes, sort without index, function in WHERE, `SELECT *`, N+1). When no rule matches, an OpenRouter-backed LLM explains the plan in plain English — but the rules engine catches the 80% of real wins, because real wins are boring and deterministic.
4. **Exposes a tiny dashboard API** (optional `APIRouter`) so a frontend can render live p95 timelines and one-click index suggestions. DDL application is gated behind a strict regex allowlist: only `CREATE INDEX [CONCURRENTLY] IF NOT EXISTS ix_<table>_<col>` makes it through.

This repository is **the PyPI package only**. The demo service and dashboard live in:

- [`Abdul-Muizz1310/slowquery-demo-backend`](https://github.com/Abdul-Muizz1310/slowquery-demo-backend) — feathers-generated FastAPI service with a seeded 1M-row dataset (Phase 4b)
- [`Abdul-Muizz1310/slowquery-dashboard-frontend`](https://github.com/Abdul-Muizz1310/slowquery-dashboard-frontend) — Next.js dashboard (Phase 4c)

## Install

```bash
pip install slowquery-detective[fastapi]
# or, to enable the LLM fallback:
pip install slowquery-detective[fastapi,llm]
```

Python 3.12+ required. The `[fastapi]` extra pulls FastAPI and Starlette; `[llm]` pulls the OpenAI SDK (used with OpenRouter's base URL).

## 3-line integration

```python
from slowquery_detective import install

install(app, engine)
```

That's the whole public surface for the happy path. The middleware wires every component — fingerprinting, ring buffer, SQLAlchemy hooks, EXPLAIN worker, rules engine — onto the FastAPI app and SQLAlchemy `AsyncEngine` you pass in.

### Optional: mount the dashboard API

```python
from slowquery_detective import install, dashboard_router

install(app, engine)
app.include_router(dashboard_router, prefix="/_slowquery")
```

This exposes the dashboard endpoints used by `slowquery-dashboard-frontend`:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/_slowquery/api/queries` | List fingerprints, sorted by `total_ms` desc |
| `GET` | `/_slowquery/api/queries/{id}` | Detail: plan + suggestions + recent samples |
| `POST` | `/_slowquery/api/queries/{id}/apply` | Run the suggested DDL (allowlist-gated, `DEMO_MODE=true` required) |
| `GET` | `/_slowquery/api/stream` | SSE: live p95 updates per fingerprint |

### Optional: LLM explainer fallback

```python
from pydantic import SecretStr
from slowquery_detective import install
from slowquery_detective.llm_explainer import LlmConfig

llm_config = LlmConfig(
    enabled=True,
    api_key=SecretStr("sk-or-v1-..."),
    model_primary="nvidia/nemotron-nano-9b-v2:free",
    model_fast="google/gemma-3-27b-it:free",
    model_fallback="z-ai/glm-4.5-air:free",
)
install(app, engine, enable_llm=True, llm_config=llm_config)
```

The cascade is `PRIMARY → FAST → FALLBACK` on HTTP 429 / 5xx / network errors. `401` is non-retriable. Per-fingerprint cooldown (60 s by default) prevents a hot fingerprint from burning LLM credits.

## Configuration

| Argument | Default | Description |
|---|---|---|
| `threshold_ms` | `100` | Queries slower than this are flagged for `EXPLAIN` |
| `sample_rate` | `1.0` | Fraction of statements to fingerprint (0.0–1.0) |
| `store_url` | `None` | Where to persist fingerprints/plans; defaults to the engine URL |
| `enable_llm` | `False` | Turn on the OpenRouter fallback |
| `llm_config` | `None` | Required when `enable_llm=True`; see `LlmConfig` |

Each argument validates at call time: negative `threshold_ms`, out-of-range `sample_rate`, or `enable_llm=True` without `llm_config` raise `ValueError`.

## Tech stack

| Layer | Choice |
|---|---|
| Python | 3.12+ |
| SQL parser | [sqlglot](https://github.com/tobymao/sqlglot) 25+ |
| Validation | [pydantic](https://docs.pydantic.dev/) 2.9+ |
| Async HTTP | [httpx](https://www.python-httpx.org/) 0.27+ |
| Logging | [structlog](https://www.structlog.org/) 24+ |
| Middleware | [FastAPI](https://fastapi.tiangolo.com/) 0.115+ (via `[fastapi]` extra) |
| LLM client | [openai](https://github.com/openai/openai-python) 1.40+ pointed at OpenRouter (via `[llm]` extra) |
| Dev | pytest, pytest-asyncio, respx, testcontainers, hypothesis, ruff, mypy |

## Quality gates

Every commit goes through a strict pipeline on GitHub Actions:

- **Lint**: `ruff check` + `ruff format --check`
- **Types**: `mypy --strict` on `src/`
- **Tests**: `pytest -m "not slow and not integration"`
- **Coverage**: 80% minimum on `src/slowquery_detective`, currently **84%**
- **Build**: `uv build` produces wheel + sdist; artifacts uploaded to the run

The test suite is Spec-TDD: 7 feature specs under [`docs/specs/`](docs/specs/) list every enumerated test case, and 195 pytest items encode them — 177 unit tests that run in CI, plus 55 integration/slow tests gated on testcontainers Postgres that run locally via `uv run pytest -m integration`.

## Development

```bash
git clone https://github.com/Abdul-Muizz1310/slowquery-detective
cd slowquery-detective
uv sync --all-extras
uv run pytest                    # unit tests only (default)
uv run pytest -m integration     # testcontainers Postgres required
uv run pytest -m slow            # benchmark-style tests
uv run ruff check .
uv run mypy src/
```

## Status

- ✅ **v0.1.0 released** to [PyPI](https://pypi.org/project/slowquery-detective/) on 2026-04-11
- ✅ 177 unit tests green in CI, coverage 84%, mypy strict clean
- ✅ `pip install slowquery-detective[fastapi,llm]` verified in a fresh 3.12 venv
- 🟡 Live demo service (`slowquery-demo-backend`) in flight as Phase 4b
- 🟡 Dashboard frontend (`slowquery-dashboard-frontend`) in flight as Phase 4c

## License

MIT — see [LICENSE](LICENSE).
