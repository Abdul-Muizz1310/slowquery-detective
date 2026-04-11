# slowquery-detective

> Catch slow Postgres queries live. Fingerprint the pattern. Run `EXPLAIN` asynchronously. Suggest the index. A drop-in FastAPI + SQLAlchemy middleware.

[![ci](https://github.com/Abdul-Muizz1310/slowquery-detective/actions/workflows/ci.yml/badge.svg)](https://github.com/Abdul-Muizz1310/slowquery-detective/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.12+-3776ab?style=flat-square&logo=python&logoColor=white)
![sqlalchemy](https://img.shields.io/badge/SQLAlchemy-2.0-d71f00?style=flat-square)
![fastapi](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![mypy](https://img.shields.io/badge/mypy-strict-blue?style=flat-square)
![license](https://img.shields.io/badge/license-MIT-lightgrey?style=flat-square)

---

## What it does

1. **Fingerprints** every query via `sqlglot` — `WHERE id=1` and `WHERE id=2` collapse into one row, so you see the *pattern* that's slow.
2. **Runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`** asynchronously for queries that cross the p95 threshold, off the request path.
3. **Suggests fixes** via a deterministic rules engine (seq-scan on large tables, missing FK indexes, sort without index, function in WHERE, `SELECT *`, N+1). When no rule matches, an OpenRouter-backed LLM explains the plan in plain English.
4. **Exposes a tiny dashboard API** (optional router) so a frontend can render live p95 timelines and one-click index suggestions.

This repository is **the PyPI package only**. The demo service and dashboard live in:

- [`Abdul-Muizz1310/slowquery-demo-backend`](https://github.com/Abdul-Muizz1310/slowquery-demo-backend) — feathers-generated FastAPI service with a seeded 1M-row dataset
- [`Abdul-Muizz1310/slowquery-dashboard-frontend`](https://github.com/Abdul-Muizz1310/slowquery-dashboard-frontend) — Next.js dashboard

## 3-line integration

```python
from slowquery_detective import install

install(app, engine)
```

Optional: mount the dashboard API on the same app.

```python
from slowquery_detective import install, dashboard_router

install(app, engine)
app.include_router(dashboard_router, prefix="/_slowquery")
```

> **Status:** this repo is at **Phase 4a S1 — Bootstrap**. The `install` helper is defined by `docs/specs/05-middleware.md` and lands in S3. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the component map and [`docs/DEMO.md`](docs/DEMO.md) for the reproduction script.

## Install

```bash
pip install slowquery-detective[fastapi]
# or, to enable the LLM fallback:
pip install slowquery-detective[fastapi,llm]
```

## Development

```bash
git clone https://github.com/Abdul-Muizz1310/slowquery-detective
cd slowquery-detective
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run mypy src/
```

## License

MIT — see [LICENSE](LICENSE).
