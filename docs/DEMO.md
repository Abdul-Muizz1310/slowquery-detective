# Demo

Two ways to see `slowquery-detective` working.

## Option A — Minimal toy app (package-level)

This is what S6 acceptance for Phase 4a proves: the PyPI release is installable, the 3-line integration works, and the dashboard router is mountable on any FastAPI app. No Docker, no external services, no LLM credits.

### Requirements

- Python 3.12+
- `uv` (or any PEP 517 installer; uv just makes the venv boilerplate nicer)
- A Postgres you can point at, or nothing — the minimal app works with SQLite in-memory for import smoke-testing

### Script

```bash
# 1. Fresh venv
uv venv --python 3.12 /tmp/sq-demo
source /tmp/sq-demo/bin/activate         # on bash
# or: /tmp/sq-demo/Scripts/activate      # on Windows

# 2. Install the library from PyPI
uv pip install "slowquery-detective[fastapi]"

# 3. Write a 15-line FastAPI app
cat > /tmp/sq_app.py <<'PY'
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from slowquery_detective import install, dashboard_router

app = FastAPI()
engine = create_async_engine("sqlite+aiosqlite:///:memory:")

install(app, engine)
app.include_router(dashboard_router, prefix="/_slowquery")

@app.get("/ping")
async def ping() -> dict[str, str]:
    return {"pong": "ok"}
PY

# 4. Run it (uses uvicorn bundled with FastAPI extras)
uv pip install uvicorn aiosqlite
uvicorn --app-dir /tmp sq_app:app --port 8000
```

In a second terminal:

```bash
curl -s http://localhost:8000/ping
# -> {"pong":"ok"}

curl -s http://localhost:8000/_slowquery/api/queries
# -> [] (no slow queries yet — the ping handler is too fast to cross the default 100ms threshold)
```

This proves all four packaging concerns:

1. `pip install slowquery-detective[fastapi]` succeeds from public PyPI (23+ transitive deps resolve cleanly).
2. `from slowquery_detective import install, dashboard_router` imports without error.
3. `install(app, engine)` on a real `AsyncEngine` wires the hooks + the explain worker.
4. `app.include_router(dashboard_router, prefix="/_slowquery")` mounts the API surface; `/_slowquery/api/queries` responds 200.

## Option B — Full live demo (Phase 4b + 4c)

The "watch a query's p95 drop from 1200ms to 18ms in real time on a branch swap" experience needs the demo backend and dashboard frontend, which are separate repos:

- **`slowquery-demo-backend`** ([Phase 4b](../../docs/PLAN.md#94-phase-4--slowquery-detective--slowquery-demo-backend--slowquery-dashboard-frontend)) — feathers-generated FastAPI service with a 1M-row dataset seeded across two Neon branches: `slowquery` (missing indexes) and `slowquery-fast` (correct indexes). A Locust traffic generator fires ~100 req/s to keep the dashboard lively.
- **`slowquery-dashboard-frontend`** ([Phase 4c](../../docs/PLAN.md#94-phase-4--slowquery-detective--slowquery-demo-backend--slowquery-dashboard-frontend)) — Next.js app with Recharts for timelines, Monaco for plan viewing, and the "Apply on fast branch" button that swaps `DATABASE_URL` between the two Neon branches and watches p95 drop live.

Both repos pin to a `slowquery-detective` version via their `pyproject.toml` / the demo backend's `uv.lock`. That's the whole point of the 3-repo split: the library can be iterated independently, and the demo proves the integration surface stays stable across releases.

## What to expect when 4b + 4c are live

```
[traffic generator] ----100 req/s----> [demo backend (Render)]
                                              |
                                              v
                                       slowquery-detective
                                              |
                                      (fingerprint → buffer
                                       → rules → LLM fallback)
                                              |
                                              v
                                        Postgres store
                                              |
                 +----------SSE----------------+
                 |
                 v
          [Dashboard (Vercel)]
          - Fingerprint table (sorted by total_ms desc)
          - Live p95 line chart (recharts)
          - Click fingerprint → plan + rule suggestions
          - "Apply on fast branch" button → 1200ms → 18ms live
```

The README gif target is "click Apply → watch the p95 line fall off a cliff within 3 seconds". That needs the dashboard, so it lands in 4c.

## Programmatic smoke test

This is what CI runs on every tag push during S5:

```python
from slowquery_detective import install, dashboard_router, __version__
from slowquery_detective.fingerprint import fingerprint
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.rules import run_rules
from slowquery_detective.dashboard import DDL_ALLOWLIST_REGEX

assert __version__ == "0.1.0"

# Fingerprinting collapses parameterized queries.
fp_a, _ = fingerprint("SELECT * FROM users WHERE id = 1")
fp_b, _ = fingerprint("SELECT * FROM users WHERE id = 42")
assert fp_a == fp_b

# Ring buffer computes percentiles on demand.
rb = RingBuffer()
rb.record("x", 5.0, now=0.0)
p = rb.percentiles("x", now=0.0)
assert p is not None and p.p95_ms == 5.0

# Rules engine dispatches without errors on an empty plan.
assert run_rules({}, "", fingerprint_id="x") == []

# DDL allowlist accepts valid DDL, rejects everything else.
assert DDL_ALLOWLIST_REGEX.match("CREATE INDEX IF NOT EXISTS ix_a ON b(c);") is not None
assert DDL_ALLOWLIST_REGEX.match("DROP TABLE users;") is None
```

Running this against `slowquery-detective 0.1.0` from public PyPI should print nothing and exit 0.
