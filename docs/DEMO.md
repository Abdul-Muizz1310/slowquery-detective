# Demo

Two ways to see `slowquery-detective` working.

## Option A — Minimal toy app (package-level)

Proves the release is installable, the three-line integration works, and a tagged request's queries
can be read back. No Docker, no external services, no LLM credits: SQLite stands in for the app
database and the in-memory store is used (no `store_url`). `EXPLAIN` is Postgres-only, so on SQLite
the worker simply finds no plan; fingerprints, counts and request attribution all work.

### Requirements

- Python 3.12+
- `uv` (or any PEP 517 installer)

### Script

```bash
# 1. Fresh venv
uv venv --python 3.12 /tmp/sq-demo
source /tmp/sq-demo/bin/activate

# 2. Install the library from PyPI, plus a server and the SQLite driver
uv pip install "slowquery-detective[fastapi]" uvicorn aiosqlite

# 3. A small FastAPI app
cat > /tmp/sq_app.py <<'PY'
from fastapi import FastAPI, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from slowquery_detective import install

app = FastAPI()
engine = create_async_engine("sqlite+aiosqlite:///:memory:")


async def demo_only(request: Request) -> bool:
    # Your real hook verifies a token; this one just checks a header.
    return request.headers.get("x-demo-key") == "let-me-in"


install(app, engine, service="demo", authorize=demo_only)


@app.get("/work")
async def work() -> dict[str, int]:
    async with engine.connect() as conn:
        a = (await conn.execute(text("SELECT 1 + :n"), {"n": 1})).scalar_one()
        b = (await conn.execute(text("SELECT 1 + :n"), {"n": 2})).scalar_one()
    return {"sum": a + b}
PY

# 4. Run it
uvicorn --app-dir /tmp sq_app:app --port 8000
```

In a second terminal:

```bash
curl -s -H 'X-Request-Id: demo-1' http://localhost:8000/work
# -> {"sum":5}

curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8000/_slowquery/queries
# -> 403 (the authorize hook said no)

curl -s -H 'x-demo-key: let-me-in' 'http://localhost:8000/_slowquery/queries?request_id=demo-1'
# -> [{"id":"…","fingerprint":"select ? + ?","call_count":2,…,"p50_ms":null,…,"suggestions":[]}]
```

This proves:

1. `pip install "slowquery-detective[fastapi]"` succeeds from public PyPI.
2. `install(app, engine, service=..., authorize=...)` wires the middleware, the hooks and the routes.
3. A request tagged `X-Request-Id: demo-1` is listed with the queries it ran (both executions of
   the parameterized statement collapse into one fingerprint with `call_count` 2).
4. The routes refuse callers the hook does not accept.

For persistence across processes and restarts, add `store_url=<a Postgres URL>` and install the
`postgres` extra.

## Option B — Full live demo

The "watch a query's p95 drop on a branch swap" experience needs the demo backend and dashboard
frontend, which are separate repos:

- **`slowquery-demo-backend`** — FastAPI service with a seeded commerce dataset across two Neon
  branches: `slowquery` (missing indexes) and `slowquery-fast` (correct indexes). See its
  `docs/DEVIATIONS.md` for what ships versus what was planned.
- **`slowquery-dashboard-frontend`** — Next.js app with a fingerprint table, an EXPLAIN plan
  viewer, a live p95 timeline and the slow/fast branch switch.

Both pin a `slowquery-detective` version. That is the point of the split: the library iterates
independently, and the demo proves the integration surface across releases.

## Programmatic smoke test

```python
from slowquery_detective import __version__, install
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.fingerprint import fingerprint
from slowquery_detective.plan_scrub import scrub_plan
from slowquery_detective.rules import run_rules
from slowquery_detective.tagging import parse_request_id

assert __version__ == "0.2.0"

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

# Plans lose their literal values before they are stored.
plan = scrub_plan({"Plan": {"Filter": "(email = 'a@b.c'::text)"}})
assert plan == {"Plan": {"Filter": "(email = ?::text)"}}

# Request ids are parsed, not trusted.
assert parse_request_id("3f2b8c1e-9a4d-4e7f-8c21-5b6a7d8e9f01") is not None
assert parse_request_id("not ok") is None
```

Running this against `slowquery-detective 0.2.0` from public PyPI prints nothing and exits 0.
