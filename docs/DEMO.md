# Demo

> Status: placeholder. The reproduction script lands with `slowquery-demo-backend` in Phase 4b. This file will describe the 60-second local demo once the seeded Neon branches are live.

## Target script (to be filled in)

1. Clone `slowquery-demo-backend`, `uv sync`, point it at the `slowquery` branch.
2. Start the backend; a Locust traffic generator fires ~100 req/s.
3. Open the dashboard; watch a query's p95 spike red.
4. Click the fingerprint → side panel shows `EXPLAIN` plan and the rules-engine suggestion.
5. Hit "Apply on fast branch" → Neon API swaps `DATABASE_URL` → p95 drops from ~1200ms to ~18ms live.
6. The README gif captures the drop.
