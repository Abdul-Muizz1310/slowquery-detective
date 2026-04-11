# 03 — Rules engine

## Goal

A deterministic, pure set of rules that inspects an `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plan plus the canonical SQL and returns at most one concrete suggestion per rule. Rules cover the common-case wins (80% of real speedups are boring: missing indexes, sequential scans on filtered tables, sort-without-index, function-wrapped WHERE columns, SELECT * on wide tables, and N+1 patterns). When no rule matches, the caller falls back to the LLM explainer (see `04-explainer.md`).

## Module

`package/src/slowquery_detective/rules/`

## Public API

```python
class Suggestion(BaseModel):
    kind: Literal["index", "rewrite", "denormalize", "partition"]
    sql: str | None
    rationale: str
    confidence: float              # 0.0..1.0
    source: Literal["rules", "llm"]
    rule_name: str | None          # populated for rules; None for llm

class Rule(Protocol):
    name: str
    def apply(self, plan: PlanNode, canonical_sql: str) -> Suggestion | None: ...

ALL_RULES: tuple[Rule, ...] = (...)

def run_rules(
    plan: dict,
    canonical_sql: str,
    *,
    fingerprint_id: str,
    recent_call_count: int = 0,
) -> list[Suggestion]:
    """Return every matching rule's suggestion, sorted by confidence desc."""
```

- Rules are **pure**: no I/O, no database access, no time calls. They receive the parsed plan and canonical SQL, return a `Suggestion` or `None`.
- Rules live in individual files (`rules/seq_scan.py`, `rules/missing_fk_index.py`, etc.) and are assembled in `rules/__init__.py`.
- `run_rules` returns a **list** because a single plan can match multiple rules (e.g. seq-scan + sort-without-index). The highest-confidence one wins in the UI but the full list is persisted.

## The six rules

| # | name | Triggers when | Suggests |
|---|---|---|---|
| 1 | `seq_scan_large_table` | `Seq Scan` node with `Plan Rows > 10_000` and an equality/range WHERE predicate in the canonical SQL | `CREATE INDEX IF NOT EXISTS ix_<table>_<col> ON <table>(<col>);` |
| 2 | `missing_fk_index` | `Hash Join` / `Nested Loop` where the inner side is a `Seq Scan` on a column named `<*>_id` and no matching index exists | `CREATE INDEX IF NOT EXISTS ix_<table>_<fk_col> ON <table>(<fk_col>);` |
| 3 | `sort_without_index` | `Sort` node with cost > 1000 and an `ORDER BY` in the canonical SQL whose columns are not served by an index scan | Composite index ending with the ORDER BY column(s) |
| 4 | `function_in_where` | Canonical SQL contains `WHERE <fn>(<col>) = ?` (e.g. `LOWER`, `DATE`, `UPPER`) | Functional index: `CREATE INDEX ... ON <table>(<fn>(<col>))` or rewrite suggestion if unsafe |
| 5 | `select_star_wide_table` | Canonical SQL has `SELECT *` AND the plan's top node reports `Output` with more than 20 columns | "Project explicit columns" — suggestion `kind="rewrite"`, `sql=None` |
| 6 | `n_plus_one_suspicion` | `recent_call_count >= 50` on the same fingerprint within the 60s window | "Likely N+1; eager-load via `selectinload`/`joinedload`" — `kind="rewrite"`, `sql=None` |

## Inputs / Outputs / Invariants

1. **Pure functions** — rules never touch the network, disk, or clock.
2. **Plan parsing** — input is a Python `dict` produced by `json.loads(explain_output)`. The rules module does not re-parse plan text.
3. **Canonical SQL** — comes from `fingerprint.py`; literals are already `?`. Rules never see raw literals.
4. **At-most-one per rule** — each rule returns a single `Suggestion` or `None`. If multiple matches exist in the plan (e.g., two seq scans), the rule picks the highest-cost one.
5. **Confidence bounds** — every suggestion reports a confidence ∈ `[0.0, 1.0]`. Rules for deterministic patterns (missing FK index) return ≥ 0.9; heuristic rules (N+1) return 0.6–0.75.
6. **DDL safety** — every `CREATE INDEX` suggestion uses `IF NOT EXISTS` and a deterministic name `ix_<table>_<col>` so re-application is a no-op. No `DROP`, `ALTER`, `TRUNCATE`, or `CREATE OR REPLACE` is ever suggested.
7. **Ordering** — `run_rules` returns suggestions sorted by `confidence` descending, then by `rule_name` ascending (stable).
8. **Empty plan** — `run_rules({}, "")` returns `[]`.
9. **No raise on weird plans** — unknown node types are skipped, not errored.

## Enumerated test cases

### Fixture plans

Each rule has a `tests/fixtures/plans/` directory containing:
- `<rule>_positive.json` — a plan where the rule must fire.
- `<rule>_negative.json` — a plan where it must not fire.
- `<rule>_edge.json` — a boundary case (e.g., exactly 10_000 rows for seq_scan).

### Per-rule tests

1. `seq_scan_large_table` fires on a `Seq Scan` with `Plan Rows = 50_000` and `canonical = "SELECT * FROM orders WHERE user_id = ?"` → suggestion has `CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);`, confidence ≥ 0.85.
2. `seq_scan_large_table` does **not** fire when `Plan Rows = 500`.
3. `seq_scan_large_table` does **not** fire when there is no WHERE clause (full-table export).
4. `seq_scan_large_table` picks the highest-cost `Seq Scan` when the plan has two.
5. `missing_fk_index` fires on a Nested Loop whose inner is `Seq Scan` on `orders` filtered by `user_id`.
6. `missing_fk_index` does **not** fire when the inner side is an `Index Scan`.
7. `sort_without_index` fires on `Sort` with `Actual Total Time > 100` and `ORDER BY created_at` in the SQL.
8. `sort_without_index` does **not** fire when the plan uses an `Index Scan` that already returns pre-sorted rows.
9. `function_in_where` fires on `SELECT ... WHERE LOWER(email) = ?`; suggests `CREATE INDEX ix_users_email_lower ON users(LOWER(email))`.
10. `function_in_where` does **not** fire on `WHERE email = LOWER(?)` (function applied to parameter, not column).
11. `select_star_wide_table` fires when `Output` has 25 columns and canonical SQL contains `SELECT *`; suggestion `kind="rewrite"`, `sql=None`.
12. `select_star_wide_table` does **not** fire when column count is 8.
13. `n_plus_one_suspicion` fires when `recent_call_count = 60`; does not fire when `recent_call_count = 20`.

### Aggregate / ordering

14. A plan that triggers seq_scan + sort_without_index returns two suggestions; seq_scan ranks first because it has higher confidence.
15. A plan that triggers no rules returns `[]`.
16. Two rules with identical confidence return in alphabetical order by `rule_name` (stability).

### Failure / edge

17. `run_rules(None, "...")` → `TypeError`.
18. `run_rules({}, "")` → `[]`.
19. Plan with an unknown `Node Type` (e.g., `Gather Merge`) does not raise; it is walked for children but not matched by any current rule.
20. Malformed plan (missing `Plan` key) → `[]`, no raise.
21. Plan where `Plan Rows` is a string (some Postgres serializers do this) → coerced to int or skipped, not raised.
22. Rule suggestion for a table named with a SQL keyword (e.g. `"user"`) emits quoted identifier in the DDL.

### Security

23. `Suggestion.sql` never contains a string interpolated from `canonical_sql`; only table/column names extracted structurally from the plan are used. Verified by a test that passes a canonical SQL containing a payload like `"'); DROP TABLE users; --"` — the payload never appears in any generated DDL.
24. Table/column names are validated against a whitelist regex (`^[A-Za-z_][A-Za-z0-9_]*$`) before being formatted into DDL. Names failing the regex cause the rule to return `None` rather than emit suggestive SQL.
25. No rule emits `DROP`, `ALTER`, `TRUNCATE`, `GRANT`, `REVOKE`, `UPDATE`, or `DELETE`. Asserted by a grep test over `src/slowquery_detective/rules/`.

## Acceptance criteria

- [ ] All six rules live in individual files under `rules/` and are registered via `ALL_RULES`.
- [ ] Tests 1–25 pass.
- [ ] Every rule has at least one positive and one negative fixture in `tests/fixtures/plans/`.
- [ ] `Suggestion` is a Pydantic `BaseModel` with strict validation (`model_config = ConfigDict(extra="forbid", strict=True)`).
- [ ] DDL safety grep test runs in CI.
- [ ] mypy-strict clean.
