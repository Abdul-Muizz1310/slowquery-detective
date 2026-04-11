# 00 — Fingerprint

## Goal

Collapse literal-equivalent SQL statements into a single stable identifier so the dashboard shows query *patterns*, not 10,000 per-literal rows. Parse once via `sqlglot`, strip literals and parameter placeholders, re-serialize, hash. When `sqlglot` cannot parse the statement, fall back to a deterministic regex so the middleware never crashes on exotic Postgres syntax.

## Module

`package/src/slowquery_detective/fingerprint.py`

## Public API

```python
def fingerprint(sql: str, dialect: str = "postgres") -> tuple[str, str]:
    """Return (fingerprint_id, canonical_sql)."""
```

- `fingerprint_id` — 16-character lowercase hex (SHA1 prefix of the canonical SQL).
- `canonical_sql` — the re-serialized statement with every literal and parameter replaced by `?`, comments stripped, whitespace collapsed.

## Inputs

- `sql: str` — non-empty. May contain inline parameters (`$1`, `%s`, `?`, `:name`), numeric / string / boolean / null literals, comments, trailing whitespace, and any Postgres-dialect construct sqlglot supports.
- `dialect: str` — defaults to `"postgres"`; any other value is passed through to sqlglot.

## Outputs / Invariants

1. **Determinism** — `fingerprint(sql) == fingerprint(sql)` for identical input, always.
2. **Pattern stability** — `WHERE id = 1` and `WHERE id = 2` return the same `fingerprint_id`.
3. **Structural sensitivity** — `WHERE id = 1` and `WHERE email = 'x'` return *different* `fingerprint_ids`.
4. **Literal scrub** — the returned `canonical_sql` never contains any integer, float, string, boolean, or `NULL` literal from the input. (Security invariant: literals are often PII.)
5. **Parameter normalization** — `$1`, `%s`, `:name`, `?` all collapse to `?`.
6. **IN-list collapse** — `IN (1, 2, 3)` and `IN (4, 5, 6, 7, 8)` return the same fingerprint (single `?` or equivalent).
7. **Comment strip** — `-- foo` and `/* bar */` do not affect the fingerprint.
8. **Whitespace insensitivity** — differing indentation / newlines do not change the output.
9. **Identifiers preserved** — table and column names must appear unchanged in `canonical_sql`.
10. **Bounded cost** — `O(len(sql))` time, no unbounded recursion on nested expressions.
11. **Fallback path** — when `sqlglot.parse_one` raises, the function returns a regex-derived fingerprint; it never propagates `ParseError` to callers.

## Enumerated test cases

### Happy path

1. `SELECT * FROM users WHERE id = 1` and `... WHERE id = 42` → same `fingerprint_id`.
2. `SELECT * FROM users WHERE id = 1` and `SELECT * FROM users WHERE email = 'a'` → different `fingerprint_ids`.
3. `SELECT * FROM orders WHERE total > 99.95` and `... > 12.0` → same fingerprint; `canonical_sql` contains no numeric literal.
4. `INSERT INTO t (a, b) VALUES (1, 'x')` and `... VALUES (2, 'y')` → same fingerprint.
5. `UPDATE users SET active = true WHERE id = 1` and `... active = false WHERE id = 2` → same fingerprint.
6. Parameter styles — `$1`, `%s`, `:name`, `?` in otherwise-identical queries → same fingerprint.
7. `canonical_sql` for `SELECT 1` preserves `SELECT ?` shape and contains no digit `1`.
8. `fingerprint_id` is exactly 16 lowercase hex characters for every valid input.

### Edge cases

9. `IN (1, 2, 3)` and `IN (4, 5, 6, 7, 8, 9)` → same fingerprint.
10. `IN (1)` and `IN (1, 2)` → same fingerprint (arity-insensitive IN list).
11. `LIMIT 10` vs `LIMIT 20` → same fingerprint; `OFFSET` likewise.
12. `ORDER BY created_at DESC` vs `ORDER BY created_at ASC` → *different* fingerprint (direction is structural).
13. Leading `/* X-Request-Id: ... */` comments are stripped.
14. Line-comment `-- traceparent` is stripped.
15. Trailing semicolon does not change the fingerprint.
16. Mixed case (`select * from USERS`) and canonical case (`SELECT * FROM users`) → same fingerprint *after* normalization.
17. Multiline whitespace-heavy SQL and the same SQL collapsed to one line → same fingerprint.
18. Deeply nested subqueries (10 levels) do not blow the Python recursion limit — bounded iterative walk.

### Failure cases

19. Empty string → `ValueError`.
20. Whitespace-only string → `ValueError`.
21. `None` → `TypeError` (or `ValueError`; document which).
22. Unparseable SQL (`SELECT FROM`) does **not** raise; returns a deterministic regex-based fingerprint tagged as `fallback`.
23. Extremely long SQL (1 MB of SELECT ... UNION ...) completes in <500ms and does not OOM.
24. Dialect `"mysql"` produces valid output without crashing; differs from the `"postgres"` output for dialect-specific constructs.

### Security cases

25. `SELECT * FROM users WHERE ssn = '123-45-6789'` — `canonical_sql` must not contain the digits `123456789` or the string `ssn = '`.
26. `SELECT * FROM users WHERE api_key = 'sk-live-abc123'` — secret literal scrubbed; `'sk-live-' `substring must be absent from `canonical_sql`.
27. A SQL-injection-style payload in a literal (`' OR 1=1 --`) does not change the fingerprint vs a benign literal in the same position; both map to the same pattern.
28. `canonical_sql` is safe to log — no literal from the input survives. (Asserted by property-based test: `hypothesis.given(random literals)` → `literal not in canonical`.)

## Acceptance criteria

- [ ] `fingerprint.py` exposes `fingerprint(sql, dialect="postgres") -> tuple[str, str]`.
- [ ] Tests 1–28 all pass.
- [ ] `test_fingerprint.py` includes a hypothesis property test for invariant 4 (literal scrub).
- [ ] Fallback path is exercised in CI with at least one deliberately-unparseable fixture.
- [ ] `fingerprint_id` is stable across Python process restarts (SHA1 is deterministic; no `hash()` salting).
- [ ] mypy-strict clean.
