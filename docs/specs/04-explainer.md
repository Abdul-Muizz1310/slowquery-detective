# 04 — OpenRouter LLM fallback

## Goal

When the rules engine returns an empty list, ask an LLM to explain the plan in plain English and suggest one concrete fix. The call must be cheap, infrequent, rate-limited per fingerprint, and must return structured JSON so the dashboard can render the suggestion the same way it renders rule-driven ones. Cascades through the muizz-lab OpenRouter model triple (PRIMARY → FAST → FALLBACK) on 429/5xx per `project_openrouter_models` memory.

## Module

`package/src/slowquery_detective/llm_explainer.py`

## Public API

```python
class LlmConfig(BaseSettings):
    enabled: bool = False
    api_key: SecretStr | None = None
    base_url: HttpUrl = "https://openrouter.ai/api/v1"
    model_primary: str
    model_fast: str
    model_fallback: str
    temperature: float = 0.1
    min_confidence: float = 0.4
    per_fingerprint_cooldown_seconds: float = 60.0

async def explain(
    canonical_sql: str,
    plan_json: dict,
    *,
    config: LlmConfig,
    fingerprint_id: str,
    now: float | None = None,
) -> Suggestion | None: ...
```

- Returns `None` when disabled, when no API key is set, when the cooldown is active, or when the model abstains (confidence below `min_confidence`).
- Never raises on upstream failures — logs and returns `None`.
- Uses the `openai` client configured with `base_url=config.base_url` (OpenRouter is OpenAI-compatible).

## Prompt contract

System prompt (short, strict): *"You are a Postgres performance expert. A query is slow. Given its canonical SQL and EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) plan, return a JSON object — nothing else."*

Response schema (enforced via `response_format={"type": "json_object"}`):

```json
{
  "diagnosis": "one paragraph explaining why this is slow",
  "suggestion": "CREATE INDEX ... or query rewrite, single SQL statement, or null",
  "confidence": 0.0,
  "kind": "index" | "rewrite" | "denormalize" | "partition" | "unknown"
}
```

On a valid response:
1. If `confidence < config.min_confidence`, return `None` (abstain).
2. If `kind == "unknown"` or `suggestion` is `null`, return `None`.
3. If `suggestion` contains any SQL verb other than `CREATE INDEX`, return the Suggestion with `sql=None` and the diagnosis preserved (no executable DDL).
4. Otherwise, return `Suggestion(source="llm", rule_name=None, ...)`.

## Inputs / Outputs / Invariants

1. **Called only when rules miss** — enforced by the caller (`run_rules` → empty list → `explain`). The module itself does not re-check.
2. **Cooldown** — at most one call per `fingerprint_id` per `per_fingerprint_cooldown_seconds`. Enforced by an in-memory dict `{fingerprint_id: last_called_at}`. Uses injected `now` for testability.
3. **Cascade** — on HTTP 429 or 5xx from `model_primary`, retry once on `model_fast`; on another 429/5xx, retry on `model_fallback`. No further retries.
4. **Deterministic fallback** — any exception (network, JSON parse, schema validation) returns `None` after logging.
5. **No raw secrets in logs** — API key never appears in any log record.
6. **No literal leakage** — the prompt uses `canonical_sql` (literals already parameterized); raw user SQL is never sent.
7. **Timeout** — 15 seconds per attempt, 45 seconds total worst case.
8. **Strict JSON parse** — uses `json.loads` + Pydantic validation; a response that is valid JSON but doesn't match the schema is treated as an abstention.
9. **Temperature** — `config.temperature` default `0.1`; never above `0.3`.
10. **Config-gated** — `LlmConfig(enabled=False)` short-circuits before any network call; `api_key is None` logs a warning and short-circuits.

## Enumerated test cases

All HTTP calls are mocked via `respx` (recorded fixtures for the happy path, hand-written for failure branches).

### Happy path

1. Primary returns a valid JSON with `confidence=0.82` and a `CREATE INDEX` suggestion → `explain(...)` returns a `Suggestion(source="llm", kind="index", confidence=0.82, sql="CREATE INDEX ...")`.
2. `canonical_sql` and `plan_json` both appear in the request body sent to OpenRouter (asserted via `respx` request snapshot).
3. `Authorization: Bearer <api_key>` header set from `config.api_key.get_secret_value()`.
4. `response_format={"type": "json_object"}` present in the request payload.

### Cascade

5. Primary returns 429 → retries on FAST → FAST returns 200 → result returned; `primary` and `fast` both seen by `respx`.
6. Primary and FAST both 429 → retries on FALLBACK → FALLBACK returns 200 → result.
7. All three return 429 → returns `None`, logs `"slowquery.llm.cascade_exhausted"` at `WARNING`.
8. Primary returns 500, FAST returns 200 → result from FAST.
9. Primary returns 401 (auth) → no cascade (not a retriable failure); returns `None`, logs `"slowquery.llm.auth_failure"` at `ERROR`.

### Abstention / schema

10. Model returns `{"confidence": 0.2, ...}` → abstain → `None`.
11. Model returns `{"suggestion": null, "confidence": 0.9, ...}` → abstain → `None`.
12. Model returns `{"kind": "unknown", ...}` → abstain → `None`.
13. Model returns a suggestion containing `DROP TABLE users` → returned with `sql=None`, diagnosis preserved.
14. Model returns invalid JSON → returns `None`, logs `"slowquery.llm.invalid_json"`.
15. Model returns valid JSON but missing `confidence` key → Pydantic validation fails → `None`.
16. Model returns `confidence > 1.0` or `< 0.0` → Pydantic validation fails → `None`.

### Cooldown

17. Two `explain(...)` calls for the same fingerprint within 10s → second call returns `None` without hitting the network (asserted: zero requests on the second call).
18. Two calls for the same fingerprint 61s apart → both hit the network.
19. Two calls for *different* fingerprints within 10s → both hit the network.
20. Cooldown respects injected `now`; advancing the injected clock past the cooldown unblocks.

### Config-gated

21. `LlmConfig(enabled=False)` → `explain` returns `None`, zero network calls.
22. `LlmConfig(enabled=True, api_key=None)` → `explain` returns `None`, logs `"slowquery.llm.missing_key"` at `WARNING`, zero network calls.
23. `config.temperature = 0.5` → used in the request body.

### Failure modes

24. Network timeout (15s) → `respx` simulates a hang → cascade moves to next model.
25. `httpx.ConnectError` → treated as 5xx for cascade purposes.
26. Malformed `plan_json` (not a dict) → `TypeError` at call site before network; pre-validated by Pydantic.

### Security

27. API key never appears in structlog output. Asserted by capturing log records and grep-asserting the secret string is absent.
28. The request body sent to OpenRouter contains `canonical_sql` but does **not** contain any literal like `'sk-live-'` or raw personal data. (Guaranteed by fingerprint step but re-asserted.)
29. The system prompt is loaded from a constant in the module; no user-controlled string is ever interpolated into it.
30. `temperature` capped at `0.3` — a `LlmConfig(temperature=0.9)` raises `ValidationError` at config time.

## Acceptance criteria

- [ ] `explain` coroutine + `LlmConfig` exported from `llm_explainer.py`.
- [ ] Tests 1–30 pass; all network mocked via `respx`.
- [ ] One `vcrpy` cassette records a real OpenRouter happy-path call (run manually, then committed); `pytest.mark.slow` gates its replay in CI.
- [ ] Cascade PRIMARY → FAST → FALLBACK implemented in a dedicated helper so the tests can parametrize status codes cleanly.
- [ ] No literal / secret leakage verified by test 27 and test 28.
- [ ] mypy-strict clean.
