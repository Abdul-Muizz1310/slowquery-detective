"""Red tests for docs/specs/04-explainer.md.

30 enumerated cases. All HTTP is mocked via respx; no live OpenRouter calls.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest
import respx
from pydantic import SecretStr, ValidationError

import slowquery_detective.llm_explainer as llm_module
from slowquery_detective.llm_explainer import LlmConfig, explain


@pytest.fixture(autouse=True)
def _reset_cooldown() -> None:
    """Clear the module-level cooldown map before every test.

    Cooldown is process-local state by design; tests that share the same
    fingerprint id would otherwise poison each other.
    """
    llm_module._COOLDOWN.clear()


FID = "abcdef0123456789"
BASE = "https://openrouter.ai/api/v1"
PRIMARY = "nvidia/nemotron-nano-9b-v2:free"
FAST = "google/gemma-3-27b-it:free"
FALLBACK = "z-ai/glm-4.5-air:free"


def _config(**overrides: Any) -> LlmConfig:
    kwargs: dict[str, Any] = dict(
        enabled=True,
        api_key=SecretStr("sk-or-v1-test-key"),
        model_primary=PRIMARY,
        model_fast=FAST,
        model_fallback=FALLBACK,
    )
    kwargs.update(overrides)
    return LlmConfig(**kwargs)


def _openrouter_body(
    *,
    diagnosis: str = "Seq scan on a 1M row table with a WHERE predicate.",
    suggestion: str | None = "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
    confidence: float = 0.82,
    kind: str = "index",
) -> dict[str, Any]:
    content = json.dumps(
        {
            "diagnosis": diagnosis,
            "suggestion": suggestion,
            "confidence": confidence,
            "kind": kind,
        }
    )
    return {
        "choices": [{"message": {"content": content}}],
        "model": PRIMARY,
    }


PLAN: dict[str, Any] = {
    "Plan": {
        "Node Type": "Seq Scan",
        "Relation Name": "orders",
        "Plan Rows": 1_000_000,
    }
}
CANONICAL = "SELECT * FROM orders WHERE user_id = ?"


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@respx.mock
async def test_01_primary_returns_valid_suggestion() -> None:
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None
    assert s.source == "llm"
    assert s.kind == "index"
    assert s.confidence == 0.82
    assert s.sql is not None and "ix_orders_user_id" in s.sql


@respx.mock
async def test_02_request_body_carries_sql_and_plan() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert route.called
    body = json.loads(route.calls.last.request.content.decode())
    messages_text = json.dumps(body["messages"])
    assert CANONICAL in messages_text
    assert "Seq Scan" in messages_text


@respx.mock
async def test_03_auth_bearer_header_set() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert route.calls.last.request.headers["authorization"] == "Bearer sk-or-v1-test-key"


@respx.mock
async def test_04_response_format_json_object_requested() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    body = json.loads(route.calls.last.request.content.decode())
    assert body["response_format"] == {"type": "json_object"}


# ---------------------------------------------------------------------------
# Cascade
# ---------------------------------------------------------------------------


@respx.mock
async def test_05_cascade_primary_429_to_fast_200() -> None:
    route = respx.post(f"{BASE}/chat/completions")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(200, json=_openrouter_body()),
    ]
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None
    assert route.call_count == 2
    # Second call should target the FAST model.
    second_body = json.loads(route.calls[1].request.content.decode())
    assert second_body["model"] == FAST


@respx.mock
async def test_06_cascade_to_fallback_on_double_429() -> None:
    route = respx.post(f"{BASE}/chat/completions")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(429),
        httpx.Response(200, json=_openrouter_body()),
    ]
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None
    assert route.call_count == 3
    third_body = json.loads(route.calls[2].request.content.decode())
    assert third_body["model"] == FALLBACK


@respx.mock
async def test_07_all_three_429_returns_none(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    respx.post(f"{BASE}/chat/completions").mock(return_value=httpx.Response(429))
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None
    assert any("cascade_exhausted" in r.message for r in caplog.records)


@respx.mock
async def test_08_cascade_primary_500_to_fast() -> None:
    route = respx.post(f"{BASE}/chat/completions")
    route.side_effect = [
        httpx.Response(500),
        httpx.Response(200, json=_openrouter_body()),
    ]
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None


@respx.mock
async def test_09_401_is_not_retriable(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR)
    route = respx.post(f"{BASE}/chat/completions").mock(return_value=httpx.Response(401))
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None
    assert route.call_count == 1  # no cascade on auth failure
    assert any("auth_failure" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Abstention / schema
# ---------------------------------------------------------------------------


@respx.mock
async def test_10_confidence_below_min_returns_none() -> None:
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body(confidence=0.2))
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None


@respx.mock
async def test_11_null_suggestion_returns_none() -> None:
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body(suggestion=None, confidence=0.9))
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None


@respx.mock
async def test_12_kind_unknown_returns_none() -> None:
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body(kind="unknown"))
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None


@respx.mock
async def test_13_destructive_suggestion_stripped_to_null() -> None:
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(
            200,
            json=_openrouter_body(suggestion="DROP TABLE users;", confidence=0.9),
        )
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None
    assert s.sql is None
    assert "Seq scan" in s.rationale or s.rationale  # diagnosis preserved


@respx.mock
async def test_14_invalid_json_returns_none(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json={"choices": [{"message": {"content": "not json"}}]})
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None
    assert any("invalid_json" in r.message for r in caplog.records)


@respx.mock
async def test_15_missing_confidence_rejected() -> None:
    content = json.dumps({"diagnosis": "...", "suggestion": "CREATE INDEX ...", "kind": "index"})
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None


@respx.mock
async def test_16_confidence_out_of_range_rejected() -> None:
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body(confidence=1.5))
    )
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is None


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------


@respx.mock
async def test_17_cooldown_blocks_second_call() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config(per_fingerprint_cooldown_seconds=60.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=0.0)
    s = await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=10.0)
    assert s is None
    assert route.call_count == 1


@respx.mock
async def test_18_cooldown_expires_after_interval() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config(per_fingerprint_cooldown_seconds=60.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=0.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=61.0)
    assert route.call_count == 2


@respx.mock
async def test_19_cooldown_per_fingerprint() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config()
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id="fid_a", now=0.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id="fid_b", now=1.0)
    assert route.call_count == 2


@respx.mock
async def test_20_injected_now_advances_cooldown() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config(per_fingerprint_cooldown_seconds=5.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=0.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=2.0)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=6.0)
    assert route.call_count == 2


# ---------------------------------------------------------------------------
# Config-gated
# ---------------------------------------------------------------------------


@respx.mock
async def test_21_disabled_short_circuits() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config(enabled=False)
    s = await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=0.0)
    assert s is None
    assert route.call_count == 0


@respx.mock
async def test_22_missing_api_key_short_circuits(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config(api_key=None)
    s = await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=0.0)
    assert s is None
    assert route.call_count == 0
    assert any("missing_key" in r.message for r in caplog.records)


@respx.mock
async def test_23_temperature_propagates_to_request() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    cfg = _config(temperature=0.25)
    await explain(CANONICAL, PLAN, config=cfg, fingerprint_id=FID, now=0.0)
    body = json.loads(route.calls.last.request.content.decode())
    assert body["temperature"] == 0.25


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


@respx.mock
async def test_24_timeout_cascades_to_next_model() -> None:
    route = respx.post(f"{BASE}/chat/completions")
    route.side_effect = [
        httpx.ReadTimeout("simulated"),
        httpx.Response(200, json=_openrouter_body()),
    ]
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None
    assert route.call_count == 2


@respx.mock
async def test_25_connect_error_treated_as_retriable() -> None:
    route = respx.post(f"{BASE}/chat/completions")
    route.side_effect = [
        httpx.ConnectError("simulated"),
        httpx.Response(200, json=_openrouter_body()),
    ]
    s = await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    assert s is not None


async def test_26_malformed_plan_json_raises_type_error() -> None:
    with pytest.raises((TypeError, ValueError)):
        await explain(CANONICAL, "not a dict", config=_config(), fingerprint_id=FID, now=0.0)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


@respx.mock
async def test_27_api_key_not_in_log_records(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    for record in caplog.records:
        assert "sk-or-v1-test-key" not in record.message
        assert "sk-or-v1-test-key" not in str(record.args)


@respx.mock
async def test_28_request_body_contains_no_original_literal() -> None:
    route = respx.post(f"{BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_openrouter_body())
    )
    # Canonical SQL is already parameter-scrubbed by fingerprint.py, so by
    # design the request body carries no original literal. Re-assert.
    await explain(CANONICAL, PLAN, config=_config(), fingerprint_id=FID, now=0.0)
    body = route.calls.last.request.content.decode()
    assert "sk-live-" not in body
    assert "'secret-payload'" not in body


async def test_29_system_prompt_is_module_constant() -> None:
    """System prompt must be a module-level constant, not user-interpolated."""
    import slowquery_detective.llm_explainer as module

    # Look for any uppercase-named STR constant; spec invariant says the
    # prompt lives as a module constant so no user input can reach it.
    prompt_constants = [
        v
        for name, v in vars(module).items()
        if name.isupper() and isinstance(v, str) and "Postgres" in v
    ]
    # In S3 the constant doesn't exist yet; this test will pass once S4
    # introduces SYSTEM_PROMPT: str = "...". For now it fails.
    assert len(prompt_constants) >= 1


def test_30_temperature_cap_enforced_at_config_time() -> None:
    with pytest.raises(ValidationError):
        LlmConfig(
            enabled=True,
            api_key=SecretStr("k"),
            model_primary=PRIMARY,
            model_fast=FAST,
            model_fallback=FALLBACK,
            temperature=0.9,
        )
