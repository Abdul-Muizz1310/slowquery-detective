"""Additional unit tests for explain.py — cover remaining missing lines.

Targets: synthesize_params edge cases, _explain_statement parse paths,
_process_one edge cases (explain returns None, store errors on suggestions).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from slowquery_detective.explain import (
    CachedPlan,
    ExplainJob,
    ExplainWorker,
    synthesize_params,
)
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

FID = "abcdef0123456789"


# ---------------------------------------------------------------------------
# synthesize_params edge cases
# ---------------------------------------------------------------------------


def test_synthesize_no_placeholder_returns_sql_as_is() -> None:
    sql = "SELECT * FROM users"
    assert synthesize_params(sql) == sql


def test_synthesize_date_like_column() -> None:
    result = synthesize_params("SELECT * FROM t WHERE created_at = ?")
    assert result is not None
    assert "now()" in result


def test_synthesize_bool_like_column() -> None:
    result = synthesize_params("SELECT * FROM t WHERE is_active = ?")
    assert result is not None
    assert "true" in result


def test_synthesize_id_like_column() -> None:
    result = synthesize_params("SELECT * FROM t WHERE user_id = ?")
    assert result is not None
    assert "1" in result


def test_synthesize_jsonb_path_returns_null() -> None:
    result = synthesize_params("SELECT jsonb_path_query(data, ?)")
    assert result is not None
    assert "NULL" in result


def test_synthesize_arrow_operator_returns_null() -> None:
    result = synthesize_params("SELECT data->>?")
    assert result is not None
    assert "NULL" in result


def test_synthesize_in_clause_uses_empty_string() -> None:
    result = synthesize_params("SELECT * FROM t WHERE name IN (?)")
    assert result is not None
    assert "''" in result


def test_synthesize_like_clause_uses_empty_string() -> None:
    result = synthesize_params("SELECT * FROM t WHERE name like ?")
    assert result is not None
    assert "''" in result


def test_synthesize_generic_placeholder_uses_one() -> None:
    """When context doesn't match any known pattern, default to 1."""
    result = synthesize_params("SELECT ?")
    assert result is not None
    assert "1" in result


# ---------------------------------------------------------------------------
# _explain_statement parse paths
# ---------------------------------------------------------------------------


def _mock_store() -> MagicMock:
    store = MagicMock(spec=StoreWriter)
    store.upsert_plan = AsyncMock(return_value=None)
    store.insert_suggestions = AsyncMock(return_value=None)
    store.upsert_fingerprint = AsyncMock(return_value=None)
    store.record_sample = AsyncMock(return_value=None)
    return store


def _rules_empty(_plan: dict[str, Any], _sql: str) -> list[Suggestion]:
    return []


async def test_explain_result_dict_not_list() -> None:
    """When EXPLAIN returns a plain dict (not wrapped in list), it should still work."""

    class _DictEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> _Conn:
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, _stmt: Any) -> Any:
                    class _R:
                        def scalar_one(self) -> dict[str, Any]:
                            return {"Plan": {"Node Type": "Seq Scan"}}

                    return _R()

            return _Conn()

    worker = ExplainWorker(
        engine=_DictEngine(),
        store=_mock_store(),
        rules=_rules_empty,
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT 1",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.2)
    await worker.stop()
    assert worker.plan_cache_get(FID) is not None


async def test_explain_result_empty_list_caches_nothing() -> None:
    """When EXPLAIN returns an empty list, plan should be None."""

    class _EmptyEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> _Conn:
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, _stmt: Any) -> Any:
                    class _R:
                        def scalar_one(self) -> list[Any]:
                            return []

                    return _R()

            return _Conn()

    worker = ExplainWorker(
        engine=_EmptyEngine(),
        store=_mock_store(),
        rules=_rules_empty,
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT 1",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.2)
    await worker.stop()
    # Plan is None, so it gets cooldown but no cache entry with a plan
    assert worker.plan_cache_get(FID) is None


async def test_store_insert_suggestions_error_logged_not_fatal() -> None:
    """If store.insert_suggestions raises, the plan is still cached."""
    store = _mock_store()
    store.insert_suggestions = AsyncMock(side_effect=RuntimeError("store broken"))

    suggestion = Suggestion(
        kind="index",
        sql="CREATE INDEX ...",
        rationale="test",
        confidence=0.9,
        source="rules",
    )

    def _rules_with_suggestion(_plan: dict[str, Any], _sql: str) -> list[Suggestion]:
        return [suggestion]

    class _SimpleEngine:
        def connect(self) -> Any:
            class _Conn:
                async def __aenter__(self) -> _Conn:
                    return self

                async def __aexit__(self, *_: object) -> None:
                    return None

                async def execute(self, _stmt: Any) -> Any:
                    class _R:
                        def scalar_one(self) -> list[dict[str, Any]]:
                            return [{"Plan": {"Node Type": "Seq Scan"}}]

                    return _R()

            return _Conn()

    worker = ExplainWorker(
        engine=_SimpleEngine(),
        store=store,
        rules=_rules_with_suggestion,
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT 1",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.2)
    await worker.stop()
    # Plan should still be cached despite store error
    assert worker.plan_cache_get(FID) is not None


async def test_explainer_returns_none_no_suggestions_persisted() -> None:
    """When explainer returns None, no suggestions should be stored."""
    store = _mock_store()
    explainer = AsyncMock(return_value=None)

    worker = ExplainWorker(
        engine=MagicMock(
            connect=lambda: type(
                "_Conn",
                (),
                {
                    "__aenter__": AsyncMock(return_value=MagicMock(
                        execute=AsyncMock(return_value=MagicMock(
                            scalar_one=lambda: [{"Plan": {"Node Type": "Seq Scan"}}]
                        ))
                    )),
                    "__aexit__": AsyncMock(return_value=None),
                },
            )(),
        ),
        store=store,
        rules=_rules_empty,
        explainer=explainer,
        now=lambda: 0.0,
    )
    await worker.start()
    worker.submit(
        ExplainJob(
            fingerprint_id=FID,
            canonical_sql="SELECT 1",
            observed_ms=500.0,
            enqueued_at=0.0,
        )
    )
    await asyncio.sleep(0.2)
    await worker.stop()
    explainer.assert_awaited_once()
    store.insert_suggestions.assert_not_awaited()


async def test_start_twice_is_idempotent() -> None:
    """Starting an already-running worker should be a no-op."""
    worker = ExplainWorker(
        engine=MagicMock(),
        store=_mock_store(),
        rules=_rules_empty,
        explainer=None,
        now=lambda: 0.0,
    )
    await worker.start()
    task1 = worker._task
    await worker.start()  # no-op
    assert worker._task is task1
    await worker.stop()
