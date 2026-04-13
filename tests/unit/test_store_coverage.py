"""Unit tests for store.py — the abstract StoreWriter base class.

Every method raises NotImplementedError by design. These tests
verify the contract and improve coverage of the base class.
"""

from __future__ import annotations

import pytest

from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter


def test_store_writer_constructor() -> None:
    writer = StoreWriter("postgresql://localhost/test")
    assert writer._store_url == "postgresql://localhost/test"


async def test_upsert_fingerprint_raises_not_implemented() -> None:
    writer = StoreWriter("postgresql://x")
    with pytest.raises(NotImplementedError, match="upsert_fingerprint"):
        await writer.upsert_fingerprint("abc", "SELECT 1")


async def test_record_sample_raises_not_implemented() -> None:
    writer = StoreWriter("postgresql://x")
    with pytest.raises(NotImplementedError, match="record_sample"):
        await writer.record_sample("abc", 42.0)


async def test_upsert_plan_raises_not_implemented() -> None:
    writer = StoreWriter("postgresql://x")
    with pytest.raises(NotImplementedError, match="upsert_plan"):
        await writer.upsert_plan("abc", {"Plan": {}}, "text", 1.0)


async def test_insert_suggestions_raises_not_implemented() -> None:
    writer = StoreWriter("postgresql://x")
    s = Suggestion(
        kind="index",
        sql="CREATE INDEX ...",
        rationale="test",
        confidence=0.9,
        source="rules",
    )
    with pytest.raises(NotImplementedError, match="insert_suggestions"):
        await writer.insert_suggestions("abc", [s])


async def test_close_raises_not_implemented() -> None:
    writer = StoreWriter("postgresql://x")
    with pytest.raises(NotImplementedError, match="close"):
        await writer.close()
