"""Spec 07 case 16 — the Postgres store's own engine settings (no database)."""

from __future__ import annotations

import pytest

from slowquery_detective.pg_store import CONNECT_TIMEOUT_S, PostgresStoreWriter, connect_args_for


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "postgresql+asyncpg://u:p@ep-x.neon.tech/db",
            {"timeout": CONNECT_TIMEOUT_S},
        ),
        (
            "postgresql+asyncpg://u:p@ep-x-pooler.neon.tech/db",
            {"timeout": CONNECT_TIMEOUT_S, "statement_cache_size": 0},
        ),
        (
            "postgresql+psycopg://u:p@ep-x-pooler.neon.tech/db",
            {"connect_timeout": int(CONNECT_TIMEOUT_S)},
        ),
    ],
)
def test_16_connect_args(url: str, expected: dict[str, object]) -> None:
    assert connect_args_for(url) == expected


def test_16_connect_timeout_is_short() -> None:
    # A read route may wait on the store; asyncpg's own default is 60 s.
    assert 0 < CONNECT_TIMEOUT_S <= 10


async def test_16_engine_is_small_and_separate() -> None:
    store = PostgresStoreWriter("postgresql://u:p@127.0.0.1:1/db")
    try:
        assert store.engine.pool.size() == 2  # type: ignore[attr-defined]
        assert store.url == "postgresql+asyncpg://u:p@127.0.0.1:1/db"
    finally:
        await store.close()


def test_16_invalid_url_is_rejected_before_any_engine() -> None:
    with pytest.raises(ValueError):
        PostgresStoreWriter("mysql://u:p@h/db")
