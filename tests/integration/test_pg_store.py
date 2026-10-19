"""Spec 07 cases 34-42 against a real Postgres (Testcontainers)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from slowquery_detective.pg_store import PostgresStoreWriter
from slowquery_detective.store import StoreWriter
from tests.store_contract import FP_A, SERVICE, StoreContract, batch, suggestion

pytestmark = pytest.mark.integration

TABLES = {
    "query_fingerprints",
    "request_stats",
    "query_samples",
    "explain_plans",
    "suggestions",
    "schema_version",
}


def asyncpg_url(pg: PostgresContainer) -> str:
    return pg.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")


@pytest.fixture()
async def admin(pg: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    """An engine for inspecting the database, starting from no slowquery schema."""
    eng = create_async_engine(asyncpg_url(pg))
    async with eng.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS slowquery CASCADE"))
    yield eng
    await eng.dispose()


class TestPostgresStore(StoreContract):
    @pytest.fixture()
    async def store(self, pg: PostgresContainer, admin: AsyncEngine) -> AsyncIterator[StoreWriter]:
        s = PostgresStoreWriter(asyncpg_url(pg))
        await s.start()
        yield s
        await s.close()


async def _tables(admin: AsyncEngine) -> set[str]:
    async with admin.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'slowquery'"
            )
        )
        return {r[0] for r in rows}


async def test_34_start_creates_schema_and_is_idempotent(
    pg: PostgresContainer, admin: AsyncEngine
) -> None:
    first = PostgresStoreWriter(asyncpg_url(pg))
    await first.start()
    assert await _tables(admin) == TABLES
    await first.write_batch(SERVICE, batch([(FP_A, 1.0, "R")]))
    await first.start()  # same instance again
    await first.close()

    second = PostgresStoreWriter(asyncpg_url(pg))  # a new boot
    await second.start()
    assert await _tables(admin) == TABLES
    rows = await second.list_request_queries(SERVICE, "R", since=None, limit=5)
    assert [r.call_count for r in rows] == [1]
    await second.close()

    async with admin.connect() as conn:
        versions = (await conn.execute(text("SELECT version FROM slowquery.schema_version"))).all()
        constraint = (
            await conn.execute(
                text(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conname = 'uq_suggestions_fp_kind_sql'"
                )
            )
        ).scalar_one()
    assert versions == [(1,)]
    assert constraint == "UNIQUE (fingerprint_id, kind, sql)"


async def test_35_concurrent_first_boots_all_succeed(
    pg: PostgresContainer, admin: AsyncEngine
) -> None:
    for _round in range(3):
        async with admin.begin() as conn:
            await conn.execute(text("DROP SCHEMA IF EXISTS slowquery CASCADE"))
        stores = [PostgresStoreWriter(asyncpg_url(pg)) for _ in range(4)]
        try:
            await asyncio.gather(*(s.start() for s in stores))
        finally:
            await asyncio.gather(*(s.close() for s in stores))
        assert await _tables(admin) == TABLES


async def test_39_unique_constraint_holds_against_direct_duplicates(
    pg: PostgresContainer, admin: AsyncEngine
) -> None:
    store = PostgresStoreWriter(asyncpg_url(pg))
    await store.start()
    ddl = "CREATE INDEX IF NOT EXISTS ix_a_id ON a(id);"
    await asyncio.gather(*(store.insert_suggestions(FP_A, [suggestion(ddl)]) for _ in range(5)))
    async with admin.connect() as conn:
        n = (await conn.execute(text("SELECT count(*) FROM slowquery.suggestions"))).scalar_one()
    await store.close()
    assert n == 1


async def test_store_engine_is_separate_and_closed(
    pg: PostgresContainer, admin: AsyncEngine
) -> None:
    store = PostgresStoreWriter(asyncpg_url(pg))
    await store.start()
    assert store.engine.pool.size() <= 2  # type: ignore[attr-defined]
    await store.close()
    await store.close()  # idempotent
