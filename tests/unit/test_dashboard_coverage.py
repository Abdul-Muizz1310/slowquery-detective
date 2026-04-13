"""Unit tests for dashboard.py — the DDL allowlist and router construction."""

from __future__ import annotations

from fastapi import APIRouter

from slowquery_detective.dashboard import DDL_ALLOWLIST_REGEX, dashboard_router


def test_ddl_allowlist_accepts_valid_create_index() -> None:
    assert DDL_ALLOWLIST_REGEX.match(
        "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);"
    )


def test_ddl_allowlist_accepts_concurrently() -> None:
    assert DDL_ALLOWLIST_REGEX.match(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_orders_user_id ON orders(user_id);"
    )


def test_ddl_allowlist_rejects_drop_table() -> None:
    assert not DDL_ALLOWLIST_REGEX.match("DROP TABLE orders;")


def test_ddl_allowlist_rejects_missing_if_not_exists() -> None:
    assert not DDL_ALLOWLIST_REGEX.match("CREATE INDEX ix_x ON y(z);")


def test_dashboard_router_is_api_router() -> None:
    assert isinstance(dashboard_router, APIRouter)
