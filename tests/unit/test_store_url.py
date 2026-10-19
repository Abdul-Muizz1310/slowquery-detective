"""Spec 07 case 43 — store URL validation and normalisation."""

from __future__ import annotations

import pytest

from slowquery_detective.store_url import normalise_store_url


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgres://u:p@h/db", "postgresql+asyncpg://u:p@h/db"),
        ("postgresql://u:p@h:5432/db", "postgresql+asyncpg://u:p@h:5432/db"),
        ("postgresql+asyncpg://u:p@h/db", "postgresql+asyncpg://u:p@h/db"),
        (
            "postgresql://u:p@ep-x-pooler.neon.tech/db?sslmode=require&channel_binding=require",
            "postgresql+asyncpg://u:p@ep-x-pooler.neon.tech/db?ssl=require",
        ),
        (
            "postgresql+asyncpg://u:p@h/db?ssl=require&sslmode=verify-full",
            "postgresql+asyncpg://u:p@h/db?ssl=require",
        ),
        (
            "postgresql+psycopg://u:p@h/db?sslmode=require&channel_binding=require",
            "postgresql+psycopg://u:p@h/db?sslmode=require&channel_binding=require",
        ),
        ("POSTGRESQL://u:p@h/db", "postgresql+asyncpg://u:p@h/db"),
        ("postgresql:///db?host=/run/postgresql", "postgresql+asyncpg:///db?host=/run/postgresql"),
    ],
)
def test_43_normalises_postgres_urls(url: str, expected: str) -> None:
    assert normalise_store_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "",
        "   ",
        "mysql://u:p@h/db",
        "sqlite+aiosqlite:///x.db",
        "postgresql+psycopg2://u:p@h/db",
        "postgresql+pg8000://u:p@h/db",
        "not a url",
        "postgresql://",
    ],
)
def test_43_rejects_non_async_postgres_urls(url: str) -> None:
    with pytest.raises(ValueError):
        normalise_store_url(url)


def test_43_error_never_echoes_the_password() -> None:
    with pytest.raises(ValueError) as info:
        normalise_store_url("mysql://user:hunter2@h/db")
    assert "hunter2" not in str(info.value)
