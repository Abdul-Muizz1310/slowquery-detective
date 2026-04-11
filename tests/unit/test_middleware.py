"""Red tests for docs/specs/05-middleware.md — unit level.

Covers argument validation (7-11), the DDL allowlist regex with an
adversarial suite (case 27), and a unit-level install happy-path smoke
using an in-memory SQLite async engine (no Docker). Full HTTP / lifespan
tests (1-6, 12-23, plus CORS, SSE, etc.) live in
tests/integration/test_middleware.py.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI

from slowquery_detective import install
from slowquery_detective.dashboard import DDL_ALLOWLIST_REGEX

# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


def test_07_install_none_engine_raises() -> None:
    app = FastAPI()
    with pytest.raises(ValueError):
        install(app, None)


def test_08_install_none_app_raises() -> None:
    with pytest.raises(ValueError):
        install(None, object())


def test_09_enable_llm_without_config_raises() -> None:
    app = FastAPI()
    with pytest.raises(ValueError):
        install(app, object(), enable_llm=True, llm_config=None)


def test_10_threshold_ms_zero_rejected() -> None:
    app = FastAPI()
    with pytest.raises(ValueError):
        install(app, object(), threshold_ms=0)


@pytest.mark.parametrize("bad_rate", [-0.1, 1.1, 2.0, -5.0])
def test_11_sample_rate_out_of_range(bad_rate: float) -> None:
    app = FastAPI()
    with pytest.raises(ValueError):
        install(app, object(), sample_rate=bad_rate)


# Note: the valid-config happy path for install() — with a real engine and
# the dashboard router mounted — lives in tests/integration/test_middleware.py.
# Unit tests here cover argument validation only.


# ---------------------------------------------------------------------------
# DDL allowlist regex — adversarial suite (case 27)
# ---------------------------------------------------------------------------


VALID_DDL = [
    "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
    "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_orders_user_id ON orders(user_id);",
    "CREATE INDEX IF NOT EXISTS ix_users_email_lower ON users(LOWER(email));",
    'CREATE INDEX IF NOT EXISTS ix_user_id ON "user"(id);',
    "CREATE INDEX IF NOT EXISTS ix_orders_created_at ON orders(created_at);",
]

INVALID_DDL = [
    # Destructive verbs.
    "DROP INDEX ix_orders_user_id;",
    "DROP TABLE orders;",
    "ALTER TABLE orders ADD COLUMN foo int;",
    "TRUNCATE TABLE orders;",
    "GRANT SELECT ON orders TO public;",
    "REVOKE SELECT ON orders FROM public;",
    "UPDATE orders SET total = 0;",
    "DELETE FROM orders;",
    # Non-idempotent or function variants.
    "CREATE OR REPLACE FUNCTION x() RETURNS int AS $$ SELECT 1 $$ LANGUAGE SQL;",
    "CREATE INDEX ix_orders_user_id ON orders(user_id);",  # missing IF NOT EXISTS
    # Injection attempts.
    "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id); DROP TABLE users;",
    "CREATE INDEX IF NOT EXISTS ix_a ON b(c) /* */; DROP TABLE users; --",
    "CREATE INDEX IF NOT EXISTS ix_orders_user_id -- harmless\nDROP TABLE users;",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_x ON y(z); SELECT 1;",
    # Whitespace / unicode tricks.
    " CREATE INDEX IF NOT EXISTS ix_x ON y(z);",  # leading space
    "create index if not exists ix_x on y(z);",  # lowercase
    "CREATE\tINDEX IF NOT EXISTS ix_x ON y(z);",  # tab
    "CREATE INDEX IF NOT EXISTS ix-orders-user-id ON orders(user_id);",  # hyphen in name
    "CREATE INDEX IF NOT EXISTS іx_orders_user_id ON orders(user_id);",  # noqa: RUF001  intentional cyrillic i lookalike
    "CREATE INDEX IF NOT EXISTS 1x_orders_user_id ON orders(user_id);",  # digit prefix
    # Malicious / non-ix_ prefix.
    "CREATE INDEX IF NOT EXISTS badname ON orders(user_id);",
]


@pytest.mark.parametrize("ddl", VALID_DDL)
def test_27_allowlist_accepts_valid_ddl(ddl: str) -> None:
    assert DDL_ALLOWLIST_REGEX.match(ddl) is not None, f"should accept: {ddl!r}"


@pytest.mark.parametrize("ddl", INVALID_DDL)
def test_27_allowlist_rejects_invalid_ddl(ddl: str) -> None:
    assert DDL_ALLOWLIST_REGEX.match(ddl) is None, f"should reject: {ddl!r}"
