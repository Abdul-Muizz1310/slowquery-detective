"""Integration test conftest — opt-in only.

Integration tests depend on Testcontainers Postgres (Docker). They are
**skipped by default** and only run when explicitly requested:

    uv run pytest -m integration          # run only integration tests

This prevents accidental 20+ minute Docker waits during normal development.
CI uses ``pytest -m "not slow and not integration"`` so these never run there.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip integration tests unless explicitly selected via -m integration."""
    if not items:
        return

    # If the user ran `pytest -m integration` or `-m "integration and ..."`, let them through.
    markexpr = str(config.getoption("-m", default=""))
    if "integration" in markexpr:
        if not _docker_is_available():
            skip_marker = pytest.mark.skip(reason="Docker daemon not available")
            for item in items:
                if "integration" in str(item.fspath):
                    item.add_marker(skip_marker)
        return

    # Default: skip all integration tests.
    skip_marker = pytest.mark.skip(
        reason="integration tests skipped by default (use: pytest -m integration)"
    )
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(skip_marker)


def _docker_is_available() -> bool:
    """Return True if Docker is reachable, False otherwise."""
    try:
        import docker

        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Shared session-scoped Postgres container (boots once for ALL integration tests)
# ---------------------------------------------------------------------------

from collections.abc import Iterator

from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg() -> Iterator[PostgresContainer]:
    """Single Postgres container shared across all integration test files."""
    with PostgresContainer("postgres:16-alpine") as container:
        yield container
