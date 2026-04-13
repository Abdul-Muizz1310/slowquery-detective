"""Integration test conftest — skip the entire directory when Docker is unavailable.

All integration tests in this directory depend on Testcontainers Postgres,
which requires a running Docker daemon. When Docker Desktop is not running
(the normal state on developer machines without it), every test in this
directory errors at fixture setup. This conftest intercepts collection and
gracefully skips the whole directory instead of producing 52 errors.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Auto-skip integration tests when Docker is unreachable."""
    if not items:
        return

    # Only check Docker once per collection, not per test.
    docker_available = _docker_is_available()
    if docker_available:
        return

    skip_marker = pytest.mark.skip(
        reason="Docker daemon not available (integration tests require Testcontainers)"
    )
    for item in items:
        # Only touch items that live under the integration directory.
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
