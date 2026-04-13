"""Integration test conftest — opt-in only.

Integration tests depend on Testcontainers Postgres (Docker). They are
**skipped by default** and only run when explicitly requested:

    uv run pytest -m integration          # run only integration tests
    uv run pytest --run-integration       # run everything including integration

This prevents accidental 20+ minute Docker waits during normal development.
CI uses ``pytest -m "not slow and not integration"`` so these never run there.
"""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires Docker)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip integration tests unless explicitly requested."""
    if not items:
        return

    # If the user explicitly asked for integration tests, let them run.
    if config.getoption("--run-integration", default=False):
        # Still check Docker is available.
        if not _docker_is_available():
            skip_marker = pytest.mark.skip(reason="Docker daemon not available")
            for item in items:
                if "integration" in str(item.fspath):
                    item.add_marker(skip_marker)
        return

    # If the user ran `pytest -m integration`, let them through.
    markexpr = config.getoption("-m", default="")
    if markexpr and "integration" in str(markexpr):
        if not _docker_is_available():
            skip_marker = pytest.mark.skip(reason="Docker daemon not available")
            for item in items:
                if "integration" in str(item.fspath):
                    item.add_marker(skip_marker)
        return

    # Default: skip all integration tests silently.
    skip_marker = pytest.mark.skip(reason="integration tests skipped by default (use --run-integration)")
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
