#!/usr/bin/env bash
# Local dev helper for slowquery-detective.
# Runs the same gates CI runs: lint → format → types → tests.
set -euo pipefail

cd "$(dirname "$0")/.."

uv sync --frozen --all-extras
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest -m "not slow and not integration"
