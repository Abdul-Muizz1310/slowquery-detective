#!/usr/bin/env bash
# Propagate secrets from the workspace-level .env into GitHub Actions secrets
# for the slowquery-detective repo. Re-run whenever a secret rotates.
#
# slowquery-detective is a PyPI library, so the only secrets it needs in CI
# are for the release job (TestPyPI + PyPI tokens).
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "$0")/../../.env"

REPO=Abdul-Muizz1310/slowquery-detective

gh secret set TEST_PYPI_API_TOKEN --repo "$REPO" --body "${TEST_PYPI_API_TOKEN:-}"
gh secret set PYPI_API_TOKEN --repo "$REPO" --body "${PYPI_API_TOKEN:-}"
