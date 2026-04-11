"""Bootstrap smoke test.

Proves the package imports cleanly and exposes ``__version__``. Replaced /
removed once the real Spec-TDD test files land in S2.
"""

from __future__ import annotations

import slowquery_detective


def test_package_imports() -> None:
    assert slowquery_detective.__version__ == "0.1.0"
