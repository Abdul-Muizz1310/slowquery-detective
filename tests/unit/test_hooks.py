"""Red tests for docs/specs/02-hooks.md — unit-level (no DB).

Covers the argument-validation and idempotency assertions that do not
require a real SQLAlchemy engine. Integration cases (1-12, 18, 19, 23)
live in tests/integration/test_hooks.py and run against testcontainers
Postgres.
"""

from __future__ import annotations

import pytest

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.hooks import attach, detach

# ---------------------------------------------------------------------------
# Failure cases — pure validation, no DB required
# ---------------------------------------------------------------------------


def test_13_attach_none_engine_raises() -> None:
    buf = RingBuffer()
    with pytest.raises(ValueError):
        attach(None, buf)


def test_14_attach_none_buffer_raises() -> None:
    with pytest.raises(ValueError):
        attach(object(), None)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_rate", [-0.1, 1.1, 2.0, -5.0])
def test_15_sample_rate_out_of_range(bad_rate: float) -> None:
    buf = RingBuffer()
    with pytest.raises(ValueError):
        attach(object(), buf, sample_rate=bad_rate)


def test_detach_noop_on_unattached_engine() -> None:
    """detach(engine) on an engine that was never attached is a no-op."""
    # Not an enumerated case in the spec, but invariant 8 ("Detach
    # cleanliness") says detach is safe to call without a prior attach.
    detach(object())  # must not raise (currently NotImplementedError)


