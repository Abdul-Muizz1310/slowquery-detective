"""SQLAlchemy event listeners — see ``docs/specs/02-hooks.md``.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

from typing import Any

from slowquery_detective.buffer import RingBuffer


def attach(
    engine: Any,
    buffer: RingBuffer,
    *,
    sample_rate: float = 1.0,
) -> None:
    """Attach ``before_cursor_execute`` / ``after_cursor_execute`` listeners."""
    raise NotImplementedError("S4: implement hooks.attach per docs/specs/02-hooks.md")


def detach(engine: Any) -> None:
    """Remove listeners previously registered by :func:`attach`."""
    raise NotImplementedError("S4: implement hooks.detach per docs/specs/02-hooks.md")
