"""SQLAlchemy event listeners — see ``docs/specs/02-hooks.md``.

Attaches ``before_cursor_execute`` / ``after_cursor_execute`` to a
SQLAlchemy ``Engine`` (or ``AsyncEngine.sync_engine``) and records
``(fingerprint_id, duration_ms)`` pairs into a :class:`RingBuffer`. All
hook exceptions are caught and logged — a crash inside the hook must
never poison the host request.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

from sqlalchemy import event

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.fingerprint import fingerprint as fingerprint_fn

_LOG = logging.getLogger("slowquery.hooks")

# ``cursor.info`` key used to stash per-statement start time.
_START_KEY = "_slowquery_start"

# Sentinel attached to engines so idempotent attach/detach is cheap.
_ATTACHED_ATTR = "_slowquery_attached"


def attach(
    engine: Any,
    buffer: RingBuffer,
    *,
    sample_rate: float = 1.0,
) -> None:
    """Attach slow-query listeners to ``engine``.

    Args:
        engine: A SQLAlchemy ``Engine`` or ``AsyncEngine``.
        buffer: The :class:`RingBuffer` to record samples into.
        sample_rate: Fraction of statements to fingerprint (0.0..1.0).
    """
    if engine is None:
        raise ValueError("engine must not be None")
    if buffer is None:
        raise ValueError("buffer must not be None")
    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be in [0.0, 1.0]")

    sync_engine = _sync_engine(engine)

    if getattr(sync_engine, _ATTACHED_ATTR, False):
        _LOG.warning("slowquery.hooks.already_attached engine=%r", id(sync_engine))
        return

    rng = random.Random(id(sync_engine))

    def _before(conn: Any, cursor: Any, statement: str, *_rest: Any) -> None:
        # Sampling first — cheapest filter.
        if sample_rate < 1.0 and rng.random() >= sample_rate:
            cursor.info[_START_KEY] = None
            return
        cursor.info[_START_KEY] = time.perf_counter()

    def _after(conn: Any, cursor: Any, statement: str, *_rest: Any) -> None:
        start = cursor.info.pop(_START_KEY, None)
        if start is None:
            return
        duration_ms = (time.perf_counter() - start) * 1000.0
        try:
            fp_id, _ = fingerprint_fn(statement)
        except Exception:
            _LOG.debug("slowquery.hooks.fingerprint_skipped", exc_info=True)
            return
        try:
            buffer.record(fp_id, duration_ms)
        except Exception:
            _LOG.error("slowquery.hooks.record_failed", exc_info=True)

    event.listen(sync_engine, "before_cursor_execute", _before)
    event.listen(sync_engine, "after_cursor_execute", _after)

    # Stash listener references on the engine so detach can remove them.
    sync_engine._slowquery_listeners = (_before, _after)
    sync_engine._slowquery_attached = True


def detach(engine: Any) -> None:
    """Remove listeners previously registered by :func:`attach`.

    Idempotent and safe to call on an engine that was never attached.
    """
    if engine is None:
        return
    sync_engine = _sync_engine(engine)
    if not getattr(sync_engine, _ATTACHED_ATTR, False):
        return

    listeners = getattr(sync_engine, "_slowquery_listeners", None)
    if listeners is not None:
        before, after = listeners
        try:
            event.remove(sync_engine, "before_cursor_execute", before)
        except Exception:
            _LOG.debug("slowquery.hooks.remove_before_failed", exc_info=True)
        try:
            event.remove(sync_engine, "after_cursor_execute", after)
        except Exception:
            _LOG.debug("slowquery.hooks.remove_after_failed", exc_info=True)

    with _SuppressSetattrErrors():
        sync_engine._slowquery_listeners = None
        sync_engine._slowquery_attached = False


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _sync_engine(engine: Any) -> Any:
    """Return the sync engine whether ``engine`` is async or sync.

    SQLAlchemy's ``AsyncEngine`` exposes its underlying sync engine via the
    ``sync_engine`` attribute; event listeners always attach to the sync
    engine even in async mode. Typed as ``Any`` because we intentionally
    accept arbitrary duck-typed engines in the test suite.
    """
    return getattr(engine, "sync_engine", engine)


class _SuppressSetattrErrors:
    """Swallow ``AttributeError`` so detach works on plain objects."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return exc_type is AttributeError
