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

import slowquery_detective.fingerprint as _fp_mod
from slowquery_detective.buffer import RingBuffer

_LOG = logging.getLogger("slowquery.hooks")

# ``cursor.info`` key used to stash per-statement start time.
_START_KEY = "_slowquery_start"

# Sentinel attached to engines so idempotent attach/detach is cheap.
_ATTACHED_ATTR = "_slowquery_attached"

# Fallback storage for cursor adapters that lack ``.info`` (e.g. asyncpg).
# Keyed by ``id(cursor)``; cleaned up in ``_after``.
_CURSOR_STORE: dict[int, float | None] = {}


OnRecordCallback = Any  # Callable[[str, str, float], None] — kept as Any to avoid circular imports


def attach(
    engine: Any,
    buffer: RingBuffer,
    *,
    sample_rate: float = 1.0,
    on_record: OnRecordCallback | None = None,
) -> None:
    """Attach slow-query listeners to ``engine``.

    Args:
        engine: A SQLAlchemy ``Engine`` or ``AsyncEngine``.
        buffer: The :class:`RingBuffer` to record samples into.
        sample_rate: Fraction of statements to fingerprint (0.0..1.0).
        on_record: Optional callback ``(fp_id, canonical_sql, duration_ms)``
            invoked after each successful buffer record. Used by the middleware
            to submit explain jobs.
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

    def _set_start(cursor: Any, value: float | None) -> None:
        """Store start time on cursor.info (psycopg2) or fallback dict (asyncpg)."""
        try:
            cursor.info[_START_KEY] = value
        except AttributeError:
            _CURSOR_STORE[id(cursor)] = value

    def _pop_start(cursor: Any) -> float | None:
        """Retrieve and remove start time from cursor.info or fallback dict."""
        try:
            val: float | None = cursor.info.pop(_START_KEY, None)
            return val
        except AttributeError:
            return _CURSOR_STORE.pop(id(cursor), None)

    def _before(conn: Any, cursor: Any, statement: str, *_rest: Any) -> None:
        # Sampling first — cheapest filter.
        if sample_rate < 1.0 and rng.random() >= sample_rate:
            _set_start(cursor, None)
            return
        _set_start(cursor, time.perf_counter())

    def _after(conn: Any, cursor: Any, statement: str, *_rest: Any) -> None:
        start = _pop_start(cursor)
        if start is None:
            return
        # Skip EXPLAIN queries from the worker to avoid self-referential recording.
        stripped = statement.lstrip()
        if stripped.upper().startswith("EXPLAIN"):
            return
        duration_ms = (time.perf_counter() - start) * 1000.0
        try:
            fp_id, canonical_sql = _fp_mod.fingerprint(statement)
        except Exception:
            _LOG.debug("slowquery.hooks.fingerprint_skipped", exc_info=True)
            return
        try:
            buffer.record(fp_id, duration_ms)
        except Exception:
            _LOG.error("slowquery.hooks.record_failed", exc_info=True)
            return
        if on_record is not None:
            try:
                on_record(fp_id, canonical_sql, duration_ms)
            except Exception:
                _LOG.debug("slowquery.hooks.on_record_failed", exc_info=True)

    def _on_error(exception_context: Any) -> None:
        """Fire fingerprinting even when the query raises an error.

        ``handle_error`` is a :class:`DialectEvents` event; its single
        argument is an ``ExceptionContext`` carrying the statement and
        connection that failed. The cursor is accessed via
        ``execution_context.cursor`` (the top-level ``cursor`` attribute
        is not materialised on all SQLAlchemy versions).
        """
        try:
            ec = exception_context.execution_context
            cursor = ec.cursor if ec is not None else None
        except AttributeError:
            cursor = None
        statement = getattr(exception_context, "statement", None)
        if cursor is not None and statement is not None:
            conn = getattr(exception_context, "connection", None)
            _after(conn, cursor, statement)
        elif cursor is not None:
            # Clean up _CURSOR_STORE even when statement is None
            # to prevent memory leaks on connection-level errors
            _pop_start(cursor)

    event.listen(sync_engine, "before_cursor_execute", _before)
    event.listen(sync_engine, "after_cursor_execute", _after)
    event.listen(sync_engine, "handle_error", _on_error)

    # Stash listener references on the engine so detach can remove them.
    sync_engine._slowquery_listeners = (_before, _after, _on_error)
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
        before, after, on_error = listeners
        try:
            event.remove(sync_engine, "before_cursor_execute", before)
        except Exception:
            _LOG.debug("slowquery.hooks.remove_before_failed", exc_info=True)
        try:
            event.remove(sync_engine, "after_cursor_execute", after)
        except Exception:
            _LOG.debug("slowquery.hooks.remove_after_failed", exc_info=True)
        try:
            event.remove(sync_engine, "handle_error", on_error)
        except Exception:
            _LOG.debug("slowquery.hooks.remove_error_failed", exc_info=True)

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
