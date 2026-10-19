"""SQLAlchemy event listeners — see ``docs/specs/02-hooks.md`` and spec 07.

Attaches ``before_cursor_execute`` / ``after_cursor_execute`` / ``handle_error``
to a SQLAlchemy ``Engine`` (or ``AsyncEngine.sync_engine``), records
``(fingerprint_id, duration_ms)`` into a :class:`RingBuffer`, and hands an
:class:`Observation` to an optional callback. All hook exceptions are caught
and logged — a crash inside the hook must never poison the host request.

The start time of a statement is an attribute of its SQLAlchemy
``ExecutionContext`` (one per execution), not of the cursor: asyncpg's adapted
cursor has no ``.info`` (spec 07 case 19, upstreamed from
slowquery-demo-backend's shim 3). A statement run on behalf of a tagged
request is always recorded; ``sample_rate`` applies to untagged statements.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import Any, NamedTuple

from sqlalchemy import event

import slowquery_detective.fingerprint as _fp_mod
from slowquery_detective.buffer import RingBuffer
from slowquery_detective.tagging import current_request_id

_LOG = logging.getLogger("slowquery.hooks")

# ``ExecutionContext`` attribute holding the statement's start time.
_START_ATTR = "_slowquery_start"

# Sentinel attached to engines so idempotent attach/detach is cheap.
_ATTACHED_ATTR = "_slowquery_attached"


class Observation(NamedTuple):
    """One recorded statement, as the hook saw it.

    ``statement`` and ``parameters`` are the driver-level values; they travel
    only to the EXPLAIN worker for slow statements and are never logged or
    stored. A ``NamedTuple`` rather than a frozen dataclass: it is built once
    per statement on the hot path, and a frozen dataclass's ``__init__`` costs
    about a microsecond more.
    """

    fingerprint_id: str
    canonical_sql: str
    duration_ms: float
    statement: str
    parameters: Any
    executemany: bool
    request_id: str | None


OnRecord = Callable[[Observation], None]


def _is_explain(statement: str) -> bool:
    return statement.lstrip()[:7].upper() == "EXPLAIN"


def _take_start(context: Any) -> float | None:
    """Read and clear the start time stored on ``context``."""
    if context is None:
        return None
    start: float | None = getattr(context, _START_ATTR, None)
    if start is not None:
        try:
            setattr(context, _START_ATTR, None)
        except AttributeError:
            return None
    return start


def attach(
    engine: Any,
    buffer: RingBuffer,
    *,
    sample_rate: float = 1.0,
    on_record: OnRecord | None = None,
) -> None:
    """Attach slow-query listeners to ``engine``.

    Args:
        engine: A SQLAlchemy ``Engine`` or ``AsyncEngine``.
        buffer: The :class:`RingBuffer` to record samples into.
        sample_rate: Fraction of *untagged* statements to record (0.0..1.0).
        on_record: Optional callback receiving an :class:`Observation` after
            each successful buffer record.
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

    def _before(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        if context is None:
            return
        sampled_out = (
            sample_rate < 1.0 and current_request_id() is None and rng.random() >= sample_rate
        )
        try:
            setattr(context, _START_ATTR, None if sampled_out else time.perf_counter())
        except AttributeError:
            _LOG.debug("slowquery.hooks.context_not_writable")

    def _record(statement: str, parameters: Any, executemany: bool, start: float) -> None:
        if _is_explain(statement):
            return  # the worker's own EXPLAIN, never a user query
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
        if on_record is None:
            return
        try:
            on_record(
                Observation(
                    fingerprint_id=fp_id,
                    canonical_sql=canonical_sql,
                    duration_ms=duration_ms,
                    statement=statement,
                    parameters=parameters,
                    executemany=bool(executemany),
                    request_id=current_request_id(),
                )
            )
        except Exception:
            _LOG.debug("slowquery.hooks.on_record_failed", exc_info=True)

    def _after(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        start = _take_start(context)
        if start is not None:
            _record(statement, parameters, executemany, start)

    def _on_error(exception_context: Any) -> None:
        """Record a statement that raised; the error still reaches the caller.

        ``handle_error`` receives an ``ExceptionContext``; its
        ``execution_context`` is the same object ``_before`` stamped.
        """
        context = getattr(exception_context, "execution_context", None)
        start = _take_start(context)
        statement = getattr(exception_context, "statement", None)
        if start is None or statement is None:
            return
        _record(
            statement,
            getattr(exception_context, "parameters", None),
            bool(getattr(context, "executemany", False)),
            start,
        )

    event.listen(sync_engine, "before_cursor_execute", _before)
    event.listen(sync_engine, "after_cursor_execute", _after)
    event.listen(sync_engine, "handle_error", _on_error)

    # Stash listener references on the engine so detach can remove them.
    sync_engine._slowquery_listeners = (_before, _after, _on_error)
    sync_engine._slowquery_attached = True


def is_attached(engine: Any) -> bool:
    """True when :func:`attach` has listeners on ``engine``."""
    return engine is not None and bool(getattr(_sync_engine(engine), _ATTACHED_ATTR, False))


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
