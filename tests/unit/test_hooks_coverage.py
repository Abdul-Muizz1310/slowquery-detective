"""Additional unit tests for hooks.py — coverage of attach/detach logic.

These tests use mock engines and the SQLAlchemy event system to exercise
the hook registration and detach paths without requiring a real Postgres.
The before/after cursor hooks need cursor.info (Postgres-specific), so
they are tested at the integration level. We focus on:
- _sync_engine helper
- _SuppressSetattrErrors context manager
- attach validation beyond what's already tested
- detach paths including listener removal
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event

from slowquery_detective.buffer import RingBuffer
from slowquery_detective.hooks import _SuppressSetattrErrors, _sync_engine, attach, detach


# ---------------------------------------------------------------------------
# _sync_engine helper
# ---------------------------------------------------------------------------


def test_sync_engine_returns_sync_engine_attr() -> None:
    """When engine has .sync_engine, it should be returned."""
    inner = object()
    engine = MagicMock()
    engine.sync_engine = inner
    assert _sync_engine(engine) is inner


def test_sync_engine_returns_self_when_no_attr() -> None:
    """When engine has no .sync_engine, return the engine itself."""

    class _PlainEngine:
        pass

    engine = _PlainEngine()
    assert _sync_engine(engine) is engine


# ---------------------------------------------------------------------------
# _SuppressSetattrErrors
# ---------------------------------------------------------------------------


def test_suppress_setattr_errors_swallows_attribute_error() -> None:
    with _SuppressSetattrErrors():
        raise AttributeError("test")
    # Should not propagate


def test_suppress_setattr_errors_propagates_other_errors() -> None:
    with pytest.raises(RuntimeError):
        with _SuppressSetattrErrors():
            raise RuntimeError("not an attribute error")


def test_suppress_setattr_enter_returns_none() -> None:
    ctx = _SuppressSetattrErrors()
    result = ctx.__enter__()
    assert result is None


# ---------------------------------------------------------------------------
# attach/detach with mock engines that support SQLAlchemy event system
# ---------------------------------------------------------------------------


class _MockSyncEngine:
    """A minimal mock that supports SQLAlchemy event.listen/remove."""

    dispatch = MagicMock()

    def __init__(self) -> None:
        # Reset per instance to avoid cross-test contamination
        self._slowquery_attached = False
        self._slowquery_listeners = None


def test_attach_sets_attached_flag() -> None:
    """attach() should mark the engine as attached."""
    engine = _MockSyncEngine()
    buf = RingBuffer()

    # We need to patch event.listen since _MockSyncEngine isn't a real engine
    from unittest.mock import patch

    with patch.object(event, "listen"):
        attach(engine, buf)

    assert engine._slowquery_attached is True
    assert engine._slowquery_listeners is not None


def test_attach_idempotent_warns(caplog: Any) -> None:
    """Second attach on the same engine warns and doesn't double-register."""
    engine = _MockSyncEngine()
    buf = RingBuffer()

    from unittest.mock import patch

    with patch.object(event, "listen"):
        attach(engine, buf)
        attach(engine, buf)  # should warn

    assert any("already_attached" in r.message for r in caplog.records)


def test_detach_clears_attached_flag() -> None:
    """detach() should clear the attached flag and listeners."""
    engine = _MockSyncEngine()
    buf = RingBuffer()

    from unittest.mock import patch

    with patch.object(event, "listen"):
        attach(engine, buf)

    with patch.object(event, "remove"):
        detach(engine)

    assert engine._slowquery_attached is False
    assert engine._slowquery_listeners is None


def test_detach_on_none_is_noop() -> None:
    """detach(None) should not raise."""
    detach(None)


def test_detach_on_unattached_engine_is_noop() -> None:
    """detach on engine that was never attached is safe."""
    engine = _MockSyncEngine()
    detach(engine)  # no exception


def test_detach_handles_remove_failure_gracefully() -> None:
    """If event.remove raises, detach should not crash."""
    engine = _MockSyncEngine()
    buf = RingBuffer()

    from unittest.mock import patch

    with patch.object(event, "listen"):
        attach(engine, buf)

    with patch.object(event, "remove", side_effect=RuntimeError("boom")):
        detach(engine)  # should not raise

    # Flag should still be cleared
    assert engine._slowquery_attached is False


def test_detach_on_frozen_object_does_not_crash() -> None:
    """If the engine's attributes can't be set (frozen), detach handles it."""

    class _FrozenEngine:
        __slots__ = ()

        @property
        def _slowquery_attached(self) -> bool:
            return True

        @property
        def _slowquery_listeners(self) -> tuple[Any, ...]:
            return (lambda: None, lambda: None, lambda: None)

    engine = _FrozenEngine()
    from unittest.mock import patch

    with patch.object(event, "remove"):
        # This exercises the _SuppressSetattrErrors path
        detach(engine)


def test_attach_with_async_engine_uses_sync_engine() -> None:
    """AsyncEngine wraps a sync_engine; attach should use it."""
    sync_inner = _MockSyncEngine()
    mock_async = MagicMock()
    mock_async.sync_engine = sync_inner

    buf = RingBuffer()
    from unittest.mock import patch

    with patch.object(event, "listen"):
        attach(mock_async, buf)

    assert sync_inner._slowquery_attached is True
