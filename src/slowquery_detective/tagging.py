"""Request tagging — see ``docs/specs/07-fleet-observability.md`` §Request tagging.

An ASGI middleware copies a valid ``X-Request-Id`` header into a context
variable for the duration of the request. The SQLAlchemy hooks read it, so
every statement a request causes — in the endpoint, in a dependency, in a
Starlette ``BackgroundTasks`` function or in an ``asyncio`` task the request
spawned — is attributed to that request id.

The library's own background tasks are started with :func:`spawn_untagged`,
in an empty context, so nothing they execute is ever attributed to a request.
"""

from __future__ import annotations

import asyncio
import contextvars
import re
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:  # starlette ships with the [fastapi] extra; the hooks need only the var
    from starlette.types import ASGIApp, Receive, Scope, Send

REQUEST_ID_PATTERN: Final = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_REQUEST_ID_RE: Final = re.compile(REQUEST_ID_PATTERN)
_HEADER: Final = b"x-request-id"

_REQUEST_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "slowquery_request_id", default=None
)


def parse_request_id(value: str | None) -> str | None:
    """Return ``value`` if it is an acceptable request id, else ``None``.

    Accepted: 1-128 ASCII characters from ``[A-Za-z0-9._:-]``, starting with
    a letter or digit. Anything else is treated as "no request id" rather than
    an error, so a malformed header never breaks the host request.
    """
    if value is None:
        return None
    # ``re.match`` with ``$`` would accept a trailing newline; fullmatch does not.
    if _REQUEST_ID_RE.fullmatch(value) is None or not value.isascii():
        return None
    return value


def current_request_id() -> str | None:
    """The request id of the request this code runs on behalf of, if any."""
    return _REQUEST_ID.get()


def spawn_untagged(coro: Coroutine[Any, Any, None], *, name: str) -> asyncio.Task[None]:
    """Start ``coro`` as a task in a fresh, empty context."""
    return asyncio.get_running_loop().create_task(coro, name=name, context=contextvars.Context())


def _header_request_id(scope: Scope) -> str | None:
    values = [v for k, v in scope.get("headers", ()) if k.lower() == _HEADER]
    if len(values) != 1:
        return None
    try:
        decoded = values[0].decode("ascii")
    except UnicodeDecodeError:
        return None
    return parse_request_id(decoded)


class RequestIdMiddleware:
    """Pure ASGI middleware: tag each HTTP request with its ``X-Request-Id``."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        token = _REQUEST_ID.set(_header_request_id(scope))
        try:
            await self.app(scope, receive, send)
        finally:
            _REQUEST_ID.reset(token)
