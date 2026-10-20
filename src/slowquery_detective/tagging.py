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


class request_scope:  # noqa: N801 - used like contextlib's lower-case managers
    """Attribute the statements run inside the block to ``request_id``.

    For work that runs outside the request that caused it — a background job
    runner processing an upload later — so its queries are still charged to
    the upload's request id::

        with request_scope(upload.request_id):
            await process(upload)

    Works as ``with`` and as ``async with``. On exit, normal or by exception,
    the previous id is restored (nested scopes restore the outer id; inside a
    tagged request, the request's id). Tasks created inside the scope keep the
    id. ``request_id`` must pass the same rule as the ``X-Request-Id`` header:
    an id the middleware would ignore raises ``ValueError``, a non-``str``
    raises ``TypeError``.
    """

    __slots__ = ("_request_id", "_token")

    def __init__(self, request_id: str) -> None:
        if not isinstance(request_id, str):
            raise TypeError("request_id must be a str")
        if parse_request_id(request_id) is None:
            raise ValueError(
                "request_id must be 1-128 characters of [A-Za-z0-9._:-], "
                "starting with a letter or digit"
            )
        self._request_id = request_id
        self._token: contextvars.Token[str | None] | None = None

    def __enter__(self) -> request_scope:
        if self._token is not None:
            raise RuntimeError("this request_scope is already entered")
        self._token = _REQUEST_ID.set(self._request_id)
        return self

    def __exit__(self, *_exc: object) -> None:
        token, self._token = self._token, None
        if token is not None:
            _REQUEST_ID.reset(token)

    async def __aenter__(self) -> request_scope:
        return self.__enter__()

    async def __aexit__(self, *exc: object) -> None:
        self.__exit__(*exc)


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


def _route_path(scope: Scope) -> str:
    path: str = scope.get("path", "")
    root: str = scope.get("root_path", "")
    return path[len(root) :] if root and path.startswith(root) else path


class RequestIdMiddleware:
    """Pure ASGI middleware: tag each HTTP request with its ``X-Request-Id``.

    Requests under ``exclude_prefix`` (the library's own routes) are never
    tagged: a caller measuring request R sends ``X-Request-Id: R`` on the
    measure call too, and whatever runs during that call must not be charged
    to R.
    """

    def __init__(self, app: ASGIApp, exclude_prefix: str | None = None) -> None:
        self.app = app
        self._exclude = exclude_prefix

    def _excluded(self, scope: Scope) -> bool:
        if self._exclude is None:
            return False
        path = _route_path(scope)
        return path == self._exclude or path.startswith(self._exclude + "/")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        rid = None if self._excluded(scope) else _header_request_id(scope)
        token = _REQUEST_ID.set(rid)
        try:
            await self.app(scope, receive, send)
        finally:
            _REQUEST_ID.reset(token)
