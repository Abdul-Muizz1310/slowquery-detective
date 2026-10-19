"""The read routes, mounted at ``mount_path`` — spec 07 §Routes, cases 23-26.

Every route depends on the host's ``authorize`` hook. No hook means 403: the
library never decides on its own that a caller may read a service's query
log. The 0.1 static shared secret (``SLOWQUERY_PLATFORM_TOKEN`` /
``X-Platform-Token``) is gone; a host that wants a token check puts it in the
hook. Authorization is a router-level dependency, so FastAPI runs it before
it validates query parameters and an unauthorized caller gets 403, never 422.
"""

from __future__ import annotations

import inspect
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Annotated, Final

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import AwareDatetime

from slowquery_detective.runtime import Runtime
from slowquery_detective.schemas import QueryDetail, QueryRow
from slowquery_detective.tagging import REQUEST_ID_PATTERN

_LOG = logging.getLogger("slowquery.router")

Authorize = Callable[[Request], Awaitable[bool]]

_FINGERPRINT_ID: Final = re.compile(r"[0-9a-f]{16}")


def build_router(runtime: Runtime, authorize: Authorize | None) -> APIRouter:
    """The two read routes for one installed runtime."""

    async def _authorized(request: Request) -> None:
        if authorize is None:
            raise HTTPException(status_code=403, detail="forbidden")
        try:
            verdict: object = authorize(request)
            if inspect.isawaitable(verdict):
                verdict = await verdict
        except HTTPException:
            raise
        except Exception as exc:
            # Class only: a verifier's message may quote the credential.
            _LOG.warning("slowquery.router.authorize_failed", extra={"error": type(exc).__name__})
            raise HTTPException(status_code=403, detail="forbidden") from None
        if verdict is not True:
            raise HTTPException(status_code=403, detail="forbidden")

    async def _ready_store() -> None:
        await runtime.flush()
        if not await runtime.ensure_ready():
            raise HTTPException(status_code=503, detail="slowquery store unavailable")

    router = APIRouter(dependencies=[Depends(_authorized)], include_in_schema=False)

    @router.get("/queries", response_model=list[QueryRow])
    async def list_queries(
        request_id: Annotated[str | None, Query(pattern=REQUEST_ID_PATTERN)] = None,
        since: Annotated[AwareDatetime | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> list[QueryRow]:
        await _ready_store()
        if request_id is None:
            return await runtime.store.list_queries(runtime.service, since=since, limit=limit)
        return await runtime.store.list_request_queries(
            runtime.service, request_id, since=since, limit=limit
        )

    @router.get("/queries/{fingerprint_id}", response_model=QueryDetail)
    async def query_detail(fingerprint_id: str) -> QueryDetail:
        if _FINGERPRINT_ID.fullmatch(fingerprint_id) is None:
            raise HTTPException(status_code=404, detail="not found")
        await _ready_store()
        detail = await runtime.store.get_query(runtime.service, fingerprint_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="not found")
        return detail

    return router
