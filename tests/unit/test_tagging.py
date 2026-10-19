"""Spec 07 cases 7-10 — X-Request-Id parsing and context propagation."""

from __future__ import annotations

import asyncio
import contextvars

import httpx
import pytest
from fastapi import BackgroundTasks, Depends, FastAPI
from slowquery_detective.tagging import (
    RequestIdMiddleware,
    current_request_id,
    parse_request_id,
)

UUID = "3f2b8c1e-9a4d-4e7f-8c21-5b6a7d8e9f01"


# ---------------------------------------------------------------------------
# Case 7 / 8 — parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        UUID,
        "01JAZ3M8Y5Q2W7E4R6T9U1I0OP",  # ULID
        "a",
        "A" * 128,
        "req.1_2:3-4",
        "0",
    ],
)
def test_07_valid_request_ids_are_accepted(value: str) -> None:
    assert parse_request_id(value) == value


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "A" * 129,
        "-leading-dash",
        ".leading-dot",
        "has space",
        "a/b",
        "café",
        "line\nbreak",
        "tab\tchar",
        "semi;colon",
    ],
)
def test_08_invalid_request_ids_are_rejected(value: str | None) -> None:
    assert parse_request_id(value) is None


# ---------------------------------------------------------------------------
# Case 8 / 9 — the middleware, end to end through ASGI
# ---------------------------------------------------------------------------


def _app(seen: dict[str, str | None]) -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)

    def _dep() -> str | None:
        seen["dependency"] = current_request_id()
        return current_request_id()

    def _background() -> None:
        seen["background"] = current_request_id()

    async def _spawned() -> None:
        await asyncio.sleep(0)
        seen["task"] = current_request_id()

    @app.get("/probe")
    async def probe(background: BackgroundTasks, dep: str | None = Depends(_dep)) -> dict[str, str]:
        seen["endpoint"] = current_request_id()
        background.add_task(_background)
        await asyncio.create_task(_spawned())
        return {"ok": "yes"}

    return app


async def _get(app: FastAPI, headers: list[tuple[str, str]] | None = None) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get("/probe", headers=headers)


async def test_09_request_id_visible_in_endpoint_dependency_background_and_task() -> None:
    seen: dict[str, str | None] = {}
    resp = await _get(_app(seen), [("X-Request-Id", UUID)])
    assert resp.status_code == 200
    assert seen == {"dependency": UUID, "endpoint": UUID, "task": UUID, "background": UUID}
    # After the request the caller's context is untouched.
    assert current_request_id() is None


async def test_09_untagged_request_sees_none() -> None:
    seen: dict[str, str | None] = {}
    resp = await _get(_app(seen))
    assert resp.status_code == 200
    assert set(seen.values()) == {None}


async def test_08_invalid_header_leaves_request_untagged_and_served() -> None:
    seen: dict[str, str | None] = {}
    resp = await _get(_app(seen), [("X-Request-Id", "bad id with spaces")])
    assert resp.status_code == 200
    assert seen["endpoint"] is None


async def test_08_two_request_id_headers_are_ambiguous() -> None:
    seen: dict[str, str | None] = {}
    resp = await _get(_app(seen), [("X-Request-Id", "one"), ("X-Request-Id", "two")])
    assert resp.status_code == 200
    assert seen["endpoint"] is None


async def test_09_header_name_is_case_insensitive() -> None:
    seen: dict[str, str | None] = {}
    await _get(_app(seen), [("x-request-id", "lower")])
    assert seen["endpoint"] == "lower"


async def test_middleware_passes_non_http_scopes_through() -> None:
    calls: list[str] = []

    async def inner(scope: dict[str, object], receive: object, send: object) -> None:
        calls.append(str(scope["type"]))
        assert current_request_id() is None

    mw = RequestIdMiddleware(inner)  # type: ignore[arg-type]

    async def _receive() -> dict[str, object]:
        return {"type": "lifespan.startup"}

    async def _send(_message: dict[str, object]) -> None:
        return None

    await mw({"type": "lifespan"}, _receive, _send)
    assert calls == ["lifespan"]


# ---------------------------------------------------------------------------
# Case 10 — library tasks run in an empty context
# ---------------------------------------------------------------------------


async def test_10_fresh_context_task_does_not_inherit_request_id() -> None:
    from slowquery_detective.tagging import spawn_untagged

    seen: list[str | None] = []

    async def _work() -> None:
        seen.append(current_request_id())

    async def _inside_request() -> None:
        task = spawn_untagged(_work(), name="t")
        await task

    from slowquery_detective.tagging import _REQUEST_ID

    ctx = contextvars.copy_context()
    ctx.run(_REQUEST_ID.set, UUID)
    await asyncio.get_running_loop().create_task(_inside_request(), context=ctx)
    assert seen == [None]
