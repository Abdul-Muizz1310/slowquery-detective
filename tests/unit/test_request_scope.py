"""Spec 07 cases 60-65 — request_scope for work outside a request (0.2.1)."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from slowquery_detective import current_request_id, request_scope
from slowquery_detective.tagging import RequestIdMiddleware


def test_60_scope_sets_and_restores_the_id() -> None:
    assert current_request_id() is None
    with request_scope("A"):
        assert current_request_id() == "A"
    assert current_request_id() is None


def test_61_nesting_restores_the_outer_id() -> None:
    with request_scope("A"):
        with request_scope("B"):
            assert current_request_id() == "B"
        assert current_request_id() == "A"
    assert current_request_id() is None


def test_62_exception_exit_propagates_and_restores() -> None:
    with request_scope("A"):
        with pytest.raises(RuntimeError, match="job failed"), request_scope("B"):
            raise RuntimeError("job failed")
        assert current_request_id() == "A"
    assert current_request_id() is None


async def test_63_task_created_inside_the_scope_keeps_the_id() -> None:
    gate = asyncio.Event()
    seen: list[str | None] = []

    async def job() -> None:
        await gate.wait()  # runs only after the scope has exited
        seen.append(current_request_id())

    with request_scope("JOB-1"):
        task = asyncio.create_task(job())
    assert current_request_id() is None
    gate.set()
    await task
    assert seen == ["JOB-1"]


async def test_64_async_with_is_supported() -> None:
    async with request_scope("A"):
        await asyncio.sleep(0)
        assert current_request_id() == "A"
    assert current_request_id() is None


async def test_64_scope_inside_a_tagged_request_restores_the_request_id() -> None:
    seen: list[str | None] = []
    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)

    @app.get("/probe")
    async def probe() -> dict[str, str]:
        seen.append(current_request_id())
        with request_scope("JOB-9"):
            seen.append(current_request_id())
        seen.append(current_request_id())
        return {"ok": "1"}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        await client.get("/probe", headers={"X-Request-Id": "REQ-1"})
    assert seen == ["REQ-1", "JOB-9", "REQ-1"]


@pytest.mark.parametrize(
    "bad",
    ["", "A" * 129, "-leading-dash", "has space", "a/b", "café", "line\nbreak", "semi;colon"],
)
def test_65_invalid_ids_raise_and_set_nothing(bad: str) -> None:
    with pytest.raises(ValueError), request_scope(bad):
        pytest.fail("the body must not run")
    assert current_request_id() is None


@pytest.mark.parametrize("bad", [None, 42, b"bytes"])
def test_65_non_strings_raise_type_error(bad: object) -> None:
    with pytest.raises(TypeError):
        request_scope(bad)  # type: ignore[arg-type]


def test_65_reentering_an_entered_scope_raises() -> None:
    scope = request_scope("A")
    with scope:
        with pytest.raises(RuntimeError), scope:
            pass
        assert current_request_id() == "A"
    assert current_request_id() is None
    with scope:  # usable again once exited
        assert current_request_id() == "A"
