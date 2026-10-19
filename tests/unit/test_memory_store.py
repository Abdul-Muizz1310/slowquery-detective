"""Spec 07 cases 36-42 against the in-memory store, plus its own bounds."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from slowquery_detective.store import InMemoryStoreWriter, StoreWriter
from tests.store_contract import FP_A, SERVICE, T0, StoreContract, at, batch


class TestMemoryStore(StoreContract):
    @pytest.fixture()
    async def store(self) -> AsyncIterator[StoreWriter]:
        s = InMemoryStoreWriter()
        await s.start()
        yield s
        await s.close()


async def test_memory_store_bounds_samples_per_fingerprint() -> None:
    store = InMemoryStoreWriter(max_samples_per_fingerprint=3)
    await store.start()
    await store.write_batch(
        SERVICE, batch([(FP_A, 500.0 + i, None) for i in range(5)], threshold_ms=100.0)
    )
    detail = await store.get_query(SERVICE, FP_A)
    assert detail is not None
    assert [s.duration_ms for s in detail.recent_samples] == [504.0, 503.0, 502.0]


async def test_memory_store_bounds_request_rows_oldest_first() -> None:
    store = InMemoryStoreWriter(max_request_rows=2)
    await store.start()
    await store.write_batch(SERVICE, batch([(FP_A, 1.0, "R1")], start=T0))
    await store.write_batch(SERVICE, batch([(FP_A, 1.0, "R2")], start=T0 + 1))
    await store.write_batch(SERVICE, batch([(FP_A, 1.0, "R3")], start=T0 + 2))
    assert await store.list_request_queries(SERVICE, "R1", since=None, limit=50) == []
    assert len(await store.list_request_queries(SERVICE, "R3", since=None, limit=50)) == 1


def test_memory_store_rejects_invalid_bounds() -> None:
    with pytest.raises(ValueError):
        InMemoryStoreWriter(max_samples_per_fingerprint=0)
    with pytest.raises(ValueError):
        InMemoryStoreWriter(max_request_rows=0)


def test_store_writer_is_abstract() -> None:
    with pytest.raises(TypeError):
        StoreWriter()  # type: ignore[abstract]

    class Partial(StoreWriter):
        async def start(self) -> None:
            return None

    with pytest.raises(TypeError):
        Partial()  # type: ignore[abstract]


async def test_memory_store_sweep_uses_strict_cutoff() -> None:
    store = InMemoryStoreWriter()
    await store.start()
    await store.write_batch(SERVICE, batch([(FP_A, 500.0, "R")], threshold_ms=100.0))
    await store.sweep(SERVICE, at(T0))
    detail = await store.get_query(SERVICE, FP_A)
    assert detail is not None
    assert len(detail.recent_samples) == 1


async def test_memory_store_bounds_fingerprints_by_least_recently_seen() -> None:
    from tests.store_contract import FP_B, FP_C

    store = InMemoryStoreWriter(max_fingerprints=2)
    await store.start()
    await store.write_batch(SERVICE, batch([(FP_A, 500.0, "R")], start=T0, threshold_ms=100.0))
    await store.write_batch(SERVICE, batch([(FP_B, 1.0, None)], start=T0 + 1))
    await store.write_batch(SERVICE, batch([(FP_C, 1.0, None)], start=T0 + 2))
    ids = {r.id for r in await store.list_queries(SERVICE, since=None, limit=50)}
    assert ids == {FP_B, FP_C}
    # Everything hanging off the evicted fingerprint went with it.
    assert await store.get_query(SERVICE, FP_A) is None
    assert await store.list_request_queries(SERVICE, "R", since=None, limit=50) == []
