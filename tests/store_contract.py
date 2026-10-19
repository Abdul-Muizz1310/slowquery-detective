"""Spec 07 cases 36-42 — the behaviour every StoreWriter must have.

Not collected on its own (no ``Test`` prefix). ``tests/unit/test_memory_store.py``
runs it against :class:`InMemoryStoreWriter`; ``tests/integration/test_pg_store.py``
runs it against :class:`PostgresStoreWriter` on a real Postgres.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from slowquery_detective.recorder import Batch, Recorder
from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

SERVICE = "paper-trail"
OTHER = "magpie"
FP_A = "a1a1a1a1a1a1a1a1"
FP_B = "b2b2b2b2b2b2b2b2"
FP_C = "c3c3c3c3c3c3c3c3"
SQL = {FP_A: "select * from a where id = ?", FP_B: "select * from b", FP_C: "select 1"}
T0 = 1_790_000_000.0  # 2026-09-21T...Z, whole seconds so timestamps round-trip exactly


def at(t: float) -> datetime:
    return datetime.fromtimestamp(t, UTC)


class _Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def batch(
    observations: list[tuple[str, float, str | None]],
    *,
    start: float = T0,
    step: float = 1.0,
    threshold_ms: float = 1_000.0,
) -> Batch:
    """A batch from ``(fingerprint_id, duration_ms, request_id)`` observations."""
    clock = _Clock(start)
    rec = Recorder(threshold_ms=threshold_ms, clock=clock)
    for i, (fp, ms, rid) in enumerate(observations):
        if i:
            clock.t += step
        rec.observe(fp, SQL[fp], ms, rid)
    return rec.drain()


def with_percentiles(b: Batch, fp: str, p50: float, p95: float, p99: float) -> Batch:
    fps = dict(b.fingerprints)
    fps[fp] = replace(fps[fp], p50_ms=p50, p95_ms=p95, p99_ms=p99)
    return replace(b, fingerprints=fps)


def suggestion(sql: str | None, kind: str = "index", source: str = "rules") -> Suggestion:
    return Suggestion(
        kind=kind,  # type: ignore[arg-type]
        sql=sql,
        rationale="seq scan on a",
        confidence=0.9,
        source=source,  # type: ignore[arg-type]
        rule_name="seq_scan_large_table" if source == "rules" else None,
    )


class StoreContract:
    """Subclasses provide an async ``store`` fixture yielding a started store."""

    # -- 36 ------------------------------------------------------------------

    async def test_36_batch_round_trips_every_field(self, store: StoreWriter) -> None:
        b = batch([(FP_A, 10.0, None), (FP_A, 30.0, None)])
        await store.write_batch(SERVICE, with_percentiles(b, FP_A, 11.0, 28.0, 29.6))
        rows = await store.list_queries(SERVICE, since=None, limit=50)
        assert len(rows) == 1
        row = rows[0]
        assert row.id == FP_A
        assert row.fingerprint == SQL[FP_A]
        assert row.first_seen == at(T0)
        assert row.last_seen == at(T0 + 1)
        assert row.call_count == 2
        assert row.total_ms == 40.0
        assert row.max_ms == 30.0
        assert (row.p50_ms, row.p95_ms, row.p99_ms) == (11.0, 28.0, 29.6)
        assert row.suggestions == []

    async def test_36_second_batch_adds_counts_and_keeps_extremes(self, store: StoreWriter) -> None:
        await store.write_batch(SERVICE, batch([(FP_A, 50.0, None)], start=T0 + 100))
        await store.write_batch(SERVICE, batch([(FP_A, 5.0, None), (FP_A, 5.0, None)], start=T0))
        row = (await store.list_queries(SERVICE, since=None, limit=50))[0]
        assert row.call_count == 3
        assert row.total_ms == 60.0
        assert row.max_ms == 50.0
        assert row.first_seen == at(T0)
        assert row.last_seen == at(T0 + 100)
        # No percentiles in either batch -> still null.
        assert row.p50_ms is None

    async def test_36_percentiles_kept_when_a_later_batch_has_none(
        self, store: StoreWriter
    ) -> None:
        await store.write_batch(
            SERVICE, with_percentiles(batch([(FP_A, 10.0, None)]), FP_A, 1.0, 2.0, 3.0)
        )
        await store.write_batch(SERVICE, batch([(FP_A, 10.0, None)], start=T0 + 5))
        row = (await store.list_queries(SERVICE, since=None, limit=50))[0]
        assert (row.p50_ms, row.p95_ms, row.p99_ms) == (1.0, 2.0, 3.0)

    # -- 37 ------------------------------------------------------------------

    async def test_37_request_queries_are_scoped_to_request_and_service(
        self, store: StoreWriter
    ) -> None:
        await store.write_batch(
            SERVICE,
            batch(
                [
                    (FP_A, 2.0, "R1"),
                    (FP_A, 6.0, "R1"),
                    (FP_B, 1.0, "R1"),
                    (FP_A, 100.0, "R2"),
                    (FP_C, 3.0, None),
                ]
            ),
        )
        await store.write_batch(OTHER, batch([(FP_C, 9.0, "R1")]))
        rows = await store.list_request_queries(SERVICE, "R1", since=None, limit=50)
        assert [r.id for r in rows] == [FP_A, FP_B]
        a = rows[0]
        assert (a.call_count, a.total_ms, a.max_ms) == (2, 8.0, 6.0)
        assert (a.p50_ms, a.p95_ms, a.p99_ms) == (None, None, None)
        assert a.first_seen == at(T0)
        assert a.last_seen == at(T0 + 1)
        assert a.fingerprint == SQL[FP_A]
        assert await store.list_request_queries(SERVICE, "nope", since=None, limit=50) == []
        other = await store.list_request_queries(OTHER, "R1", since=None, limit=50)
        assert [(r.id, r.call_count) for r in other] == [(FP_C, 1)]

    async def test_37_request_rows_accumulate_across_batches(self, store: StoreWriter) -> None:
        await store.write_batch(SERVICE, batch([(FP_A, 2.0, "R1")]))
        await store.write_batch(SERVICE, batch([(FP_A, 3.0, "R1")], start=T0 + 10))
        (row,) = await store.list_request_queries(SERVICE, "R1", since=None, limit=50)
        assert (row.call_count, row.total_ms, row.max_ms) == (2, 5.0, 3.0)
        assert row.last_seen == at(T0 + 10)

    async def test_37_service_isolation_for_fingerprint_list(self, store: StoreWriter) -> None:
        await store.write_batch(SERVICE, batch([(FP_A, 2.0, None)]))
        await store.write_batch(OTHER, batch([(FP_B, 2.0, None)]))
        assert [r.id for r in await store.list_queries(SERVICE, since=None, limit=50)] == [FP_A]
        assert [r.id for r in await store.list_queries(OTHER, since=None, limit=50)] == [FP_B]

    # -- 38 ------------------------------------------------------------------

    async def test_38_order_limit_and_since(self, store: StoreWriter) -> None:
        await store.write_batch(
            SERVICE,
            batch(
                [(FP_A, 5.0, "R"), (FP_B, 50.0, "R"), (FP_C, 50.0, "R")],
                step=10.0,
            ),
        )
        rows = await store.list_queries(SERVICE, since=None, limit=50)
        # total_ms desc, then id asc for ties
        assert [r.id for r in rows] == [FP_B, FP_C, FP_A]
        assert [r.id for r in await store.list_queries(SERVICE, since=None, limit=1)] == [FP_B]
        # since is inclusive on last_seen: FP_B last seen T0+10, FP_C T0+20
        since = at(T0 + 10)
        assert [r.id for r in await store.list_queries(SERVICE, since=since, limit=50)] == [
            FP_B,
            FP_C,
        ]
        assert [
            r.id for r in await store.list_request_queries(SERVICE, "R", since=since, limit=50)
        ] == [FP_B, FP_C]
        assert (await store.list_queries(SERVICE, since=at(T0 + 21), limit=50)) == []

    # -- 39 ------------------------------------------------------------------

    async def test_39_suggestions_are_unique_per_fingerprint_kind_sql(
        self, store: StoreWriter
    ) -> None:
        ddl = "CREATE INDEX IF NOT EXISTS ix_a_id ON a(id);"
        await store.write_batch(SERVICE, batch([(FP_A, 1.0, None)]))
        await store.insert_suggestions(FP_A, [suggestion(ddl), suggestion(ddl)])
        await store.insert_suggestions(FP_A, [suggestion(ddl)])
        await store.insert_suggestions(FP_A, [suggestion(None, kind="rewrite", source="llm")])
        await store.insert_suggestions(FP_A, [suggestion(None, kind="rewrite", source="llm")])
        await store.insert_suggestions(FP_A, [suggestion(ddl, kind="partition")])
        await store.insert_suggestions(FP_A, [])
        row = (await store.list_queries(SERVICE, since=None, limit=50))[0]
        got = [(s.kind, s.source, s.sql) for s in row.suggestions]
        assert got == [
            ("index", "rules", ddl),
            ("rewrite", "llm", None),
            ("partition", "rules", ddl),
        ]
        assert all(s.applied_at is None for s in row.suggestions)
        assert row.suggestions[0].rationale == "seq scan on a"

    # -- 40 / 41 -------------------------------------------------------------

    async def test_40_plan_before_fingerprint_and_latest_plan_wins(
        self, store: StoreWriter
    ) -> None:
        await store.upsert_plan(FP_A, {"Plan": {"Node Type": "Seq Scan"}}, 10.0, at(T0))
        await store.upsert_plan(FP_A, {"Plan": {"Node Type": "Index Scan"}}, 2.5, at(T0 + 60))
        await store.write_batch(SERVICE, batch([(FP_A, 1.0, None)]))
        detail = await store.get_query(SERVICE, FP_A)
        assert detail is not None
        assert detail.explain_plan is not None
        assert detail.explain_plan.plan_json == {"Plan": {"Node Type": "Index Scan"}}
        assert detail.explain_plan.cost == 2.5
        assert detail.explain_plan.captured_at == at(T0 + 60)

    async def test_41_detail_unknown_is_none(self, store: StoreWriter) -> None:
        assert await store.get_query(SERVICE, FP_A) is None
        await store.write_batch(OTHER, batch([(FP_A, 1.0, None)]))
        assert await store.get_query(SERVICE, FP_A) is None

    async def test_41_detail_has_plan_suggestions_and_newest_samples(
        self, store: StoreWriter
    ) -> None:
        observations: list[tuple[str, float, str | None]] = [
            (FP_A, 200.0 + i, "R1" if i % 2 else None) for i in range(25)
        ]
        await store.write_batch(SERVICE, batch(observations, threshold_ms=100.0))
        await store.insert_suggestions(
            FP_A, [suggestion("CREATE INDEX IF NOT EXISTS ix_a ON a(id)")]
        )
        detail = await store.get_query(SERVICE, FP_A)
        assert detail is not None
        assert detail.canonical_sql == SQL[FP_A]
        assert detail.fingerprint.id == FP_A
        assert detail.fingerprint.call_count == 25
        assert detail.explain_plan is None
        assert [s.sql for s in detail.suggestions] == ["CREATE INDEX IF NOT EXISTS ix_a ON a(id)"]
        assert detail.fingerprint.suggestions == detail.suggestions
        samples = detail.recent_samples
        assert len(samples) == 20
        assert [s.duration_ms for s in samples] == [224.0 - i for i in range(20)]
        assert samples[0].sampled_at == at(T0 + 24)
        assert samples[0].request_id is None
        assert samples[1].request_id == "R1"

    # -- 42 ------------------------------------------------------------------

    async def test_42_sweep_deletes_old_samples_and_request_rows_for_service_only(
        self, store: StoreWriter
    ) -> None:
        old = batch([(FP_A, 500.0, "OLD")], start=T0, threshold_ms=100.0)
        new = batch([(FP_A, 600.0, "NEW")], start=T0 + 3_600, threshold_ms=100.0)
        await store.write_batch(SERVICE, old)
        await store.write_batch(SERVICE, new)
        await store.write_batch(OTHER, old)
        await store.sweep(SERVICE, at(T0 + 60))
        assert await store.list_request_queries(SERVICE, "OLD", since=None, limit=50) == []
        assert len(await store.list_request_queries(SERVICE, "NEW", since=None, limit=50)) == 1
        assert len(await store.list_request_queries(OTHER, "OLD", since=None, limit=50)) == 1
        detail = await store.get_query(SERVICE, FP_A)
        assert detail is not None
        assert [s.request_id for s in detail.recent_samples] == ["NEW"]
        # Fingerprint totals are lifetime numbers and survive the sweep.
        assert detail.fingerprint.call_count == 2
        other = await store.get_query(OTHER, FP_A)
        assert other is not None
        assert [s.request_id for s in other.recent_samples] == ["OLD"]

    async def test_42_sweep_boundary_keeps_rows_at_the_cutoff(self, store: StoreWriter) -> None:
        await store.write_batch(SERVICE, batch([(FP_A, 500.0, "R")], threshold_ms=100.0))
        await store.sweep(SERVICE, at(T0))
        assert len(await store.list_request_queries(SERVICE, "R", since=None, limit=50)) == 1
        await store.sweep(SERVICE, at(T0) + timedelta(microseconds=1))
        assert await store.list_request_queries(SERVICE, "R", since=None, limit=50) == []

    # -- empty batch ----------------------------------------------------------

    async def test_empty_batch_is_a_no_op(self, store: StoreWriter) -> None:
        from slowquery_detective.recorder import EMPTY_BATCH

        await store.write_batch(SERVICE, EMPTY_BATCH)
        assert await store.list_queries(SERVICE, since=None, limit=50) == []
