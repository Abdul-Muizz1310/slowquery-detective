"""Async EXPLAIN worker — see ``docs/specs/06-explain-worker.md``.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from slowquery_detective.rules.base import Suggestion
from slowquery_detective.store import StoreWriter

RulesCallable = Callable[[dict[str, Any], str], list[Suggestion]]
ExplainerCallable = Callable[..., Awaitable[Suggestion | None]]


@dataclass(frozen=True)
class ExplainJob:
    fingerprint_id: str
    canonical_sql: str
    observed_ms: float
    enqueued_at: float


@dataclass(frozen=True)
class CachedPlan:
    plan_json: dict[str, Any]
    plan_text: str
    cost: float
    captured_at: float
    suggestions: tuple[Suggestion, ...]


class ExplainWorker:
    """Background asyncio task that runs EXPLAIN and feeds the rules/LLM pipe."""

    def __init__(
        self,
        engine: Any,
        store: StoreWriter,
        rules: RulesCallable,
        explainer: ExplainerCallable | None,
        *,
        per_fingerprint_cooldown_seconds: float = 60.0,
        explain_timeout_seconds: float = 10.0,
        max_queue_size: int = 256,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._store = store
        self._rules = rules
        self._explainer = explainer
        self._cooldown = per_fingerprint_cooldown_seconds
        self._timeout = explain_timeout_seconds
        self._max_queue_size = max_queue_size
        self._now = now

    async def start(self) -> None:
        raise NotImplementedError(
            "S4: implement ExplainWorker.start per docs/specs/06-explain-worker.md"
        )

    async def stop(self) -> None:
        raise NotImplementedError(
            "S4: implement ExplainWorker.stop per docs/specs/06-explain-worker.md"
        )

    def submit(self, job: ExplainJob) -> bool:
        raise NotImplementedError(
            "S4: implement ExplainWorker.submit per docs/specs/06-explain-worker.md"
        )

    def plan_cache_get(self, fingerprint_id: str) -> CachedPlan | None:
        raise NotImplementedError(
            "S4: implement ExplainWorker.plan_cache_get per docs/specs/06-explain-worker.md"
        )
