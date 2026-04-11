"""FastAPI integration — see ``docs/specs/05-middleware.md``.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

from typing import Any

from slowquery_detective.llm_explainer import LlmConfig


def install(
    app: Any,
    engine: Any,
    *,
    threshold_ms: int = 100,
    sample_rate: float = 1.0,
    store_url: str | None = None,
    enable_llm: bool = False,
    llm_config: LlmConfig | None = None,
) -> None:
    """Attach slowquery-detective to a FastAPI app + SQLAlchemy engine.

    3-line integration::

        from slowquery_detective import install
        install(app, engine)
    """
    raise NotImplementedError("S4: implement install() per docs/specs/05-middleware.md")
