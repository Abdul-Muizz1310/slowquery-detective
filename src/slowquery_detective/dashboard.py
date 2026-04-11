"""Dashboard API router — see ``docs/specs/05-middleware.md``.

S3 stub. All behavior lands in S4.

The DDL allowlist regex lives here as a module-level constant so the rules
engine, the middleware, and the red tests all share exactly one definition.
"""

from __future__ import annotations

import re

# Only ``CREATE INDEX [CONCURRENTLY] IF NOT EXISTS ix_...`` is ever executable
# via ``POST /api/queries/{id}/apply``. Anything else returns 400.
DDL_ALLOWLIST_REGEX: re.Pattern[str] = re.compile(
    r"^CREATE INDEX( CONCURRENTLY)? IF NOT EXISTS "
    r'ix_[A-Za-z0-9_]+ ON [A-Za-z0-9_"]+\s*\('
    r"[A-Za-z0-9_,\s()]+\);?$"
)


def _build_router() -> object:
    """Construct the APIRouter exposed by the package.

    Kept behind a helper so the module imports even when FastAPI isn't
    installed in the consumer's environment.
    """
    raise NotImplementedError("S4: build dashboard_router per docs/specs/05-middleware.md")


# Lazily-built at import time in S4. For S3 the name exists so tests can
# import it and fail with ``NotImplementedError`` when they try to use it.
class _LazyRouter:
    def __getattr__(self, item: str) -> object:
        raise NotImplementedError("S4: implement dashboard_router per docs/specs/05-middleware.md")


dashboard_router: object = _LazyRouter()
