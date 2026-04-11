"""slowquery-detective — catch slow Postgres queries, suggest fixes.

Public surface::

    from slowquery_detective import install, dashboard_router

See ``docs/specs/05-middleware.md`` for the full contract.
"""

from slowquery_detective.dashboard import dashboard_router
from slowquery_detective.middleware import install

__version__ = "0.1.0"

__all__ = ["__version__", "dashboard_router", "install"]
