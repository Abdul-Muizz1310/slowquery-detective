"""slowquery-detective — catch slow Postgres queries, suggest fixes.

Public surface::

    from slowquery_detective import install

    install(app, engine, service="paper-trail",
            store_url=DATABASE_URL, authorize=verify_platform_token)

See ``docs/specs/07-fleet-observability.md`` for the full contract.
"""

from slowquery_detective.llm_explainer import LlmConfig
from slowquery_detective.middleware import install
from slowquery_detective.pg_store import PostgresStoreWriter
from slowquery_detective.schemas import QueryDetail, QueryRow, SuggestionOut
from slowquery_detective.store import InMemoryStoreWriter, StoreWriter
from slowquery_detective.tagging import current_request_id, request_scope

__version__ = "0.2.1"

__all__ = [
    "InMemoryStoreWriter",
    "LlmConfig",
    "PostgresStoreWriter",
    "QueryDetail",
    "QueryRow",
    "StoreWriter",
    "SuggestionOut",
    "__version__",
    "current_request_id",
    "install",
    "request_scope",
]
