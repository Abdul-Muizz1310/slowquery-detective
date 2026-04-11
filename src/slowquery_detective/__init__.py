"""slowquery-detective — catch slow Postgres queries, suggest fixes.

Public surface is intentionally tiny: see the ``install`` helper (added in S3)
for the 3-line FastAPI + SQLAlchemy integration described in
``docs/specs/05-middleware.md``.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
