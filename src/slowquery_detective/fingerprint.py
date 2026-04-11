"""Fingerprint parameterizer — collapse literal-equivalent SQL into one ID.

See ``docs/specs/00-fingerprint.md`` for the full contract. All behavior lands
in S4; this module is an S3 stub that exists only so tests can import it and
fail with :class:`NotImplementedError` instead of :class:`ImportError`.
"""

from __future__ import annotations


def fingerprint(sql: str, dialect: str = "postgres") -> tuple[str, str]:
    """Return ``(fingerprint_id, canonical_sql)`` for a SQL statement.

    Args:
        sql: Non-empty SQL text. May include inline literals and parameters.
        dialect: sqlglot dialect name. Defaults to ``"postgres"``.

    Returns:
        A tuple of ``(fingerprint_id, canonical_sql)``.

    Raises:
        NotImplementedError: always, until S4 ships.
    """
    raise NotImplementedError("S4: implement fingerprint() per docs/specs/00-fingerprint.md")
