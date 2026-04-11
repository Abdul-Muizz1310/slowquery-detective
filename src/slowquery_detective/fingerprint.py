"""Fingerprint parameterizer — collapse literal-equivalent SQL into one ID.

See ``docs/specs/00-fingerprint.md`` for the full contract. The high-level
shape is: parse with sqlglot, walk the AST replacing every literal / param
with a single ``Placeholder``, collapse IN-lists of placeholders to one
placeholder, re-serialize, lowercase + collapse whitespace, SHA1 the first
16 hex chars. Any parse failure (or ``RecursionError`` on pathologically
deep input) falls through to a deterministic regex-based fallback so the
middleware never crashes on exotic SQL.
"""

from __future__ import annotations

import hashlib
import re

import sqlglot
from sqlglot import exp

# Match SQL string literals (single-quoted, supporting ``''`` as an escaped
# single quote), numeric literals, and bare keyword literals.
_FALLBACK_LITERAL_RE = re.compile(
    r"""
    '(?:[^']|'')*'              # single-quoted string literal
    | -?\d+(?:\.\d+)?           # numeric literal
    | \btrue\b | \bfalse\b | \bnull\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Match driver / dialect parameter placeholders.
_PARAM_RE = re.compile(r"\$\d+|%s|:\w+")

# Collapse all whitespace runs.
_WS_RE = re.compile(r"\s+")

# Strip SQL comments before the regex fallback so ``-- foo`` / ``/* bar */``
# don't leak into the canonical form.
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def fingerprint(sql: str, dialect: str = "postgres") -> tuple[str, str]:
    """Return ``(fingerprint_id, canonical_sql)`` for a SQL statement.

    Args:
        sql: Non-empty SQL text. May include inline literals and parameters.
        dialect: sqlglot dialect name. Defaults to ``"postgres"``.

    Returns:
        A tuple of ``(fingerprint_id, canonical_sql)`` where ``fingerprint_id``
        is 16 lowercase hex characters and ``canonical_sql`` is the normalized
        parameterized form with every literal replaced by ``?``.

    Raises:
        TypeError: if ``sql`` is ``None``.
        ValueError: if ``sql`` is empty or whitespace-only.
    """
    if sql is None:
        raise TypeError("sql must not be None")
    if not sql or not sql.strip():
        raise ValueError("sql must not be empty")

    canonical = _canonicalize_via_sqlglot(sql, dialect)
    if canonical is None:
        canonical = _canonicalize_via_regex(sql)

    # Final normalization pass applies to both paths so they converge.
    canonical = _PARAM_RE.sub("?", canonical)
    canonical = _WS_RE.sub(" ", canonical).strip().rstrip(";").lower()

    fid = hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]
    return fid, canonical


def _canonicalize_via_sqlglot(sql: str, dialect: str) -> str | None:
    """Parse via sqlglot and scrub literals. Returns ``None`` on any failure.

    Any exception — parse error, recursion error, unexpected AST shape — is
    swallowed so the caller can fall back to the regex path.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return None

    if tree is None:
        return None

    try:
        # 1. Parameters ($1, :name, ?) -> Placeholder
        for param in list(tree.find_all(exp.Parameter)):
            param.replace(exp.Placeholder())

        # 2. Literals (numeric, string) -> Placeholder
        for literal in list(tree.find_all(exp.Literal)):
            literal.replace(exp.Placeholder())

        # 3. Boolean / NULL -> Placeholder
        for boolean in list(tree.find_all(exp.Boolean)):
            boolean.replace(exp.Placeholder())
        for null in list(tree.find_all(exp.Null)):
            null.replace(exp.Placeholder())

        # 4. Named placeholders (``:name``) -> unnamed ``?`` so parameter
        #    style never affects the fingerprint.
        for ph in list(tree.find_all(exp.Placeholder)):
            if ph.args.get("this"):
                ph.replace(exp.Placeholder())

        # 5. Collapse IN (<literal-list>) -> IN (?) so arity doesn't affect
        #    the fingerprint. Only collapse lists where every child is now a
        #    Placeholder — we keep subquery IN lists intact.
        for in_node in list(tree.find_all(exp.In)):
            expressions = in_node.args.get("expressions") or []
            if expressions and all(isinstance(e, exp.Placeholder) for e in expressions):
                in_node.set("expressions", [exp.Placeholder()])

        return tree.sql(dialect=dialect, comments=False)
    except Exception:
        return None


def _canonicalize_via_regex(sql: str) -> str:
    """Regex-based best-effort scrub for SQL that sqlglot can't parse."""
    stripped = _LINE_COMMENT_RE.sub("", sql)
    stripped = _BLOCK_COMMENT_RE.sub("", stripped)
    return _FALLBACK_LITERAL_RE.sub("?", stripped)
