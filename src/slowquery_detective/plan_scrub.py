"""Remove literal values from an EXPLAIN plan — spec 07 cases 24-27.

The worker plans a slow statement with its real parameters, so Postgres
prints their values into the plan (``Index Cond: (email = 'a@b.c'::text)``).
The rules engine may look at that plan in memory; anything that leaves the
worker — the stored plan, the plan sent to an LLM — goes through
:func:`scrub_plan` first.

Every string value is treated as an expression unless its key is one of the
structural keys that name things (``Node Type``, ``Relation Name``, …). In an
expression, quoted string literals and numeric literals become ``?``;
double-quoted identifiers, plain identifiers (``col1``) and parameter symbols
(``$1``) are kept. Numbers that are JSON values (costs, rows, widths) are the
planner's own output, not query data, and are kept.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Final

# Keys whose string values are names, not expressions.
STRUCTURAL_KEYS: Final[frozenset[str]] = frozenset(
    {
        "Node Type",
        "Parent Relationship",
        "Relation Name",
        "Schema",
        "Alias",
        "Index Name",
        "Join Type",
        "Strategy",
        "Scan Direction",
        "Partial Mode",
        "Operation",
        "Subplan Name",
        "CTE Name",
        "Function Name",
        "Command",
        "Sort Method",
        "Sort Space Type",
    }
)

_TOKEN: Final = re.compile(
    r"""
      (?P<ident>"(?:[^"]|"")*")                       # quoted identifier: keep
    | (?P<string>'(?:[^']|'')*')                      # string literal: scrub
    | (?P<number>(?<![\w$.])\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?![\w.]))  # number: scrub
    """,
    re.VERBOSE,
)


def _replace(match: re.Match[str]) -> str:
    if match.group("ident") is not None:
        return match.group("ident")
    return "?"


def scrub_expression(text: str) -> str:
    """Replace every literal value in a plan expression with ``?``."""
    return _TOKEN.sub(_replace, text)


def _scrub_value(key: str | None, value: Any) -> Any:
    if isinstance(value, str):
        return value if key in STRUCTURAL_KEYS else scrub_expression(value)
    if isinstance(value, Mapping):
        return {k: _scrub_value(k, v) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub_value(key, v) for v in value]
    return value


def scrub_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of ``plan`` with every literal value replaced by ``?``."""
    return {k: _scrub_value(k, v) for k, v in plan.items()}
