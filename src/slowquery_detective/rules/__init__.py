"""Rules engine — see ``docs/specs/03-rules.md``.

Public exports for the six-rule pipeline. The rule classes themselves live
in sibling modules and are assembled into the ``ALL_RULES`` tuple by
``rules/base.py``.
"""

from __future__ import annotations

from slowquery_detective.rules.base import ALL_RULES, Rule, Suggestion, run_rules

__all__ = ["ALL_RULES", "Rule", "Suggestion", "run_rules"]
