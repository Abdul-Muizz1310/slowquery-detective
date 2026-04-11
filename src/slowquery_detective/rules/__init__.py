"""Rules engine — see ``docs/specs/03-rules.md``.

S3 stub. All behavior lands in S4.
"""

from __future__ import annotations

from slowquery_detective.rules.base import ALL_RULES, Rule, Suggestion, run_rules

__all__ = ["ALL_RULES", "Rule", "Suggestion", "run_rules"]
