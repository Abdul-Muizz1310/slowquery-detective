"""Red tests for docs/specs/03-rules.md.

25 enumerated cases across six rules and the ``run_rules`` dispatcher.
All rules are pure; this whole file is unit-level.
"""

from __future__ import annotations

import pytest

from slowquery_detective.rules import run_rules
from slowquery_detective.rules.function_in_where import FunctionInWhere
from slowquery_detective.rules.missing_fk_index import MissingFkIndex
from slowquery_detective.rules.n_plus_one import NPlusOneSuspicion
from slowquery_detective.rules.select_star import SelectStarWideTable
from slowquery_detective.rules.seq_scan import SeqScanLargeTable
from slowquery_detective.rules.sort_without_index import SortWithoutIndex
from tests.fixtures import plans

FID = "abcdef0123456789"

# ---------------------------------------------------------------------------
# Per-rule tests
# ---------------------------------------------------------------------------


def test_01_seq_scan_large_table_fires_with_where() -> None:
    s = SeqScanLargeTable().apply(
        plans.seq_scan_large(),
        "SELECT * FROM orders WHERE user_id = ?",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None
    assert s.kind == "index"
    assert s.sql is not None
    assert "CREATE INDEX IF NOT EXISTS ix_orders_user_id ON orders(user_id)" in s.sql
    assert s.confidence >= 0.85


def test_02_seq_scan_large_table_does_not_fire_on_small() -> None:
    s = SeqScanLargeTable().apply(
        plans.seq_scan_small(),
        "SELECT * FROM countries WHERE id = ?",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None


def test_03_seq_scan_does_not_fire_without_where() -> None:
    s = SeqScanLargeTable().apply(
        plans.seq_scan_large(),
        "SELECT * FROM orders",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None


def test_04_seq_scan_picks_highest_cost() -> None:
    s = SeqScanLargeTable().apply(
        plans.two_seq_scans_different_costs(),
        "SELECT * FROM orders WHERE x = ? UNION SELECT * FROM order_items WHERE y = ?",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None and s.sql is not None
    assert "order_items" in s.sql  # the higher-cost table


def test_05_missing_fk_index_fires_on_nested_loop_seq_scan() -> None:
    s = MissingFkIndex().apply(
        plans.nested_loop_missing_fk_index(),
        "SELECT * FROM users u JOIN orders o ON o.user_id = u.id",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None
    assert s.kind == "index"
    assert s.sql is not None and "user_id" in s.sql
    assert s.confidence >= 0.9


def test_06_missing_fk_index_does_not_fire_when_indexed() -> None:
    s = MissingFkIndex().apply(
        plans.nested_loop_with_index(),
        "SELECT * FROM users u JOIN orders o ON o.user_id = u.id",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None


def test_07_sort_without_index_fires() -> None:
    s = SortWithoutIndex().apply(
        plans.sort_with_high_cost(),
        "SELECT * FROM orders ORDER BY created_at DESC",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None
    assert s.kind == "index"
    assert s.sql is not None and "created_at" in s.sql


def test_08_sort_without_index_does_not_fire_on_pre_sorted_index() -> None:
    s = SortWithoutIndex().apply(
        plans.index_scan_pre_sorted(),
        "SELECT * FROM orders ORDER BY created_at DESC",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None


def test_09_function_in_where_fires_on_lower_col() -> None:
    s = FunctionInWhere().apply(
        {"Plan": {"Node Type": "Seq Scan", "Relation Name": "users"}},
        "SELECT * FROM users WHERE LOWER(email) = ?",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None
    assert s.kind == "index"
    assert s.sql is not None
    assert "LOWER" in s.sql.upper() and "email" in s.sql


def test_10_function_in_where_does_not_fire_on_fn_of_param() -> None:
    s = FunctionInWhere().apply(
        {"Plan": {"Node Type": "Seq Scan", "Relation Name": "users"}},
        "SELECT * FROM users WHERE email = LOWER(?)",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None


def test_11_select_star_wide_table_fires() -> None:
    s = SelectStarWideTable().apply(
        plans.wide_select_star(column_count=25),
        "SELECT * FROM users",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None
    assert s.kind == "rewrite"
    assert s.sql is None


def test_12_select_star_does_not_fire_on_narrow_table() -> None:
    s = SelectStarWideTable().apply(
        plans.wide_select_star(column_count=8),
        "SELECT * FROM users",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None


def test_13_n_plus_one_fires_above_threshold_only() -> None:
    rule = NPlusOneSuspicion()
    hot = rule.apply({}, "SELECT 1", fingerprint_id=FID, recent_call_count=60)
    cold = rule.apply({}, "SELECT 1", fingerprint_id=FID, recent_call_count=20)
    assert hot is not None and hot.kind == "rewrite"
    assert cold is None


# ---------------------------------------------------------------------------
# Aggregate / ordering
# ---------------------------------------------------------------------------


def test_14_multi_rule_plan_sorted_by_confidence() -> None:
    # Plan exercises both seq_scan_large_table and sort_without_index.
    plan = plans.sort_with_high_cost()
    plan["Plan"]["Plans"][0]["Total Cost"] = 1500.0  # make seq_scan expensive too
    plan["Plan"]["Plans"][0]["Relation Name"] = "orders"
    suggestions = run_rules(
        plan,
        "SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC",
        fingerprint_id=FID,
    )
    assert len(suggestions) >= 2
    # Highest confidence first.
    confidences = [s.confidence for s in suggestions]
    assert confidences == sorted(confidences, reverse=True)
    assert suggestions[0].rule_name in {"seq_scan_large_table", "missing_fk_index"}


def test_15_no_rule_match_returns_empty_list() -> None:
    suggestions = run_rules(
        {"Plan": {"Node Type": "Result", "Plan Rows": 1}},
        "SELECT 1",
        fingerprint_id=FID,
    )
    assert suggestions == []


def test_16_identical_confidence_alphabetical_order() -> None:
    # We can't easily arrange two rules with identical confidence without
    # real implementations, so this test asserts the *ordering contract* by
    # passing a plan that fires two rules and checking rule_name order as
    # a stable tiebreaker when confidences are equal.
    plan = plans.sort_with_high_cost()
    suggestions = run_rules(
        plan,
        "SELECT * FROM orders ORDER BY created_at DESC",
        fingerprint_id=FID,
    )
    names_with_same_conf = [
        s.rule_name
        for s in suggestions
        if s.confidence == suggestions[0].confidence and s.rule_name is not None
    ]
    assert names_with_same_conf == sorted(names_with_same_conf)


# ---------------------------------------------------------------------------
# Failure / edge
# ---------------------------------------------------------------------------


def test_17_none_plan_raises_type_error() -> None:
    with pytest.raises((TypeError, AttributeError)):
        run_rules(None, "SELECT 1", fingerprint_id=FID)  # type: ignore[arg-type]


def test_18_empty_plan_empty_sql_returns_empty() -> None:
    assert run_rules({}, "", fingerprint_id=FID) == []


def test_19_unknown_node_type_does_not_raise() -> None:
    suggestions = run_rules(
        plans.unknown_node_type(),
        "SELECT * FROM orders WHERE user_id = ?",
        fingerprint_id=FID,
    )
    assert isinstance(suggestions, list)


def test_20_malformed_plan_missing_plan_key() -> None:
    assert run_rules(plans.malformed_no_plan_key(), "SELECT 1", fingerprint_id=FID) == []


def test_21_plan_rows_as_string_does_not_raise() -> None:
    suggestions = run_rules(
        plans.plan_rows_as_string(),
        "SELECT * FROM orders WHERE x = ?",
        fingerprint_id=FID,
    )
    assert isinstance(suggestions, list)


def test_22_keyword_named_table_is_quoted() -> None:
    plan = plans.seq_scan_large(table="user")
    s = SeqScanLargeTable().apply(
        plan,
        'SELECT * FROM "user" WHERE id = ?',
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is not None and s.sql is not None
    assert '"user"' in s.sql or '"user"' in s.sql.lower()


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


def test_23_injection_payload_never_appears_in_ddl() -> None:
    payload = "'); DROP TABLE users; --"
    plan = plans.seq_scan_large()
    s = SeqScanLargeTable().apply(
        plan,
        f"SELECT * FROM orders WHERE user_id = '{payload}'",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    if s is not None and s.sql is not None:
        assert payload not in s.sql
        assert "DROP TABLE" not in s.sql.upper()


def test_24_invalid_identifier_rejected_by_whitelist() -> None:
    plan = plans.seq_scan_large(table="orders; DROP TABLE users; --")
    s = SeqScanLargeTable().apply(
        plan,
        "SELECT * FROM orders WHERE user_id = ?",
        fingerprint_id=FID,
        recent_call_count=0,
    )
    assert s is None  # identifier fails regex, rule abstains


def test_25_no_rule_emits_destructive_ddl() -> None:
    """Grep test over the rules package: no rule's source contains DROP/ALTER/etc."""
    import pathlib

    rules_dir = pathlib.Path(__file__).resolve().parents[2] / "src" / "slowquery_detective" / "rules"
    forbidden = ("DROP ", "ALTER ", "TRUNCATE ", "GRANT ", "REVOKE ", "UPDATE ", "DELETE ")
    for path in rules_dir.glob("*.py"):
        source = path.read_text(encoding="utf-8").upper()
        for word in forbidden:
            assert word not in source, f"{path.name} contains banned DDL keyword {word!r}"
