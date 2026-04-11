"""In-memory EXPLAIN (FORMAT JSON) plan fixtures used by rules tests.

Kept in Python (not JSON) so refactoring is mechanical and type-checked.
Each helper returns a minimal plan that exercises one rule cleanly. A real
Postgres plan has many more fields; rules must only consult the ones named
in docs/specs/03-rules.md.
"""

from __future__ import annotations

from typing import Any


def seq_scan_large(
    *,
    table: str = "orders",
    rows: int = 50_000,
    total_cost: float = 1500.0,
) -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": table,
            "Plan Rows": rows,
            "Total Cost": total_cost,
            "Output": ["id", "user_id", "total", "created_at"],
        }
    }


def seq_scan_small(*, table: str = "countries") -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": table,
            "Plan Rows": 500,
            "Total Cost": 10.0,
            "Output": ["id", "code"],
        }
    }


def nested_loop_missing_fk_index() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Nested Loop",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "users",
                    "Plan Rows": 10_000,
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Plan Rows": 1_000_000,
                    "Filter": "(user_id = users.id)",
                },
            ],
        }
    }


def nested_loop_with_index() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Nested Loop",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "users", "Plan Rows": 10_000},
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "orders",
                    "Index Name": "ix_orders_user_id",
                    "Plan Rows": 1,
                },
            ],
        }
    }


def sort_with_high_cost() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Sort",
            "Sort Key": ["created_at"],
            "Actual Total Time": 120.0,
            "Total Cost": 2500.0,
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Plan Rows": 100_000,
                }
            ],
        }
    }


def index_scan_pre_sorted() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Index Scan",
            "Relation Name": "orders",
            "Index Name": "ix_orders_created_at",
            "Plan Rows": 1000,
            "Total Cost": 20.0,
        }
    }


def two_seq_scans_different_costs() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Append",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Plan Rows": 50_000,
                    "Total Cost": 1000.0,
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "order_items",
                    "Plan Rows": 500_000,
                    "Total Cost": 9000.0,
                },
            ],
        }
    }


def wide_select_star(column_count: int = 25) -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Plan Rows": 100,
            "Total Cost": 5.0,
            "Output": [f"col_{i}" for i in range(column_count)],
        }
    }


def empty_plan() -> dict[str, Any]:
    return {}


def unknown_node_type() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Gather Merge",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Plan Rows": 500,
                }
            ],
        }
    }


def malformed_no_plan_key() -> dict[str, Any]:
    return {"Planning Time": 1.2}


def plan_rows_as_string() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "orders",
            "Plan Rows": "100000",  # some serializers emit strings
            "Total Cost": 1500.0,
        }
    }
