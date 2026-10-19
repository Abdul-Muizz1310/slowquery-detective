"""Spec 07 cases 24-27 — literal values never leave the worker inside a plan."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from slowquery_detective.plan_scrub import scrub_expression, scrub_plan


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("(email = 'a@b.c'::text)", "(email = ?::text)"),
        ("(ids = ANY ('{1,2,3}'::integer[]))", "(ids = ANY (?::integer[]))"),
        ("(name = 'O''Brien'::text)", "(name = ?::text)"),
        ("(id = 42)", "(id = ?)"),
        ("(amount > -3.5)", "(amount > -?)"),
        ("(total >= 1e6)", "(total >= ?)"),
        ("(col1 = col2)", "(col1 = col2)"),
        ("(id = $1)", "(id = $1)"),
        ("((created_at)::date > '2024-01-01'::date)", "((created_at)::date > ?::date)"),
        ("lower((email)::text)", "lower((email)::text)"),
        ("(t.v = E'line\\nbreak'::text)", "(t.v = E?::text)"),
        ("(x IS NULL)", "(x IS NULL)"),
    ],
)
def test_24_25_scrub_expression(raw: str, expected: str) -> None:
    assert scrub_expression(raw) == expected


def _plan() -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Nested Loop",
            "Total Cost": 1234.5,
            "Plan Rows": 42,
            "Plan Width": 8,
            "Join Filter": "(o.total > 100)",
            "Plans": [
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "users",
                    "Schema": "public",
                    "Alias": "u1",
                    "Index Name": "ix_users_email2",
                    "Index Cond": "(email = 'secret@corp.com'::text)",
                    "Plan Rows": 1,
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Alias": "o",
                    "Filter": "(note = 'sk-live-abc123'::text)",
                    "Recheck Cond": "(user_id = 7)",
                    "Hash Cond": "(o.k = 'z'::text)",
                    "Sort Key": ["(o.v || 'pepper'::text)"],
                    "Output": ["o.id", "'literal-out'::text"],
                    "Plan Rows": 50000,
                },
            ],
        },
        "Planning Time": 0.12,
    }


def test_24_secrets_are_removed_from_every_expression_field() -> None:
    scrubbed = scrub_plan(_plan())
    text = json.dumps(scrubbed)
    for secret in ("secret@corp.com", "sk-live-abc123", "pepper", "literal-out", "'z'"):
        assert secret not in text
    child = scrubbed["Plan"]["Plans"][1]
    assert child["Filter"] == "(note = ?::text)"
    assert child["Recheck Cond"] == "(user_id = ?)"
    assert child["Sort Key"] == ["(o.v || ?::text)"]
    assert child["Output"] == ["o.id", "?::text"]
    assert scrubbed["Plan"]["Join Filter"] == "(o.total > ?)"


def test_25_identifiers_and_planner_numbers_are_kept() -> None:
    scrubbed = scrub_plan(_plan())
    root = scrubbed["Plan"]
    assert root["Node Type"] == "Nested Loop"
    assert root["Total Cost"] == 1234.5
    assert root["Plan Rows"] == 42
    idx = root["Plans"][0]
    assert idx["Relation Name"] == "users"
    assert idx["Index Name"] == "ix_users_email2"
    assert idx["Alias"] == "u1"
    assert idx["Schema"] == "public"
    assert idx["Index Cond"] == "(email = ?::text)"
    assert scrubbed["Planning Time"] == 0.12


def test_26_input_is_not_mutated() -> None:
    plan = _plan()
    before = copy.deepcopy(plan)
    scrub_plan(plan)
    assert plan == before


def test_26_odd_shapes_are_tolerated() -> None:
    assert scrub_plan({}) == {}
    assert scrub_plan({"Plan": {"Plans": [1, None, "x = 'y'"]}}) == {
        "Plan": {"Plans": [1, None, "x = ?"]}
    }
    assert scrub_plan({"Plan": {"Filter": None, "Flag": True}}) == {
        "Plan": {"Filter": None, "Flag": True}
    }


@settings(max_examples=200, deadline=None)
@given(
    st.text(
        alphabet=st.characters(
            codec="utf-8", categories=("L", "N", "P", "S", "Zs"), exclude_characters="\x00"
        ),
        min_size=4,
        max_size=40,
    )
)
def test_27_property_no_literal_survives(secret: str) -> None:
    quoted = secret.replace("'", "''")
    plan = {
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "t",
            "Filter": f"(v = '{quoted}'::text)",
            "Plans": [{"Node Type": "Index Scan", "Index Cond": f"(w = '{quoted}'::text)"}],
        }
    }
    scrubbed = scrub_plan(plan)
    assert scrubbed["Plan"]["Filter"] == "(v = ?::text)"
    assert scrubbed["Plan"]["Plans"][0]["Index Cond"] == "(w = ?::text)"
