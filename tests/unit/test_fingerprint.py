"""Red tests for docs/specs/00-fingerprint.md.

28 enumerated cases. Every test currently fails with :class:`NotImplementedError`
from the S3 stub in ``src/slowquery_detective/fingerprint.py``. S4 will turn
them green.
"""

from __future__ import annotations

import hypothesis
import hypothesis.strategies as st
import pytest

from slowquery_detective.fingerprint import fingerprint

# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_01_same_pattern_different_literal() -> None:
    fid_a, _ = fingerprint("SELECT * FROM users WHERE id = 1")
    fid_b, _ = fingerprint("SELECT * FROM users WHERE id = 42")
    assert fid_a == fid_b


def test_02_different_where_column_differs() -> None:
    fid_a, _ = fingerprint("SELECT * FROM users WHERE id = 1")
    fid_b, _ = fingerprint("SELECT * FROM users WHERE email = 'a'")
    assert fid_a != fid_b


def test_03_float_literals_normalized() -> None:
    fid_a, canon = fingerprint("SELECT * FROM orders WHERE total > 99.95")
    fid_b, _ = fingerprint("SELECT * FROM orders WHERE total > 12.0")
    assert fid_a == fid_b
    assert "99.95" not in canon
    assert "12.0" not in canon


def test_04_insert_values_parameterized() -> None:
    fid_a, _ = fingerprint("INSERT INTO t (a, b) VALUES (1, 'x')")
    fid_b, _ = fingerprint("INSERT INTO t (a, b) VALUES (2, 'y')")
    assert fid_a == fid_b


def test_05_boolean_literals_parameterized() -> None:
    fid_a, _ = fingerprint("UPDATE users SET active = true WHERE id = 1")
    fid_b, _ = fingerprint("UPDATE users SET active = false WHERE id = 2")
    assert fid_a == fid_b


@pytest.mark.parametrize(
    "placeholder",
    ["$1", "%s", ":name", "?"],
    ids=["dollar", "pyformat", "named", "qmark"],
)
def test_06_parameter_styles_collapse(placeholder: str) -> None:
    reference, _ = fingerprint("SELECT * FROM users WHERE id = ?")
    fid, _ = fingerprint(f"SELECT * FROM users WHERE id = {placeholder}")
    assert fid == reference


def test_07_canonical_sql_contains_no_digit_one() -> None:
    _, canon = fingerprint("SELECT 1")
    assert "1" not in canon


def test_08_fingerprint_id_is_16_lowercase_hex() -> None:
    fid, _ = fingerprint("SELECT * FROM users WHERE id = 1")
    assert len(fid) == 16
    assert fid == fid.lower()
    assert all(c in "0123456789abcdef" for c in fid)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_09_in_list_collapses_across_lengths() -> None:
    fid_a, _ = fingerprint("SELECT * FROM t WHERE id IN (1, 2, 3)")
    fid_b, _ = fingerprint("SELECT * FROM t WHERE id IN (4, 5, 6, 7, 8, 9)")
    assert fid_a == fid_b


def test_10_in_list_single_vs_multi() -> None:
    fid_a, _ = fingerprint("SELECT * FROM t WHERE id IN (1)")
    fid_b, _ = fingerprint("SELECT * FROM t WHERE id IN (1, 2)")
    assert fid_a == fid_b


def test_11_limit_and_offset_normalized() -> None:
    fid_a, _ = fingerprint("SELECT * FROM t LIMIT 10 OFFSET 0")
    fid_b, _ = fingerprint("SELECT * FROM t LIMIT 20 OFFSET 100")
    assert fid_a == fid_b


def test_12_order_by_direction_is_structural() -> None:
    fid_a, _ = fingerprint("SELECT * FROM t ORDER BY created_at DESC")
    fid_b, _ = fingerprint("SELECT * FROM t ORDER BY created_at ASC")
    assert fid_a != fid_b


def test_13_block_comment_stripped() -> None:
    fid_a, _ = fingerprint("/* X-Request-Id: abc */ SELECT 1")
    fid_b, _ = fingerprint("SELECT 1")
    assert fid_a == fid_b


def test_14_line_comment_stripped() -> None:
    fid_a, _ = fingerprint("-- traceparent: foo\nSELECT 1")
    fid_b, _ = fingerprint("SELECT 1")
    assert fid_a == fid_b


def test_15_trailing_semicolon_ignored() -> None:
    fid_a, _ = fingerprint("SELECT 1;")
    fid_b, _ = fingerprint("SELECT 1")
    assert fid_a == fid_b


def test_16_case_insensitive_keywords() -> None:
    fid_a, _ = fingerprint("select * from USERS where ID = 1")
    fid_b, _ = fingerprint("SELECT * FROM users WHERE id = 1")
    assert fid_a == fid_b


def test_17_whitespace_insensitive() -> None:
    fid_a, _ = fingerprint("SELECT\n  *\n  FROM users\n  WHERE id = 1")
    fid_b, _ = fingerprint("SELECT * FROM users WHERE id = 1")
    assert fid_a == fid_b


def test_18_deeply_nested_subqueries_do_not_recurse_crash() -> None:
    nested = "SELECT * FROM (" * 10 + "SELECT 1" + ")" * 10
    fid, _ = fingerprint(nested)
    assert len(fid) == 16


# ---------------------------------------------------------------------------
# Failure cases
# ---------------------------------------------------------------------------


def test_19_empty_string_raises() -> None:
    with pytest.raises(ValueError):
        fingerprint("")


def test_20_whitespace_only_raises() -> None:
    with pytest.raises(ValueError):
        fingerprint("   \n  \t ")


def test_21_none_raises_type_error() -> None:
    with pytest.raises((TypeError, ValueError)):
        fingerprint(None)  # type: ignore[arg-type]


def test_22_unparseable_sql_falls_back_to_regex() -> None:
    # SELECT FROM is invalid but the function must not raise.
    fid, canon = fingerprint("SELECT FROM")
    assert len(fid) == 16
    assert canon  # some canonical form returned


def test_23_extremely_long_sql_completes() -> None:
    big = "SELECT 1" + (" UNION SELECT 1" * 2000)
    fid, _ = fingerprint(big)
    assert len(fid) == 16


def test_24_mysql_dialect_produces_valid_output() -> None:
    fid, canon = fingerprint("SELECT * FROM users LIMIT 10", dialect="mysql")
    assert len(fid) == 16
    assert canon


# ---------------------------------------------------------------------------
# Security cases
# ---------------------------------------------------------------------------


def test_25_ssn_literal_scrubbed() -> None:
    _, canon = fingerprint("SELECT * FROM users WHERE ssn = '123-45-6789'")
    assert "123-45-6789" not in canon
    assert "123456789" not in canon
    assert "ssn = '" not in canon


def test_26_api_key_literal_scrubbed() -> None:
    _, canon = fingerprint("SELECT * FROM users WHERE api_key = 'sk-live-abc123'")
    assert "sk-live-" not in canon
    assert "abc123" not in canon


def test_27_injection_payload_same_fingerprint_as_benign() -> None:
    fid_injection, _ = fingerprint("SELECT * FROM users WHERE name = ''' OR 1=1 --'")
    fid_benign, _ = fingerprint("SELECT * FROM users WHERE name = 'alice'")
    assert fid_injection == fid_benign


@hypothesis.given(
    literal=st.text(
        # The test asserts "user literal content never survives into the
        # canonical SQL". Short literals (1-3 characters) can coincidentally
        # overlap SQL keywords like ``select`` / ``from`` / ``where`` and
        # produce false positives that have nothing to do with the property
        # we care about (PII / secret scrubbing). ``min_size=4`` tests
        # actual content while excluding ``'`` / ``?`` / ``\`` (structural
        # markers) and control categories.
        alphabet=st.characters(
            whitelist_categories=("L", "N"),  # letters and numbers only
            blacklist_characters="?",  # placeholder marker
        ),
        min_size=4,
        max_size=32,
    )
)
@hypothesis.settings(max_examples=200, deadline=None)
def test_28_property_no_literal_survives_in_canonical(literal: str) -> None:
    sql = f"SELECT * FROM t WHERE name = '{literal}'"
    _, canon = fingerprint(sql)
    assert literal not in canon
