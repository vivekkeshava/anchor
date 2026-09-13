"""
Key derivation is load-bearing: if it is unstable, the effect barrier silently stops
deduplicating and every guarantee built on top of it evaporates. These tests pin the
properties the barrier depends on.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from anchor.hashing import canonical_json, hash_payload, idempotency_key


def test_key_is_stable_across_dict_ordering():
    a = idempotency_key("run-1", 3, "charge", {"amount": 100, "employee": "ada"})
    b = idempotency_key("run-1", 3, "charge", {"employee": "ada", "amount": 100})
    assert a == b


def test_key_is_stable_across_nested_ordering():
    a = idempotency_key("r", 0, "t", {"outer": {"x": 1, "y": [{"a": 1, "b": 2}]}})
    b = idempotency_key("r", 0, "t", {"outer": {"y": [{"b": 2, "a": 1}], "x": 1}})
    assert a == b


def test_same_call_at_different_positions_gets_different_keys():
    """
    Deduplication must apply to retries of one call, never to two distinct calls that happen
    to look alike. An agent charging the same amount twice on purpose is legitimate.
    """
    first = idempotency_key("run-1", 3, "charge", {"amount": 100})
    second = idempotency_key("run-1", 7, "charge", {"amount": 100})
    assert first != second


def test_different_runs_get_different_keys():
    a = idempotency_key("run-1", 3, "charge", {"amount": 100})
    b = idempotency_key("run-2", 3, "charge", {"amount": 100})
    assert a != b


def test_int_and_float_do_not_collide():
    assert idempotency_key("r", 0, "t", {"n": 1}) != idempotency_key("r", 0, "t", {"n": 1.5})


def test_nan_is_rejected_rather_than_hashed():
    """NaN != NaN, so a key containing one would never match itself and dedup would break."""
    with pytest.raises(ValueError, match="NaN"):
        canonical_json({"x": float("nan")})


def test_step_hash_distinguishes_type_and_name():
    payload = {"a": 1}
    assert hash_payload("tool_call", "charge", payload) != hash_payload("model_call", "charge", payload)
    assert hash_payload("tool_call", "charge", payload) != hash_payload("tool_call", "refund", payload)


json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(),
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(), children, max_size=4),
    max_leaves=12,
)


@given(payload=json_values)
@settings(max_examples=150, deadline=None)
def test_key_derivation_is_deterministic(payload):
    """Same input, same key — every time, in every process."""
    assert idempotency_key("r", 1, "tool", payload) == idempotency_key("r", 1, "tool", payload)
