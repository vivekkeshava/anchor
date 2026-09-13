"""
Content hashing for step inputs and idempotency keys.

Both the replay guard and the effect barrier depend on the same property: the same logical
call must produce the same string on every attempt, in every process, forever. That makes
JSON canonicalisation load-bearing rather than cosmetic — if key derivation is unstable,
the barrier silently stops deduplicating and every guarantee built on it evaporates.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


def _default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        # str() rather than float(): Decimal("0.10") and float 0.1 are different values and
        # must not collapse to the same key.
        return f"decimal:{value}"
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=repr)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"cannot canonicalise {type(value).__name__} for hashing")


def canonical_json(payload: Any) -> str:
    """
    Deterministic JSON.

    - `sort_keys` so dict insertion order cannot change the key.
    - No whitespace, so formatting cannot change it either.
    - `ensure_ascii` so the same string hashes identically regardless of the encoding the
      caller happened to hand us.
    - `allow_nan=False` because NaN != NaN, which would make a key that never matches itself.
    """
    if _contains_nan(payload):
        raise ValueError("NaN/Infinity cannot appear in a hashed payload: NaN never compares equal to itself")
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=_default,
    )


def _contains_nan(value: Any) -> bool:
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    if isinstance(value, dict):
        return any(_contains_nan(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_nan(v) for v in value)
    return False


def hash_payload(step_type: str, name: str, payload: Any) -> str:
    """Replay guard: identifies *what the agent asked for* at a journal position."""
    material = canonical_json({"type": step_type, "name": name, "payload": payload})
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def idempotency_key(run_id: str, step_seq: int, tool_name: str, args: Any) -> str:
    """
    Effect barrier key.

    Includes run_id and step_seq, so the same tool called with the same arguments twice in
    one run — a legitimate thing for an agent to do — gets two distinct keys and runs twice.
    Deduplication must apply to *retries of one call*, never to two different calls that
    happen to look alike.
    """
    material = canonical_json(
        {"run": run_id, "seq": step_seq, "tool": tool_name, "args": args}
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
