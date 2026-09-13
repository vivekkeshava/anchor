"""
Demo agent: an expense approval flow with tools that genuinely have side effects.

The tools write to `side_effect_ledger`, which stands in for the downstream system anchor
cannot roll back — a payment processor, a mail server. The chaos harness counts rows there
to detect duplicates, and it matters that it counts them *there* rather than in anchor's own
effects table: verifying a system's correctness with that system's own bookkeeping proves
nothing.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
from typing import Any
from uuid import UUID

import asyncpg

from anchor.runtime.registry import AgentRegistry

REGISTRY = AgentRegistry()

# Set by the worker/harness process. Kept module level so tools can reach the pool without
# threading it through the agent signature.
_POOL: asyncpg.Pool | None = None


def bind_pool(pool: asyncpg.Pool) -> None:
    global _POOL
    _POOL = pool


def _pool() -> asyncpg.Pool:
    if _POOL is None:
        raise RuntimeError("examples.expense_agent.bind_pool() was never called")
    return _POOL


async def _record_effect(
    run_id: str, logical_key: str, payload: Any, dedupe_key: str | None = None
) -> None:
    """
    The irreversible part. Once this row exists, the effect has happened.

    There is no unique constraint on (run_id, logical_key) — see schema.sql. Duplicates have
    to be *possible* for the chaos harness to be able to prove they do not occur.

    `dedupe_key` models a downstream API that supports idempotency keys (Stripe-style): the
    write is skipped if this key has already been recorded. Only a tool whose downstream does
    this may honestly declare ``retry_safe=True``.

    **anchor cannot check that claim.** `retry_safe=True` tells the barrier "re-running this
    is safe"; if the downstream does not actually deduplicate, the barrier will faithfully
    re-run it and the effect happens twice. Declaring retry_safe on a tool that ignores its
    key is the one way to get duplicate effects out of this system, and it is a mistake in
    the tool, not in the runtime.
    """
    async with _pool().acquire() as conn:
        if dedupe_key is None:
            await conn.execute(
                "INSERT INTO side_effect_ledger (run_id, logical_key, payload) VALUES ($1, $2, $3)",
                UUID(run_id),
                logical_key,
                json.dumps(payload),
            )
            return
        # A real idempotent API does this atomically on its side. Modelled here with a
        # conditional insert; the downstream's own atomicity is not anchor's concern.
        await conn.execute(
            """
            INSERT INTO side_effect_ledger (run_id, logical_key, payload)
            SELECT $1, $2, $3
             WHERE NOT EXISTS (
                   SELECT 1 FROM side_effect_ledger
                    WHERE run_id = $1 AND payload ->> 'key' = $4
             )
            """,
            UUID(run_id),
            logical_key,
            json.dumps(payload),
            dedupe_key,
        )


async def _log_model_call(run_id: str, step_seq: int) -> None:
    async with _pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO model_call_log (run_id, step_seq) VALUES ($1, $2)",
            UUID(run_id),
            step_seq,
        )


class FlakyModel:
    """
    Stands in for an LLM.

    Returns a random value on every call *on purpose*. If replay were re-invoking the model
    instead of reading the journal, the run's outputs would change between attempts and the
    chaos harness assertions would catch it. A deterministic fake would hide exactly the bug
    this project exists to prevent.
    """

    def __init__(self, run_id: str, latency: float = 0.01) -> None:
        self._run_id = run_id
        self._latency = latency
        self.calls = 0

    async def complete(self, step_seq: int, prompt: str) -> dict[str, Any]:
        self.calls += 1
        await _log_model_call(self._run_id, step_seq)
        await asyncio.sleep(self._latency)
        return {"prompt": prompt, "nonce": random.randint(0, 10**9)}


async def expense_agent(payload: Any, ctx: Any) -> dict[str, Any]:
    """
    Six journaled steps, two of them irreversible.

    `charge_card` is not retry-safe: if it may have run, anchor must stop rather than risk
    charging twice. `send_receipt` is, because the ledger insert is keyed on the idempotency
    key we pass through. The distinction is declared per tool, not inferred.
    """
    model = FlakyModel(ctx.run_id, latency=float(os.environ.get("ANCHOR_MODEL_LATENCY", "0.01")))
    amount = payload["amount"]
    employee = payload["employee"]

    classification = await ctx.call_model(
        "classify", {"amount": amount, "employee": employee},
        lambda: model.complete(0, f"classify {amount}"),
    )

    policy = await ctx.call_tool(
        "fetch_policy",
        {"employee": employee},
        lambda key: _fetch_policy(employee),
        retry_safe=True,  # read-only
    )

    decision = await ctx.call_model(
        "decide", {"classification": classification["nonce"], "policy": policy},
        lambda: model.complete(2, "decide"),
    )

    approved = amount <= policy["limit"]

    if approved:
        await ctx.call_tool(
            "charge_card",
            {"amount": amount, "employee": employee},
            lambda key: _charge_card(ctx.run_id, amount, employee, key),
            retry_safe=False,  # money moves; ambiguity must reach a human
        )

    await ctx.call_tool(
        "send_receipt",
        {"employee": employee, "approved": approved},
        lambda key: _send_receipt(ctx.run_id, employee, approved, key),
        retry_safe=True,  # downstream dedupes on the key we hand it
    )

    summary = await ctx.call_model(
        "summarise", {"approved": approved}, lambda: model.complete(5, "summarise")
    )

    return {
        "approved": approved,
        "amount": amount,
        "decision_nonce": decision["nonce"],
        "summary_nonce": summary["nonce"],
    }


REGISTRY.register("expense", expense_agent)


async def _fetch_policy(employee: str) -> dict[str, Any]:
    await asyncio.sleep(0.005)
    return {"employee": employee, "limit": 500}


async def _charge_card(run_id: str, amount: int, employee: str, key: str) -> dict[str, Any]:
    """
    Deliberately does NOT deduplicate: this models a downstream with no idempotency support,
    which is why the call site declares retry_safe=False. If a crash leaves the outcome
    unknown, nothing downstream can save us and the run must stop for review.
    """
    await asyncio.sleep(0.005)
    await _record_effect(run_id, "charge_card", {"amount": amount, "employee": employee, "key": key})
    return {"charged": amount, "idempotency_key": key}


async def _send_receipt(run_id: str, employee: str, approved: bool, key: str) -> dict[str, Any]:
    """Declared retry_safe, so the downstream MUST honour the key. It does, via dedupe_key."""
    await asyncio.sleep(0.005)
    await _record_effect(
        run_id,
        "send_receipt",
        {"employee": employee, "approved": approved, "key": key},
        dedupe_key=key,
    )
    return {"sent": True, "idempotency_key": key}


async def approval_agent(payload: Any, ctx: Any) -> dict[str, Any]:
    """
    Second agent, for the suspend/resume path.

    The `suspend` here can sit for hours. The worker that started it exits; a different one
    finishes it. Nothing about that requires the two processes to share memory, which is the
    clearest demonstration that replay genuinely works.
    """
    model = FlakyModel(ctx.run_id)
    draft = await ctx.call_model("draft", payload, lambda: model.complete(0, "draft"))

    approval = await ctx.suspend("await_human_approval", {"draft_nonce": draft["nonce"]})

    await ctx.call_tool(
        "notify_outcome",
        {"approved": approval["approved"]},
        lambda key: _send_receipt(ctx.run_id, "approver", approval["approved"], key),
        retry_safe=True,
    )
    return {"approved": approval["approved"], "draft_nonce": draft["nonce"]}


REGISTRY.register("approval", approval_agent)
