"""
The effect barrier: effectively-once side effects on an at-least-once substrate.

The journal alone cannot answer "did this tool already run?", because the tool's effect
lands in someone else's system. The barrier keeps a claim row per logical call, keyed by
content, so the question can be answered after any crash.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from anchor.errors import AmbiguousEffectError
from anchor.hashing import idempotency_key
from anchor.journal.repo import JournalRepo


class EffectBarrier:
    """
    Wraps a side-effecting call in claim → execute → complete.

    The sequence is:

    1. Claim the key. If the claim succeeds, nobody has attempted this call and we run it.
    2. If the claim fails, someone got there first:
       - `completed` → return their result. The effect happened exactly once and this
         attempt must not repeat it.
       - `pending` → a previous attempt claimed the key and then died. Whether the effect
         reached the downstream system is genuinely unknown.
    3. After the tool returns, mark the row completed with its result.

    **The window.** Between the tool's effect landing and step 3 committing, a crash leaves a
    `pending` row that anchor cannot interpret. There is no way to close this window without
    a distributed transaction across anchor's database and the downstream system, which for
    a payment API or an SMTP server does not exist. So it is handled by policy rather than
    pretended away:

    - `retry_safe=True` — the downstream deduplicates on the idempotency key we pass it, or
      the operation is naturally idempotent. Re-running is safe, so anchor re-runs.
    - `retry_safe=False` — a charge, an email. Anchor stops and raises
      :class:`AmbiguousEffectError` for a human to resolve.

    This is why the guarantee is *effectively*-once rather than exactly-once, and why every
    tool must declare which kind it is. See docs/guarantees.md.
    """

    def __init__(self, repo: JournalRepo) -> None:
        self._repo = repo

    async def execute(
        self,
        run_id: str,
        step_seq: int,
        tool_name: str,
        args: Any,
        fn: Callable[[str], Awaitable[Any]],
        retry_safe: bool = False,
    ) -> Any:
        """
        :param fn: receives the idempotency key, so it can forward it to a downstream API
                   that supports one (Stripe-style). Passing it through is what turns a
                   `retry_safe` re-run from a hope into a guarantee.
        """
        key = idempotency_key(run_id, step_seq, tool_name, args)

        existing = await self._repo.claim_effect(key, run_id, step_seq, tool_name)

        if existing is None:
            result = await fn(key)
            await self._repo.complete_effect(key, result)
            return result

        if existing["status"] == "completed":
            return existing["result"]

        # Claimed but never completed: the previous attempt died inside the window.
        if not retry_safe:
            raise AmbiguousEffectError(key, tool_name, run_id, step_seq)

        await self._repo.bump_effect_attempt(key)
        result = await fn(key)
        await self._repo.complete_effect(key, result)
        return result
