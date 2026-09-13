"""
The effect barrier. These tests are about the window between a side effect landing and the
journal recording it — the reason the guarantee is *effectively*-once.
"""

from __future__ import annotations

import pytest

from anchor.effects.barrier import EffectBarrier
from anchor.errors import AmbiguousEffectError
from anchor.hashing import idempotency_key


class Tool:
    def __init__(self) -> None:
        self.calls = 0
        self.keys: list[str] = []

    async def __call__(self, key: str) -> dict:
        self.calls += 1
        self.keys.append(key)
        return {"call": self.calls}


async def test_second_attempt_returns_the_first_result_without_re_running(repo):
    barrier = EffectBarrier(repo)
    tool = Tool()
    run_id = await repo.enqueue("demo", {})

    first = await barrier.execute(run_id, 0, "charge", {"amount": 10}, tool)
    second = await barrier.execute(run_id, 0, "charge", {"amount": 10}, tool)

    assert tool.calls == 1
    assert first == second == {"call": 1}


async def test_idempotency_key_is_handed_to_the_tool(repo):
    """
    The tool receives the key so it can forward it to a downstream API that supports one.
    That is what turns a retry_safe re-run from a hope into a guarantee.
    """
    barrier = EffectBarrier(repo)
    tool = Tool()
    run_id = await repo.enqueue("demo", {})

    await barrier.execute(run_id, 4, "charge", {"amount": 10}, tool)

    assert tool.keys == [idempotency_key(run_id, 4, "charge", {"amount": 10})]


async def test_unsafe_tool_left_pending_by_a_crash_refuses_to_re_run(repo):
    """
    The crash window: the row was claimed, the effect may or may not have landed, and the
    tool moves money. Anchor must stop rather than guess.
    """
    barrier = EffectBarrier(repo)
    tool = Tool()
    run_id = await repo.enqueue("demo", {})
    key = idempotency_key(run_id, 0, "charge_card", {"amount": 10})

    # A previous attempt claimed the key and then died before completing it.
    assert await repo.claim_effect(key, run_id, 0, "charge_card") is None

    with pytest.raises(AmbiguousEffectError) as excinfo:
        await barrier.execute(run_id, 0, "charge_card", {"amount": 10}, tool, retry_safe=False)

    assert tool.calls == 0
    assert excinfo.value.tool_name == "charge_card"


async def test_retry_safe_tool_left_pending_by_a_crash_does_re_run(repo):
    """The other honest policy: safe to repeat, so repeat it and finish the run."""
    barrier = EffectBarrier(repo)
    tool = Tool()
    run_id = await repo.enqueue("demo", {})
    key = idempotency_key(run_id, 0, "send_receipt", {"to": "ada"})

    assert await repo.claim_effect(key, run_id, 0, "send_receipt") is None

    result = await barrier.execute(run_id, 0, "send_receipt", {"to": "ada"}, tool, retry_safe=True)

    assert tool.calls == 1
    assert result == {"call": 1}


async def test_two_distinct_calls_are_not_deduplicated(repo):
    """
    An agent legitimately charging the same amount twice must charge twice. Dedup applies to
    retries of one call, never to two different calls that look alike.
    """
    barrier = EffectBarrier(repo)
    tool = Tool()
    run_id = await repo.enqueue("demo", {})

    await barrier.execute(run_id, 0, "charge", {"amount": 10}, tool)
    await barrier.execute(run_id, 5, "charge", {"amount": 10}, tool)

    assert tool.calls == 2


async def test_concurrent_attempts_run_the_effect_once(repo):
    """
    Two workers racing on the same effect: the primary key arbitrates, not a lock. Exactly
    one runs the tool; the loser reads the winner's result.
    """
    import asyncio

    barrier = EffectBarrier(repo)
    run_id = await repo.enqueue("demo", {})

    started = asyncio.Event()
    calls = 0

    async def slow_tool(key: str) -> dict:
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.sleep(0.1)
        return {"ok": True}

    async def attempt():
        try:
            return await barrier.execute(
                run_id, 0, "charge", {"a": 1}, slow_tool, retry_safe=False
            )
        except AmbiguousEffectError:
            # The loser arrived while the winner was still mid-flight. Refusing is correct
            # for an unsafe tool: from its side the outcome is genuinely unknown.
            return "ambiguous"

    results = await asyncio.gather(attempt(), attempt())

    assert calls == 1
    assert results.count("ambiguous") <= 1
