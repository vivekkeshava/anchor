"""
End-to-end through the Worker: a full agent run, suspend/resume across workers, the
needs_review path, and the poison-run bound.
"""

from __future__ import annotations

import asyncpg
import pytest
import pytest_asyncio

from anchor.hashing import idempotency_key
from anchor.worker import Worker
from examples.expense_agent import REGISTRY, bind_pool


@pytest_asyncio.fixture
async def bound(repo, pool):
    """The demo agent's tools reach the database through a module-level pool."""
    bind_pool(pool)
    return repo


async def _ledger(pool, run_id: str) -> list[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT logical_key FROM side_effect_ledger WHERE run_id = $1 ORDER BY id",
            __import__("uuid").UUID(run_id),
        )
    return [r["logical_key"] for r in rows]


async def test_full_run_completes_and_performs_each_effect_once(bound, pool):
    run_id = await bound.enqueue("expense", {"amount": 200, "employee": "ada"})
    worker = Worker(bound, REGISTRY, worker_id="w1", lease_seconds=30)

    assert await worker.run_once() is True

    run = await bound.get_run(run_id)
    assert run.status == "completed"
    assert run.result["approved"] is True
    # Under the policy limit, so the card is charged and a receipt is sent — once each.
    assert sorted(await _ledger(pool, run_id)) == ["charge_card", "send_receipt"]


async def test_run_over_policy_limit_skips_the_charge(bound, pool):
    run_id = await bound.enqueue("expense", {"amount": 900, "employee": "ada"})
    worker = Worker(bound, REGISTRY, worker_id="w1", lease_seconds=30)
    await worker.run_once()

    run = await bound.get_run(run_id)
    assert run.status == "completed"
    assert run.result["approved"] is False
    assert await _ledger(pool, run_id) == ["send_receipt"]


async def test_queue_drains_and_then_reports_empty(bound):
    for i in range(3):
        await bound.enqueue("expense", {"amount": 100, "employee": f"e{i}"})
    worker = Worker(bound, REGISTRY, worker_id="w1", lease_seconds=30)

    assert [await worker.run_once() for _ in range(3)] == [True, True, True]
    assert await worker.run_once() is False


async def test_suspend_and_resume_across_two_workers(bound, pool):
    """
    The clearest demonstration that replay works: the worker that started the run is gone
    before it finishes, and the run holds no process state while it waits.
    """
    run_id = await bound.enqueue("approval", {"doc": "expense-report"})

    starter = Worker(bound, REGISTRY, worker_id="starter", lease_seconds=30)
    await starter.run_once()

    run = await bound.get_run(run_id)
    assert run.status == "suspended"
    # The lease was released: the run occupies no worker while parked.
    assert await bound.claim("someone-else", __import__("datetime").timedelta(seconds=5)) is None

    assert await bound.resume(run_id, {"approved": True}) is True

    # A different worker object — standing in for a different process on a different host.
    finisher = Worker(bound, REGISTRY, worker_id="finisher", lease_seconds=30)
    assert await finisher.run_once() is True

    run = await bound.get_run(run_id)
    assert run.status == "completed"
    assert run.result["approved"] is True
    # The draft nonce survived the suspension, so the second worker replayed rather than
    # re-generating it.
    steps = await bound.list_steps(run_id)
    assert run.result["draft_nonce"] == steps[0].output["nonce"]


async def test_unsafe_effect_in_the_crash_window_halts_for_review(bound, pool):
    """
    Pre-claim the charge_card effect without completing it, exactly as a crash inside the
    barrier window would leave it. The run must stop rather than risk charging twice.
    """
    run_id = await bound.enqueue("expense", {"amount": 200, "employee": "ada"})
    key = idempotency_key(run_id, 3, "charge_card", {"amount": 200, "employee": "ada"})
    assert await bound.claim_effect(key, run_id, 3, "charge_card") is None

    worker = Worker(bound, REGISTRY, worker_id="w1", lease_seconds=30)
    await worker.run_once()

    run = await bound.get_run(run_id)
    assert run.status == "needs_review"
    assert "charge_card" in run.error
    # Crucially: it did not charge the card a second time.
    assert "charge_card" not in await _ledger(pool, run_id)


async def test_poison_run_is_failed_rather_than_retried_forever(bound):
    """
    A run that cannot be finalised must not occupy a worker slot indefinitely. Without this
    bound a single bad run is reclaimed the instant its lease lapses, forever.
    """
    run_id = await bound.enqueue("expense", {"amount": 200, "employee": "ada"})
    async with bound._pool.acquire() as conn:  # noqa: SLF001 - deliberate test setup
        await conn.execute(
            "UPDATE runs SET attempts = 999 WHERE run_id = $1", __import__("uuid").UUID(run_id)
        )

    worker = Worker(bound, REGISTRY, worker_id="w1", lease_seconds=30, max_attempts=20)
    assert await worker.run_once() is True

    run = await bound.get_run(run_id)
    assert run.status == "failed"
    assert "exceeded max attempts" in run.error


async def test_retry_safe_tool_with_an_idempotent_downstream_effects_once(bound, pool):
    """
    The contract behind retry_safe: anchor WILL re-run the tool after a crash in the barrier
    window, so the downstream must deduplicate on the key it was handed. A tool that declares
    retry_safe while ignoring its key produces duplicate effects — and that is a bug in the
    tool, which anchor cannot detect for it.
    """
    run_id = await bound.enqueue("expense", {"amount": 900, "employee": "ada"})
    # Over the limit, so send_receipt is step 3. Pre-claim it as a crash inside the window
    # would, forcing the retry_safe re-run path.
    key = idempotency_key(run_id, 3, "send_receipt", {"employee": "ada", "approved": False})
    assert await bound.claim_effect(key, run_id, 3, "send_receipt") is None

    worker = Worker(bound, REGISTRY, worker_id="w1", lease_seconds=30)
    await worker.run_once()

    run = await bound.get_run(run_id)
    assert run.status == "completed"
    # Re-run happened, but the downstream honoured the key: exactly one receipt.
    assert await _ledger(pool, run_id) == ["send_receipt"]
