"""Lease claiming, fencing, and suspend/resume."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from anchor.errors import FencedError

LIVE = timedelta(seconds=30)
LAPSED = timedelta(seconds=-1)


async def test_concurrent_workers_claim_each_run_exactly_once(repo):
    """
    SKIP LOCKED means N workers take N different rows instead of queueing behind the same
    one. If claiming were not exclusive, two workers would execute the same run in parallel.
    """
    run_ids = {await repo.enqueue("demo", {"i": i}) for i in range(40)}

    async def worker(name: str) -> list[str]:
        claimed = []
        while True:
            run = await repo.claim(name, LIVE)
            if run is None:
                return claimed
            claimed.append(run.run_id)

    results = await asyncio.gather(*(worker(f"w{i}") for i in range(6)))
    all_claimed = [run_id for batch in results for run_id in batch]

    assert sorted(all_claimed) == sorted(run_ids)
    assert len(all_claimed) == len(set(all_claimed))  # no run claimed twice


async def test_fencing_token_increments_on_every_claim(repo):
    await repo.enqueue("demo", {})
    first = await repo.claim("w1", LAPSED)
    second = await repo.claim("w2", LAPSED)
    assert second.fencing_token == first.fencing_token + 1


async def test_zombie_worker_writes_are_rejected(repo):
    """
    The split-brain case. w1 stalls past its lease; w2 claims the run. w1 wakes up still
    believing it owns the run. Its writes must bounce — otherwise it corrupts the journal of
    a run w2 is actively executing.
    """
    run_id = await repo.enqueue("demo", {})
    stalled = await repo.claim("w1", LAPSED)
    new_owner = await repo.claim("w2", LIVE)

    assert new_owner.fencing_token > stalled.fencing_token

    with pytest.raises(FencedError):
        await repo.begin_step(run_id, 0, "model_call", "x", "hash", {}, stalled.fencing_token)

    with pytest.raises(FencedError):
        await repo.finish(run_id, stalled.fencing_token, "completed", {"bogus": True})

    # The rightful owner is unaffected.
    await repo.begin_step(run_id, 0, "model_call", "x", "hash", {}, new_owner.fencing_token)
    await repo.complete_step(run_id, 0, {"ok": True}, new_owner.fencing_token)
    assert (await repo.get_step(run_id, 0)).output == {"ok": True}


async def test_heartbeat_reports_being_fenced(repo):
    """A failed heartbeat is not a transient error to retry — it means stop."""
    run_id = await repo.enqueue("demo", {})
    stalled = await repo.claim("w1", LAPSED)

    # Heartbeating with an already-lapsed lease succeeds — w1 is still the owner — but it
    # does not protect the run, which is precisely how a stalled worker gets overtaken.
    assert await repo.heartbeat(run_id, "w1", stalled.fencing_token, LAPSED) is True

    new_owner = await repo.claim("w2", LIVE)
    assert new_owner is not None

    assert await repo.heartbeat(run_id, "w1", stalled.fencing_token, LIVE) is False


async def test_suspended_run_is_not_claimable_until_resumed(repo):
    run_id = await repo.enqueue("demo", {})
    claimed = await repo.claim("w1", LIVE)
    await repo.begin_step(run_id, 0, "suspend", "await_human", "hash", None, claimed.fencing_token)
    await repo.suspend(run_id, claimed.fencing_token)

    assert await repo.claim("w2", LIVE) is None

    assert await repo.resume(run_id, {"approved": True}) is True

    resumed = await repo.claim("w2", LIVE)
    assert resumed is not None and resumed.run_id == run_id
    # The resume payload is journaled as the suspend step's output, so replay returns it.
    assert (await repo.get_step(run_id, 0)).output == {"approved": True}


async def test_resume_on_a_run_that_is_not_suspended_is_a_no_op(repo):
    run_id = await repo.enqueue("demo", {})
    assert await repo.resume(run_id, {"approved": True}) is False
