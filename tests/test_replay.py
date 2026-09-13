"""
Replay: a second attempt must read completed steps from the journal instead of re-executing
them, even though the model is non-deterministic.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from anchor.effects.barrier import EffectBarrier
from anchor.errors import NondeterminismError
from anchor.journal.repo import JournalRepo
from anchor.runtime.context import StepContext

LIVE = timedelta(seconds=30)
# A claim whose lease has already lapsed — i.e. the worker that held it died. Using this for
# the first claim lets the next claim succeed immediately, which is the crash scenario these
# tests are about. With a live lease the second claim correctly returns None.
LAPSED = timedelta(seconds=-1)


class CallCounter:
    """Stands in for a non-deterministic model: a different answer on every real call."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> dict:
        self.calls += 1
        return {"value": f"result-{self.calls}"}


def _context(repo: JournalRepo, run_id: str, token: int) -> StepContext:
    return StepContext(run_id, repo, EffectBarrier(repo), fencing_token=token, attempt=1)


async def _crashed_attempt(repo: JournalRepo, worker: str, run_id: str) -> StepContext:
    claimed = await repo.claim(worker, LAPSED)
    assert claimed is not None and claimed.run_id == run_id
    return _context(repo, run_id, claimed.fencing_token)


async def test_completed_steps_are_not_re_executed(repo):
    run_id = await repo.enqueue("demo", {"x": 1})
    model = CallCounter()

    ctx = await _crashed_attempt(repo, "w1", run_id)
    first = await ctx.call_model("classify", {"a": 1}, model)
    assert model.calls == 1

    ctx2 = await _crashed_attempt(repo, "w2", run_id)
    second = await ctx2.call_model("classify", {"a": 1}, model)

    # The model was NOT called again, and the replayed value is identical to the first.
    assert model.calls == 1
    assert second == first
    assert (ctx2.replayed_steps, ctx2.executed_steps) == (1, 0)


async def test_replay_resumes_at_the_first_incomplete_step(repo):
    run_id = await repo.enqueue("demo", {})
    model = CallCounter()

    ctx = await _crashed_attempt(repo, "w1", run_id)
    await ctx.call_model("one", {}, model)
    await ctx.call_model("two", {}, model)
    assert model.calls == 2

    ctx2 = await _crashed_attempt(repo, "w2", run_id)
    await ctx2.call_model("one", {}, model)
    await ctx2.call_model("two", {}, model)
    await ctx2.call_model("three", {}, model)

    # Only the genuinely new third step executed.
    assert model.calls == 3
    assert (ctx2.replayed_steps, ctx2.executed_steps) == (2, 1)


async def test_changed_code_raises_rather_than_diverging_silently(repo):
    """
    A run in flight when the agent is redeployed must stop, not splice the new code path onto
    the old history.
    """
    run_id = await repo.enqueue("demo", {})
    model = CallCounter()

    ctx = await _crashed_attempt(repo, "w1", run_id)
    await ctx.call_model("classify", {"prompt": "v1"}, model)

    ctx2 = await _crashed_attempt(repo, "w2", run_id)
    with pytest.raises(NondeterminismError) as excinfo:
        await ctx2.call_model("classify", {"prompt": "v2 — code changed"}, model)

    assert excinfo.value.step_seq == 0


async def test_journaled_clock_uuid_and_random_are_stable_across_replay(repo):
    """
    datetime.now() and uuid4() called directly in an agent silently break replay. The
    journaled helpers must hand back the recorded value on the second pass.
    """
    run_id = await repo.enqueue("demo", {})

    ctx = await _crashed_attempt(repo, "w1", run_id)
    first_now = await ctx.now()
    first_uuid = await ctx.uuid()
    first_random = await ctx.random()

    ctx2 = await _crashed_attempt(repo, "w2", run_id)
    assert await ctx2.now() == first_now
    assert await ctx2.uuid() == first_uuid
    assert await ctx2.random() == first_random


async def test_step_left_started_by_a_crash_is_re_executed(repo):
    """A step that began but never completed has no journaled output, so it must run again."""
    run_id = await repo.enqueue("demo", {})
    model = CallCounter()

    claimed = await repo.claim("w1", LIVE)
    ctx = _context(repo, run_id, claimed.fencing_token)

    # Simulate dying between begin_step and complete_step.
    await repo.begin_step(run_id, 0, "model_call", "classify", "stale-hash", {}, claimed.fencing_token)
    assert (await repo.get_step(run_id, 0)).status == "started"

    result = await ctx.call_model("classify", {}, model)

    assert model.calls == 1
    assert result["value"] == "result-1"
    assert (await repo.get_step(run_id, 0)).status == "completed"


async def test_live_lease_blocks_a_second_claim(repo):
    """The flip side: while a lease is live, nobody else may take the run."""
    await repo.enqueue("demo", {})
    first = await repo.claim("w1", LIVE)
    assert first is not None
    assert await repo.claim("w2", LIVE) is None
