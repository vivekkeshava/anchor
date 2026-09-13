"""Runs one attempt at one run: resolve the agent, drive it, record the outcome."""

from __future__ import annotations

import logging
import traceback
from typing import Any

from anchor.effects.barrier import EffectBarrier
from anchor.errors import AmbiguousEffectError, FencedError, Suspended
from anchor.journal.repo import ClaimedRun, JournalRepo
from anchor.runtime.context import StepContext
from anchor.runtime.registry import AgentRegistry

log = logging.getLogger(__name__)


class Executor:
    def __init__(self, repo: JournalRepo, registry: AgentRegistry) -> None:
        self._repo = repo
        self._registry = registry
        self._barrier = EffectBarrier(repo)

    async def execute(self, claimed: ClaimedRun) -> StepContext:
        """
        Drive one attempt to a terminal state.

        Every outcome ends with the run in a durable state, because a run left `running`
        with no live worker is indistinguishable from one whose worker is merely slow — and
        the lease is what resolves that, not this method.
        """
        ctx = StepContext(
            run_id=claimed.run_id,
            repo=self._repo,
            barrier=self._barrier,
            fencing_token=claimed.fencing_token,
            attempt=claimed.attempts,
        )
        agent_fn = self._registry.get(claimed.agent)

        try:
            result = await agent_fn(claimed.input, ctx)
        except Suspended:
            # Not a failure: the run parked itself. suspend() already journaled the step.
            await self._repo.suspend(claimed.run_id, claimed.fencing_token)
            log.info("run %s suspended", claimed.run_id)
            return ctx
        except AmbiguousEffectError as exc:
            # Not a crash: a side effect may have landed and the tool is not safe to repeat.
            # The run stops in its own terminal state so a human can reconcile it against the
            # downstream system. Retrying would risk doing it twice; marking it failed would
            # bury it among ordinary errors.
            log.warning("run %s needs review: %s", claimed.run_id, exc)
            await self._safe_finish(claimed, "needs_review", error=str(exc))
            return ctx
        except FencedError:
            # Another worker owns this run now. Write nothing further — that is the entire
            # point of the fence — and let the rightful owner finish it.
            log.warning("run %s: lease lost mid-execution, abandoning attempt", claimed.run_id)
            return ctx
        except Exception as exc:  # noqa: BLE001 - terminal states must be recorded, not raised
            log.exception("run %s failed", claimed.run_id)
            await self._safe_finish(
                claimed, "failed", error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            )
            return ctx

        await self._safe_finish(claimed, "completed", result=result)
        return ctx

    async def _safe_finish(
        self, claimed: ClaimedRun, status: str, result: Any = None, error: str | None = None
    ) -> None:
        try:
            await self._repo.finish(claimed.run_id, claimed.fencing_token, status, result, error)
        except FencedError:
            # Lost the lease between the last step and the finish write. The new owner will
            # replay the journal and reach the same conclusion, so dropping this is correct.
            log.warning("run %s: fenced while recording %s", claimed.run_id, status)
