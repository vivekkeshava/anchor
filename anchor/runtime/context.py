"""
The step wrapper: the single place where execution and replay diverge.

Everything an agent does that could vary between the original run and a replay has to pass
through here, because the journal is what makes replay deterministic — not the agent, and
certainly not the model.
"""

from __future__ import annotations

import random as _random
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID, uuid4

from anchor.effects.barrier import EffectBarrier
from anchor.errors import NondeterminismError, Suspended
from anchor.hashing import hash_payload
from anchor.journal.repo import JournalRepo


class StepContext:
    """
    Handed to an agent for one attempt at one run.

    Step positions come from a counter that advances once per wrapped call. That makes the
    mapping from code position to journal position deterministic *given the same code path*
    — and the input-hash check verifies that assumption on every replayed step rather than
    trusting it.
    """

    def __init__(
        self,
        run_id: str,
        repo: JournalRepo,
        barrier: EffectBarrier,
        fencing_token: int,
        attempt: int,
    ) -> None:
        self.run_id = run_id
        self.attempt = attempt
        self._repo = repo
        self._barrier = barrier
        self._token = fencing_token
        self._seq = 0
        # Observability for tests and the chaos harness: how much this attempt actually did
        # versus how much it read back from the journal.
        self.executed_steps = 0
        self.replayed_steps = 0

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    async def _step(
        self,
        step_type: str,
        name: str,
        payload: Any,
        fn: Callable[[int], Awaitable[Any]],
    ) -> Any:
        seq = self._next_seq()
        expected_hash = hash_payload(step_type, name, payload)
        journaled = await self._repo.get_step(self.run_id, seq)

        if journaled is not None and journaled.status == "completed":
            if journaled.input_hash != expected_hash:
                # The code asking for this position is not the code that recorded it.
                raise NondeterminismError(self.run_id, seq, journaled.input_hash, expected_hash)
            self.replayed_steps += 1
            return journaled.output

        # Either never attempted, or attempted and left 'started' by a crash. Both mean this
        # attempt has to actually execute it.
        await self._repo.begin_step(
            self.run_id, seq, step_type, name, expected_hash, payload, self._token
        )
        result = await fn(seq)
        await self._repo.complete_step(self.run_id, seq, result, self._token)
        self.executed_steps += 1
        return result

    # -- the calls an agent makes ---------------------------------------------------------

    async def call_model(self, name: str, prompt: Any, fn: Callable[[], Awaitable[Any]]) -> Any:
        """
        Journal a model call.

        On replay the model is never invoked — its output is read from the journal. This is
        why a non-deterministic model does not make replay non-deterministic, and it is the
        part of the design people usually expect to be impossible.
        """
        return await self._step("model_call", name, prompt, lambda _seq: fn())

    async def call_tool(
        self,
        name: str,
        args: Any,
        fn: Callable[[str], Awaitable[Any]],
        retry_safe: bool = False,
    ) -> Any:
        """
        Journal a tool call and route its execution through the effect barrier.

        Two layers, because they answer different questions. The journal answers "has this
        run already got a result for this position?"; the barrier answers "did this side
        effect already reach the outside world?". A crash between the effect landing and the
        journal write makes the first say no while the second says yes, and only the barrier
        can stop the tool running twice.
        """

        async def execute(seq: int) -> Any:
            return await self._barrier.execute(
                run_id=self.run_id,
                step_seq=seq,
                tool_name=name,
                args=args,
                fn=fn,
                retry_safe=retry_safe,
            )

        return await self._step("tool_call", name, args, execute)

    async def suspend(self, name: str, payload: Any = None) -> Any:
        """
        Park the run until :meth:`JournalRepo.resume` supplies a value.

        On the first pass this raises :class:`Suspended`, the executor releases the lease,
        and the worker moves on holding nothing. When the run is resumed, replay reaches this
        same position, finds the journaled payload, and returns it as a normal value.
        """
        seq = self._next_seq()
        expected_hash = hash_payload("suspend", name, payload)
        journaled = await self._repo.get_step(self.run_id, seq)

        if journaled is not None and journaled.status == "completed":
            if journaled.input_hash != expected_hash:
                raise NondeterminismError(self.run_id, seq, journaled.input_hash, expected_hash)
            self.replayed_steps += 1
            return journaled.output

        await self._repo.begin_step(
            self.run_id, seq, "suspend", name, expected_hash, payload, self._token
        )
        raise Suspended(self.run_id, seq, name)

    # -- journaled sources of non-determinism ---------------------------------------------
    #
    # Calling datetime.now() or uuid4() directly inside an agent silently breaks replay: the
    # replayed run takes a different value than the original and can diverge from the
    # journal it is supposed to be following. These record their value on first execution and
    # return the recorded one thereafter.

    async def now(self, name: str = "now") -> datetime:
        value = await self._step(
            "now", name, None, lambda _seq: _immediate(datetime.now(timezone.utc).isoformat())
        )
        return datetime.fromisoformat(value)

    async def uuid(self, name: str = "uuid") -> UUID:
        value = await self._step("uuid", name, None, lambda _seq: _immediate(str(uuid4())))
        return UUID(value)

    async def random(self, name: str = "random") -> float:
        return await self._step("random", name, None, lambda _seq: _immediate(_random.random()))

    async def sleep_until(self, when: datetime, name: str = "timer") -> None:
        """Suspend until a wall-clock instant, holding no process state while waiting."""
        await self._repo.suspend(self.run_id, self._token, resume_after=when)
        raise Suspended(self.run_id, self._seq, name)


async def _immediate(value: Any) -> Any:
    return value
