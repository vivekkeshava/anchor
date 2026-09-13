"""Postgres-backed journal. Every write that advances a run is fencing-guarded."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Sequence
from uuid import UUID, uuid4

import asyncpg

from anchor.errors import FencedError

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    agent: str
    status: str
    input: Any
    result: Any
    error: Optional[str]
    fencing_token: int
    attempts: int


@dataclass(frozen=True)
class StepRecord:
    run_id: str
    step_seq: int
    step_type: str
    name: str
    input_hash: str
    input: Any
    output: Any
    status: str
    fencing_token: int


@dataclass(frozen=True)
class ClaimedRun:
    run_id: str
    agent: str
    input: Any
    fencing_token: int
    attempts: int


def _loads(value: Optional[str]) -> Any:
    return None if value is None else json.loads(value)


class JournalRepo:
    """
    Data access for runs, steps and effects.

    Two rules hold throughout:

    1. Anything that advances a run takes a `fencing_token` and refuses the write if the run
       has since been claimed by someone else.
    2. Nothing here caches. The journal is the source of truth precisely because a worker is
       allowed to die at any instant, and in-memory state would not survive that.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # -- schema ---------------------------------------------------------------------------

    async def create_schema(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_PATH.read_text())

    async def truncate_all(self) -> None:
        """Test and chaos-harness helper; never called by the runtime."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "TRUNCATE runs, steps, effects, side_effect_ledger, model_call_log CASCADE"
            )

    # -- runs -----------------------------------------------------------------------------

    async def enqueue(self, agent: str, payload: Any, run_id: Optional[str] = None) -> str:
        run_id = run_id or str(uuid4())
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO runs (run_id, agent, status, input)
                VALUES ($1, $2, 'pending', $3)
                """,
                UUID(run_id),
                agent,
                json.dumps(payload),
            )
        return run_id

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT run_id, agent, status, input::text AS input, result::text AS result,
                       error, fencing_token, attempts
                FROM runs WHERE run_id = $1
                """,
                UUID(run_id),
            )
        if row is None:
            return None
        return RunRecord(
            run_id=str(row["run_id"]),
            agent=row["agent"],
            status=row["status"],
            input=_loads(row["input"]),
            result=_loads(row["result"]),
            error=row["error"],
            fencing_token=row["fencing_token"],
            attempts=row["attempts"],
        )

    async def counts_by_status(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT status, count(*) AS n FROM runs GROUP BY status")
        return {row["status"]: row["n"] for row in rows}

    # -- leases ---------------------------------------------------------------------------

    async def claim(self, worker_id: str, lease: timedelta) -> Optional[ClaimedRun]:
        """
        Take the oldest runnable run whose lease has lapsed.

        `FOR UPDATE SKIP LOCKED` is what makes this safe to run from N workers concurrently:
        each transaction locks a different row instead of queueing behind the same one, so
        claiming does not serialise as the worker pool grows.

        Incrementing `fencing_token` on every claim is what makes the previous owner
        detectable if it ever wakes up and tries to keep writing.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE runs
                   SET lease_owner      = $1,
                       lease_expires_at = now() + $2::interval,
                       fencing_token    = fencing_token + 1,
                       status           = 'running',
                       attempts         = attempts + 1,
                       updated_at       = now()
                 WHERE run_id = (
                     SELECT run_id FROM runs
                      WHERE status IN ('pending', 'running')
                        AND (lease_expires_at IS NULL OR lease_expires_at < now())
                        AND (resume_after IS NULL OR resume_after < now())
                      ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                      LIMIT 1
                 )
             RETURNING run_id, agent, input::text AS input, fencing_token, attempts
                """,
                worker_id,
                lease,
            )
        if row is None:
            return None
        return ClaimedRun(
            run_id=str(row["run_id"]),
            agent=row["agent"],
            input=_loads(row["input"]),
            fencing_token=row["fencing_token"],
            attempts=row["attempts"],
        )

    async def heartbeat(self, run_id: str, worker_id: str, token: int, lease: timedelta) -> bool:
        """
        Extend the lease. Returns False if this worker has been fenced.

        A worker that gets False must stop immediately: another worker now owns the run, and
        anything this one does from here would be a zombie write.
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE runs
                   SET lease_expires_at = now() + $4::interval, updated_at = now()
                 WHERE run_id = $1 AND lease_owner = $2 AND fencing_token = $3
                """,
                UUID(run_id),
                worker_id,
                token,
                lease,
            )
        return result.endswith(" 1")

    async def finish(
        self,
        run_id: str,
        token: int,
        status: str,
        result: Any = None,
        error: Optional[str] = None,
    ) -> None:
        async with self._pool.acquire() as conn:
            updated = await conn.execute(
                """
                UPDATE runs
                   SET status = $3, result = $4, error = $5,
                       lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                 WHERE run_id = $1 AND fencing_token = $2
                """,
                UUID(run_id),
                token,
                status,
                None if result is None else json.dumps(result),
                error,
            )
        if not updated.endswith(" 1"):
            raise FencedError(run_id, token)

    async def suspend(self, run_id: str, token: int, resume_after: Optional[datetime] = None) -> None:
        """Park the run and drop the lease, so it holds nothing at all while it waits."""
        async with self._pool.acquire() as conn:
            updated = await conn.execute(
                """
                UPDATE runs
                   SET status = 'suspended', lease_owner = NULL, lease_expires_at = NULL,
                       resume_after = $3, updated_at = now()
                 WHERE run_id = $1 AND fencing_token = $2
                """,
                UUID(run_id),
                token,
                resume_after,
            )
        if not updated.endswith(" 1"):
            raise FencedError(run_id, token)

    async def resume(self, run_id: str, payload: Any) -> bool:
        """
        Complete the outstanding suspend step with `payload` and make the run claimable.

        Done in one transaction: a resume that marked the run pending without journaling the
        payload would let a worker pick it up and replay into a suspend step that is still
        outstanding, suspending it again immediately.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                step = await conn.fetchrow(
                    """
                    SELECT step_seq FROM steps
                     WHERE run_id = $1 AND step_type = 'suspend' AND status = 'started'
                     ORDER BY step_seq DESC LIMIT 1
                    """,
                    UUID(run_id),
                )
                if step is None:
                    return False
                await conn.execute(
                    """
                    UPDATE steps SET output = $3, status = 'completed', completed_at = now()
                     WHERE run_id = $1 AND step_seq = $2
                    """,
                    UUID(run_id),
                    step["step_seq"],
                    json.dumps(payload),
                )
                await conn.execute(
                    """
                    UPDATE runs SET status = 'pending', resume_after = NULL, updated_at = now()
                     WHERE run_id = $1 AND status = 'suspended'
                    """,
                    UUID(run_id),
                )
        return True

    # -- steps ----------------------------------------------------------------------------

    async def get_step(self, run_id: str, step_seq: int) -> Optional[StepRecord]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT run_id, step_seq, step_type, name, input_hash,
                       input::text AS input, output::text AS output, status, fencing_token
                  FROM steps WHERE run_id = $1 AND step_seq = $2
                """,
                UUID(run_id),
                step_seq,
            )
        if row is None:
            return None
        return StepRecord(
            run_id=str(row["run_id"]),
            step_seq=row["step_seq"],
            step_type=row["step_type"],
            name=row["name"],
            input_hash=row["input_hash"],
            input=_loads(row["input"]),
            output=_loads(row["output"]),
            status=row["status"],
            fencing_token=row["fencing_token"],
        )

    async def list_steps(self, run_id: str) -> Sequence[StepRecord]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT run_id, step_seq, step_type, name, input_hash,
                       input::text AS input, output::text AS output, status, fencing_token
                  FROM steps WHERE run_id = $1 ORDER BY step_seq
                """,
                UUID(run_id),
            )
        return [
            StepRecord(
                run_id=str(r["run_id"]),
                step_seq=r["step_seq"],
                step_type=r["step_type"],
                name=r["name"],
                input_hash=r["input_hash"],
                input=_loads(r["input"]),
                output=_loads(r["output"]),
                status=r["status"],
                fencing_token=r["fencing_token"],
            )
            for r in rows
        ]

    async def begin_step(
        self,
        run_id: str,
        step_seq: int,
        step_type: str,
        name: str,
        input_hash: str,
        payload: Any,
        token: int,
    ) -> None:
        """
        Record that a step is about to execute, guarded by the fencing token.

        The guard is expressed as an INSERT ... SELECT over the run's current token so the
        check and the write are one statement. Reading the token first and inserting second
        would leave a window in which the lease is lost between the two.

        ON CONFLICT re-arms a step left 'started' by a crash: the previous attempt died
        mid-execution and this attempt is retrying it.
        """
        async with self._pool.acquire() as conn:
            written = await conn.execute(
                """
                INSERT INTO steps (run_id, step_seq, step_type, name, input_hash, input,
                                   status, fencing_token)
                SELECT $1, $2, $3, $4, $5, $6, 'started', $7
                  FROM runs
                 WHERE run_id = $1 AND fencing_token = $7
                ON CONFLICT (run_id, step_seq) DO UPDATE
                    SET status = 'started', input_hash = EXCLUDED.input_hash,
                        input = EXCLUDED.input, fencing_token = EXCLUDED.fencing_token
                """,
                UUID(run_id),
                step_seq,
                step_type,
                name,
                input_hash,
                json.dumps(payload),
                token,
            )
        if not written.endswith(" 1"):
            raise FencedError(run_id, token)

    async def complete_step(self, run_id: str, step_seq: int, output: Any, token: int) -> None:
        async with self._pool.acquire() as conn:
            updated = await conn.execute(
                """
                UPDATE steps
                   SET output = $3, status = 'completed', completed_at = now()
                 WHERE run_id = $1 AND step_seq = $2 AND fencing_token = $4
                """,
                UUID(run_id),
                step_seq,
                json.dumps(output),
                token,
            )
        if not updated.endswith(" 1"):
            raise FencedError(run_id, token)

    # -- effects --------------------------------------------------------------------------

    async def claim_effect(
        self, key: str, run_id: str, step_seq: int, tool_name: str
    ) -> Optional[dict[str, Any]]:
        """
        Try to claim the right to perform this side effect.

        Returns None when this attempt won the claim (it must now run the tool). Returns the
        existing row when somebody already claimed it, which is either a completed effect to
        return verbatim or a `pending` one that crashed mid-flight.

        `ON CONFLICT DO NOTHING` makes claiming atomic against concurrent workers without a
        lock; the primary key does the arbitration.
        """
        async with self._pool.acquire() as conn:
            inserted = await conn.fetchrow(
                """
                INSERT INTO effects (idempotency_key, run_id, step_seq, tool_name, status)
                VALUES ($1, $2, $3, $4, 'pending')
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING idempotency_key
                """,
                key,
                UUID(run_id),
                step_seq,
                tool_name,
            )
            if inserted is not None:
                return None
            row = await conn.fetchrow(
                """
                SELECT status, result::text AS result, attempts, tool_name
                  FROM effects WHERE idempotency_key = $1
                """,
                key,
            )
        return {
            "status": row["status"],
            "result": _loads(row["result"]),
            "attempts": row["attempts"],
            "tool_name": row["tool_name"],
        }

    async def complete_effect(self, key: str, result: Any) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE effects
                   SET status = 'completed', result = $2, completed_at = now()
                 WHERE idempotency_key = $1
                """,
                key,
                json.dumps(result),
            )

    async def bump_effect_attempt(self, key: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE effects SET attempts = attempts + 1 WHERE idempotency_key = $1", key
            )

    async def pending_effects(self) -> Sequence[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT idempotency_key, run_id, tool_name FROM effects WHERE status = 'pending'"
            )
        return [dict(r) for r in rows]
