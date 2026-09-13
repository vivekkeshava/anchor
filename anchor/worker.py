"""
Worker loop. This is the process the chaos harness kills.

It deliberately holds nothing that matters. Everything it learns goes to the journal before
it acts on it, which is what makes `kill -9` at an arbitrary instant a recoverable event
rather than a corrupting one.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import socket
from datetime import timedelta
from typing import Optional

from anchor.db import create_pool, dsn
from anchor.journal.repo import JournalRepo
from anchor.leases.manager import LeaseManager
from anchor.runtime.executor import Executor
from anchor.runtime.registry import AgentRegistry

log = logging.getLogger("anchor.worker")


class Worker:
    def __init__(
        self,
        repo: JournalRepo,
        registry: AgentRegistry,
        worker_id: Optional[str] = None,
        lease_seconds: float = 30.0,
        idle_sleep: float = 0.05,
        max_attempts: int = 20,
    ) -> None:
        self._repo = repo
        self._executor = Executor(repo, registry)
        self._worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self._lease = timedelta(seconds=lease_seconds)
        self._leases = LeaseManager(repo, self._worker_id, self._lease)
        self._idle_sleep = idle_sleep
        self._max_attempts = max_attempts
        self._stopping = asyncio.Event()

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def run_once(self) -> bool:
        """Claim and execute at most one run. Returns False when the queue is empty."""
        claimed = await self._leases.claim()
        if claimed is None:
            return False

        # Poison-run guard. A run whose terminal write itself keeps failing — a schema
        # mismatch, a constraint violation, a bug in the agent's result serialisation — is
        # reclaimed the instant its lease lapses, forever, occupying a worker slot and
        # starving real work. Bounding attempts turns an invisible infinite loop into a
        # recorded failure somebody can find.
        if claimed.attempts > self._max_attempts:
            log.error(
                "run %s exceeded %d attempts; marking failed rather than retrying forever",
                claimed.run_id,
                self._max_attempts,
            )
            await self._repo.finish(
                claimed.run_id,
                claimed.fencing_token,
                "failed",
                error=f"exceeded max attempts ({self._max_attempts}) without reaching a "
                f"terminal state; the run was repeatedly claimed and repeatedly failed to "
                f"finalise",
            )
            return True

        heartbeat = self._leases.start_heartbeat(claimed)
        try:
            await self._executor.execute(claimed)
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
        return True

    async def run_forever(self) -> None:
        log.info("worker %s polling %s", self._worker_id, dsn())
        while not self._stopping.is_set():
            try:
                did_work = await self.run_once()
            except Exception:  # noqa: BLE001 - a poisoned run must not kill the worker
                log.exception("worker %s: run failed", self._worker_id)
                did_work = False
            if not did_work:
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self._idle_sleep)
                except asyncio.TimeoutError:
                    pass

    def stop(self) -> None:
        self._stopping.set()


async def _amain(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    # Imported for its registration side effect; a worker can only run agents it knows.
    from examples.expense_agent import REGISTRY

    pool = await create_pool(args.dsn)
    repo = JournalRepo(pool)
    if args.create_schema:
        await repo.create_schema()

    worker = Worker(
        repo,
        REGISTRY,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
    )

    loop = asyncio.get_running_loop()
    # SIGTERM drains gracefully. SIGKILL — what the chaos harness sends — cannot be handled
    # at all, which is exactly why recovery must not depend on cleanup running.
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)

    try:
        await worker.run_forever()
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="anchor worker")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--lease-seconds", type=float, default=30.0)
    parser.add_argument("--create-schema", action="store_true")
    parser.add_argument("--log-level", default="info")
    asyncio.run(_amain(parser.parse_args()))


if __name__ == "__main__":
    main()
