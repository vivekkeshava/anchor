"""
Worker entrypoint the chaos harness spawns as a subprocess.

Separate from `anchor.worker` so the harness can kill a process that is doing nothing but
executing runs, with the demo agent's pool bound and short leases configured.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from anchor.db import create_pool
from anchor.journal.repo import JournalRepo
from anchor.worker import Worker
from examples.expense_agent import REGISTRY, bind_pool


async def _amain(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.ERROR)
    pool = await create_pool(args.dsn, min_size=1, max_size=4)
    bind_pool(pool)
    repo = JournalRepo(pool)
    worker = Worker(
        repo,
        REGISTRY,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        idle_sleep=0.02,
        max_attempts=args.max_attempts,
    )
    try:
        await worker.run_forever()
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--lease-seconds", type=float, default=2.0)
    parser.add_argument("--max-attempts", type=int, default=20)
    try:
        asyncio.run(_amain(parser.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
