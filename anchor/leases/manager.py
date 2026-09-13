"""Lease lifecycle: claim, heartbeat while working, give it back at the end."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Optional

from anchor.journal.repo import ClaimedRun, JournalRepo

log = logging.getLogger(__name__)


class LeaseManager:
    """
    Owns the heartbeat loop for a claimed run.

    The heartbeat is not merely a keepalive: its return value tells the worker whether it
    still owns the run. When a heartbeat comes back False the worker has been fenced — it
    stalled past its lease, somebody else claimed the run, and it must stop. Treating a
    failed heartbeat as a transient error to retry is how a stalled worker becomes a zombie
    that corrupts a run somebody else is executing.
    """

    def __init__(
        self,
        repo: JournalRepo,
        worker_id: str,
        lease: timedelta,
        heartbeat_interval: Optional[timedelta] = None,
    ) -> None:
        self._repo = repo
        self._worker_id = worker_id
        self._lease = lease
        # Heartbeat well inside the lease so one slow round trip does not lose it.
        self._interval = heartbeat_interval or timedelta(seconds=lease.total_seconds() / 3)
        self._fenced = asyncio.Event()

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def fenced(self) -> bool:
        return self._fenced.is_set()

    async def claim(self) -> Optional[ClaimedRun]:
        return await self._repo.claim(self._worker_id, self._lease)

    def start_heartbeat(self, claimed: ClaimedRun) -> asyncio.Task[None]:
        self._fenced.clear()
        return asyncio.create_task(self._heartbeat_loop(claimed), name=f"hb-{claimed.run_id[:8]}")

    async def _heartbeat_loop(self, claimed: ClaimedRun) -> None:
        try:
            while True:
                await asyncio.sleep(self._interval.total_seconds())
                alive = await self._repo.heartbeat(
                    claimed.run_id, self._worker_id, claimed.fencing_token, self._lease
                )
                if not alive:
                    log.warning(
                        "worker %s fenced off run %s (token %d)",
                        self._worker_id,
                        claimed.run_id,
                        claimed.fencing_token,
                    )
                    self._fenced.set()
                    return
        except asyncio.CancelledError:
            raise
