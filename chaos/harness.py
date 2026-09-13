"""
Chaos harness: kill workers at random instants and prove the invariants still hold.

This is the project's headline artifact. Everything else is a claim; this is the evidence.

The method is deliberately crude — spawn a worker, SIGKILL it at a uniformly random offset,
restart, repeat — because SIGKILL is the one failure mode no cleanup handler can soften. If
recovery survives that, it survives the gentler failures by construction.

Run:
    python -m chaos.harness --runs 100 --kills 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from anchor.db import create_pool, dsn
from anchor.journal.repo import JournalRepo
from examples.expense_agent import bind_pool

REPO_ROOT = Path(__file__).resolve().parent.parent

# The expense agent makes four model calls per run. Used to bound how much re-execution
# replay is allowed to have caused.
MODEL_STEPS_PER_RUN = 3


@dataclass
class Report:
    runs: int = 0
    kills: int = 0
    completed: int = 0
    failed: int = 0
    runs_recovered: int = 0
    total_attempts: int = 0
    naive_model_calls: int = 0
    needs_review: int = 0
    duplicate_effects: int = 0
    total_effects: int = 0
    model_calls: int = 0
    expected_model_calls: int = 0
    pending_effects: int = 0
    non_contiguous_journals: int = 0
    wall_seconds: float = 0.0
    violations: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.violations

    def render(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [
            "",
            "=" * 68,
            f"  anchor chaos report — {status}",
            "=" * 68,
            f"  runs enqueued            {self.runs}",
            f"  worker kills injected    {self.kills}  (SIGKILL at random offsets)",
            f"  wall time                {self.wall_seconds:.1f}s",
            "",
            f"  runs completed           {self.completed} / {self.runs}",
            f"  runs needing review      {self.needs_review}"
            f"   (crash landed inside the barrier window)",
            f"  runs failed              {self.failed}   <- must be 0",
            "",
            f"  side effects recorded    {self.total_effects}",
            f"  DUPLICATE side effects   {self.duplicate_effects}   <- must be 0",
            f"  effects left pending     {self.pending_effects}"
            f"   (one per run needing review)",
            "",
            f"  runs interrupted+resumed {self.runs_recovered}"
            f"   (claimed more than once)",
            f"  total claim attempts     {self.total_attempts}",
            "",
            f"  model calls made         {self.model_calls}",
            f"  ...without replay        {self.naive_model_calls}"
            f"   (every recovery would restart from step 0)",
            f"  replay budget            {self.expected_model_calls + self.kills}"
            f"   ({self.expected_model_calls} steps + {self.kills} kills)",
            f"  journals non-contiguous  {self.non_contiguous_journals}   <- must be 0",
            "=" * 68,
        ]
        if self.violations:
            lines.append("  VIOLATIONS")
            lines.extend(f"    - {v}" for v in self.violations)
            lines.append("=" * 68)
        return "\n".join(lines)

    def to_json(self) -> str:
        payload = {k: v for k, v in self.__dict__.items()}
        payload["passed"] = self.passed
        return json.dumps(payload, indent=2)


class ChaosHarness:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.report = Report(runs=args.runs)
        self._rng = random.Random(args.seed)

    # -- worker process control -----------------------------------------------------------

    def _spawn(self, index: int) -> subprocess.Popen:
        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT)
        if self.args.dsn:
            env["ANCHOR_DSN"] = self.args.dsn
        env["ANCHOR_MODEL_LATENCY"] = str(self.args.model_latency)
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "chaos.worker_entry",
                "--worker-id",
                f"chaos-{index}",
                "--lease-seconds",
                str(self.args.lease_seconds),
            ],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    @staticmethod
    def _kill(proc: subprocess.Popen) -> None:
        """
        SIGKILL, not SIGTERM.

        SIGTERM would let the worker release its lease and finish its current step, which is
        the easy case. The interesting failure is the one where no code of ours runs at all.
        """
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGKILL)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    # -- phases ---------------------------------------------------------------------------

    async def _seed_runs(self, repo: JournalRepo) -> None:
        await repo.create_schema()
        await repo.truncate_all()
        for i in range(self.args.runs):
            # Amounts straddle the policy limit of 500, so some runs charge the card (an
            # unsafe effect) and some do not.
            await repo.enqueue("expense", {"amount": 200 + (i % 5) * 150, "employee": f"emp-{i}"})

    async def _chaos_phase(self, repo: JournalRepo) -> None:
        """Kill workers at random offsets until the kill budget is spent."""
        deadline = time.monotonic() + self.args.max_seconds
        index = 0
        while self.report.kills < self.args.kills and time.monotonic() < deadline:
            proc = self._spawn(index)
            index += 1
            lifetime = self._rng.uniform(self.args.min_life, self.args.max_life)
            await asyncio.sleep(lifetime)
            self._kill(proc)
            self.report.kills += 1

            counts = await repo.counts_by_status()
            if counts.get("completed", 0) >= self.args.runs:
                break

    async def _drain_phase(self, repo: JournalRepo) -> None:
        """Let workers finish undisturbed. Recovery, not liveness under chaos, is the claim."""
        workers = [self._spawn(1000 + i) for i in range(self.args.drain_workers)]
        deadline = time.monotonic() + self.args.max_seconds
        try:
            while time.monotonic() < deadline:
                counts = await repo.counts_by_status()
                settled = counts.get("completed", 0) + counts.get("failed", 0)
                if settled >= self.args.runs:
                    return
                await asyncio.sleep(0.25)
        finally:
            for proc in workers:
                self._kill(proc)

    # -- invariants -----------------------------------------------------------------------

    async def _check(self, repo: JournalRepo, pool) -> None:
        counts = await repo.counts_by_status()
        self.report.completed = counts.get("completed", 0)
        self.report.failed = counts.get("failed", 0)
        self.report.needs_review = counts.get("needs_review", 0)

        # 1. Every run reaches a terminal state. 'completed' and 'needs_review' are both
        #    correct outcomes: the second means a crash landed in the barrier window on an
        #    unsafe effect and anchor refused to guess. Counting that as a failure would be
        #    scoring the design against a guarantee it explicitly does not claim. What is NOT
        #    acceptable is a run stuck non-terminal, or one that failed outright.
        terminal = self.report.completed + self.report.needs_review
        if terminal != self.args.runs:
            self.report.violations.append(
                f"{self.args.runs - terminal} run(s) never reached a terminal state "
                f"(status counts: {counts})"
            )
        if self.report.failed:
            self.report.violations.append(
                f"{self.report.failed} run(s) failed outright (status counts: {counts})"
            )

        async with pool.acquire() as conn:
            # 2. Exactly one ledger row per (run, logical effect). This is the headline
            #    invariant, and it is checked in the *downstream* table rather than in
            #    anchor's own effects table — verifying a system with its own bookkeeping
            #    proves nothing.
            dupes = await conn.fetch(
                """
                SELECT run_id, logical_key, count(*) AS n
                  FROM side_effect_ledger
                 GROUP BY run_id, logical_key
                HAVING count(*) > 1
                """
            )
            self.report.duplicate_effects = sum(r["n"] - 1 for r in dupes)
            self.report.total_effects = await conn.fetchval("SELECT count(*) FROM side_effect_ledger")

            if dupes:
                self.report.violations.append(
                    f"{self.report.duplicate_effects} duplicate side effect(s); "
                    f"first: run {dupes[0]['run_id']} key {dupes[0]['logical_key']}"
                )

            # 3. Replay actually replayed. Each kill can force at most one in-flight step to
            #    re-execute, so model calls are bounded by (logical steps + kills). Without
            #    replay this number would be far higher — every crash would restart a run
            #    from step zero.
            self.report.model_calls = await conn.fetchval("SELECT count(*) FROM model_call_log")

            # How many runs were actually interrupted and had to resume. Without this the
            # model-call budget proves nothing: a campaign where no kill landed mid-run
            # trivially satisfies it while exercising no recovery at all.
            self.report.runs_recovered = await conn.fetchval(
                "SELECT count(*) FROM runs WHERE attempts > 1"
            )
            self.report.total_attempts = await conn.fetchval(
                "SELECT coalesce(sum(attempts), 0) FROM runs"
            )
            # What the same campaign would have cost if every recovery restarted at step 0.
            # The gap between this and model_calls is the work replay avoided.
            self.report.naive_model_calls = self.report.total_attempts * MODEL_STEPS_PER_RUN
            # Runs halted for review stopped partway, so they cost at most a full run's
            # worth of model steps. Using the full count keeps the budget conservative.
            self.report.expected_model_calls = (
                self.report.completed + self.report.needs_review
            ) * MODEL_STEPS_PER_RUN
            budget = self.report.expected_model_calls + self.report.kills
            if self.report.model_calls > budget:
                self.report.violations.append(
                    f"{self.report.model_calls} model calls exceeds the replay budget of "
                    f"{budget} ({self.report.expected_model_calls} steps + "
                    f"{self.report.kills} kills): steps were re-executed, not replayed"
                )

            # 4. No journal has holes or stale fencing tokens. Steps are written 0..n-1 and
            #    their tokens never go backwards.
            bad = await conn.fetch(
                """
                SELECT run_id,
                       count(*)                       AS steps,
                       max(step_seq)                  AS max_seq,
                       bool_or(token_went_backwards)  AS fenced_out_of_order
                  FROM (
                        SELECT run_id, step_seq,
                               fencing_token < lag(fencing_token) OVER
                                   (PARTITION BY run_id ORDER BY step_seq)
                                   AS token_went_backwards
                          FROM steps
                       ) s
                 GROUP BY run_id
                HAVING count(*) <> max(step_seq) + 1 OR bool_or(token_went_backwards)
                """
            )
            self.report.non_contiguous_journals = len(bad)
            if bad:
                self.report.violations.append(
                    f"{len(bad)} journal(s) non-contiguous or written with a stale fencing token"
                )

            # 5. Every pending effect must correspond to a run that stopped for review. A
            #    pending effect on a *completed* run would mean the barrier let a run finish
            #    while an unsafe effect was still unaccounted for.
            pending = await conn.fetchval("SELECT count(*) FROM effects WHERE status = 'pending'")
            self.report.pending_effects = pending
            orphaned = await conn.fetchval(
                """
                SELECT count(*) FROM effects e
                  JOIN runs r ON r.run_id = e.run_id
                 WHERE e.status = 'pending' AND r.status <> 'needs_review'
                """
            )
            if orphaned:
                self.report.violations.append(
                    f"{orphaned} pending effect(s) on runs that are not marked needs_review: "
                    f"a run finished with an unsafe effect unaccounted for"
                )

    # -- driver ---------------------------------------------------------------------------

    async def run(self) -> Report:
        started = time.monotonic()
        pool = await create_pool(self.args.dsn, min_size=1, max_size=8)
        bind_pool(pool)
        repo = JournalRepo(pool)
        try:
            await self._seed_runs(repo)
            await self._chaos_phase(repo)
            await self._drain_phase(repo)
            await self._check(repo, pool)
        finally:
            await pool.close()
        self.report.wall_seconds = time.monotonic() - started
        return self.report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="anchor chaos harness")
    parser.add_argument("--runs", type=int, default=100)
    parser.add_argument("--kills", type=int, default=60)
    parser.add_argument("--min-life", type=float, default=0.15, help="min worker lifetime (s)")
    parser.add_argument("--max-life", type=float, default=1.2, help="max worker lifetime (s)")
    parser.add_argument("--lease-seconds", type=float, default=2.0)
    parser.add_argument("--drain-workers", type=int, default=4)
    parser.add_argument("--max-seconds", type=float, default=300.0)
    parser.add_argument(
        "--model-latency",
        type=float,
        default=0.05,
        help="simulated per-model-call latency; raise it so kills land mid-run",
    )
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--json", default=None, help="write the report as JSON to this path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    print(f"anchor chaos: {args.runs} runs, {args.kills} kills, dsn={args.dsn or dsn()}")
    report = asyncio.run(ChaosHarness(args).run())
    print(report.render())
    if args.json:
        Path(args.json).write_text(report.to_json())
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
