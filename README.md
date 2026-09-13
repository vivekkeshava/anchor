# anchor

**Durable execution for LLM agents: journal every step, replay instead of re-running, and
make tool side effects effectively-once.**

[![CI](https://github.com/vivekkeshava/anchor/actions/workflows/ci.yml/badge.svg)](https://github.com/vivekkeshava/anchor/actions/workflows/ci.yml)

An agent run takes minutes, branches on model output, and does things that cannot be undone —
charges a card, sends an email, files a ticket. When the worker executing one dies, retrying
from the start re-pays for every model call already made and repeats every side effect
already performed. Doing nothing loses the work and orphans the effects.

anchor journals every model and tool call to Postgres. A recovering attempt reads completed
steps back from the journal instead of executing them, and a content-keyed barrier stops any
tool side effect from happening twice.

---

## Measured, not asserted

`chaos/harness.py` SIGKILLs workers at uniformly random offsets and then checks the
invariants. Across **3 seeds × 100 runs × 60 kills — 300 runs, 180 kills, 108 of them
interrupted and resumed — zero duplicate side effects.** One campaign:

```
====================================================================
  anchor chaos report — PASS
====================================================================
  runs enqueued            100
  worker kills injected    60  (SIGKILL at random offsets)
  wall time                277.5s

  runs completed           99 / 100
  runs needing review      1   (crash landed inside the barrier window)
  runs failed              0   <- must be 0

  side effects recorded    158
  DUPLICATE side effects   0   <- must be 0
  effects left pending     1   (one per run needing review)

  runs interrupted+resumed 41   (claimed more than once)
  total claim attempts     149

  model calls made         339
  ...without replay        447   (every recovery would restart from step 0)
  replay budget            360   (300 steps + 60 kills)
  journals non-contiguous  0   <- must be 0
====================================================================
```

Read the two numbers that matter together. **Zero duplicate side effects** across 158
effects and 41 interrupted runs. **One run halted for review** — a crash landed inside the
barrier window on a card charge, and anchor refused to guess whether it had gone through.
That one run is not a bug; it is the guarantee being honest about its boundary.

Duplicates are counted in the *downstream* ledger table, not in anchor's own bookkeeping.
Verifying a system's correctness with its own records proves nothing.

Full results, environment, and the bug a second seed caught:
[docs/chaos-report.md](docs/chaos-report.md).

```bash
make db && make chaos
```

---

## How it works

```python
async def expense_agent(payload, ctx):
    # Journaled. On replay the model is never called — the output comes from the journal.
    decision = await ctx.call_model("decide", payload, lambda: model.complete(...))

    # Journaled AND behind the effect barrier. Not safe to repeat, so if a crash leaves the
    # outcome unknown the run stops for review rather than risking a double charge.
    await ctx.call_tool(
        "charge_card", {"amount": payload["amount"]},
        lambda key: charge(payload["amount"], idempotency_key=key),
        retry_safe=False,
    )

    # Parks indefinitely holding zero process state. Resumes on any worker.
    approval = await ctx.suspend("await_human_approval")
```

**Replay is deterministic because the model is never called on replay.** Its output is read
from the journal. What must be deterministic is the agent's control flow, and that is
verified rather than trusted: every replayed step compares an input hash against what the
code is now asking for, and raises `NondeterminismError` on a mismatch instead of splicing a
new code path onto an old run's history.

**Two layers, two questions.** The journal answers "does this run already have a result for
this position?". The barrier answers "did this effect already reach the outside world?". A
crash between an effect landing and its journal write makes the first say no while the
second says yes — and only the barrier can stop the tool running twice.

**Fencing.** Every claim increments a token; every journal write carries the writer's token
and is rejected if the run has moved on. A worker that stalls past its lease and wakes up
still believing it owns the run cannot corrupt a run somebody else is executing.

Full rationale: [docs/design.md](docs/design.md). The boundary of the guarantee:
[docs/guarantees.md](docs/guarantees.md).

---

## Quickstart

```bash
docker compose up -d                 # Postgres on :55432
pip install -e ".[dev]"
export ANCHOR_DSN=postgresql://anchor:anchor@localhost:55432/anchor

pytest -q                            # 32 tests
python -m chaos.harness --runs 20 --kills 10
```

Enqueue and run:

```python
from anchor.db import create_pool
from anchor.journal.repo import JournalRepo
from anchor.worker import Worker
from examples.expense_agent import REGISTRY, bind_pool

pool = await create_pool()
bind_pool(pool)
repo = JournalRepo(pool)
await repo.create_schema()

run_id = await repo.enqueue("expense", {"amount": 200, "employee": "ada"})
await Worker(repo, REGISTRY).run_once()

run = await repo.get_run(run_id)   # status='completed', result={...}
```

---

## The step API

| Call | Journaled | Barrier | Notes |
|------|-----------|---------|-------|
| `ctx.call_model(name, prompt, fn)` | yes | no | Not invoked on replay |
| `ctx.call_tool(name, args, fn, retry_safe=)` | yes | yes | `fn` receives the idempotency key |
| `ctx.suspend(name, payload)` | yes | no | Releases the lease; resumes on any worker |
| `ctx.now()` / `ctx.uuid()` / `ctx.random()` | yes | no | Journaled sources of non-determinism |
| `ctx.sleep_until(when)` | yes | no | Parks until a wall-clock instant |

**Calling `datetime.now()` or `uuid4()` directly inside an agent silently breaks replay** —
the replayed run takes a different value than the original and can branch away from the
journal it is meant to be following. Use the journaled helpers. anchor cannot detect this
violation in general; it is a discipline the agent author keeps.

---

## Terminal states

| Status | Meaning |
|--------|---------|
| `completed` | The agent returned. |
| `needs_review` | A crash landed in the barrier window on a tool that is not safe to retry. A human must reconcile that one effect. |
| `failed` | The agent raised, or the run exceeded `max_attempts`. |
| `suspended` | Parked. Holds no worker and no process state. |

`needs_review` is deliberately distinct from `failed`: conflating them buries the one case
that actually requires a person.

---

## Development

```bash
make db            # Postgres
make test          # 32 tests
make chaos         # 100 runs, 60 kills
make chaos-quick   # 20 runs, 10 kills
```

Tests cover key-derivation stability (including a hypothesis property test), replay and the
non-determinism guard, the barrier's retry-safe and unsafe policies, concurrent claiming
under `SKIP LOCKED`, zombie-worker fencing, suspend/resume across workers, and the
poison-run bound. They need Postgres and skip cleanly without it.

---

## Known limitations

- **The barrier window is real.** Between a side effect landing and its completion write,
  a crash leaves the outcome genuinely unknown. `retry_safe` tools are re-run; others halt
  for review. This is why the claim is *effectively*-once, not exactly-once.
- **Replay cost grows with journal length.** Each attempt re-reads completed steps one round
  trip at a time. Batching the prefix read would fix it; it has not been done.
- **No automatic reconciliation for `needs_review`.** Resolving one means querying the
  downstream system by hand.
- **No step-level timeouts.** A hung tool holds its lease until the heartbeat stops; the run
  is then reclaimed and the hung call is orphaned rather than cancelled.
- **Single Postgres instance.** Its availability is anchor's availability.
- **No resource limits.** An agent that loops forever produces an unbounded journal.
- **The demo agent's tools are not real integrations.** They write to a local ledger table
  standing in for a downstream system.

---

## License

MIT — see [LICENSE](LICENSE).
