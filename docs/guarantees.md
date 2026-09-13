# What anchor guarantees, and what it does not

The interesting part of a durability claim is its boundary. This document states the
boundary precisely, because a guarantee whose failure modes are undocumented is a guarantee
nobody can build on.

## The claim

> Given a crash at any instant, every run reaches a terminal state, completed steps are
> never re-executed, and no tool side effect happens more than once.

Verified by `chaos/harness.py`, which SIGKILLs workers at uniformly random offsets. See
[chaos-report.md](chaos-report.md) for a measured campaign.

## Terminal states

| Status | Meaning |
|--------|---------|
| `completed` | The agent returned. `result` holds its value. |
| `needs_review` | A crash landed inside the effect barrier's window on a tool that is not safe to retry. The outcome of that one side effect is genuinely unknown and a human must reconcile it. |
| `failed` | The agent raised, or the run exceeded `max_attempts` without reaching a terminal state. |
| `suspended` | Parked awaiting `resume()` or a timer. Not terminal, but holds no worker and no process state. |

`needs_review` is deliberately distinct from `failed`. Conflating them buries the one case
that actually requires a person.

## Effectively-once, not exactly-once

The barrier runs `claim → execute → complete`:

```
  INSERT effects (key, status='pending') ON CONFLICT DO NOTHING
      │
      ├─ claim won  ──► run the tool ──► UPDATE effects SET status='completed'
      │                      ▲                      ▲
      │                      └──── THE WINDOW ──────┘
      │
      └─ claim lost ──► completed? return the stored result
                        pending?   the previous attempt died in the window
```

**A crash inside the window is unresolvable from anchor's side.** The tool may have reached
the downstream system or may not have. Closing the window would require a distributed
transaction spanning anchor's database and the downstream service — which, for a payment API
or an SMTP server, does not exist.

So it is handled by policy, declared per tool:

- **`retry_safe=True`** — the downstream deduplicates on the idempotency key anchor passes
  it, or the operation is naturally idempotent. Anchor re-runs the tool.
- **`retry_safe=False`** — a card charge, an email. Anchor stops and marks the run
  `needs_review`.

This is why the honest word is *effectively*-once. Claiming exactly-once would mean claiming
the window does not exist.

**The window is small but real.** It spans one tool invocation plus one database round trip.
In the measured campaign it caught roughly 1 run in 100 under a deliberately hostile kill
rate. Under normal operation — where crashes are rare rather than continuous — it is
correspondingly rarer, but it never reaches zero.

## What makes replay deterministic

Replay is deterministic because **the model is never called on replay**. Its output comes
from the journal. Non-determinism in the model is irrelevant; what must be deterministic is
the agent's *control flow*, so that step N on the second pass is the same logical call as
step N on the first.

That assumption is checked rather than trusted: every replayed step compares an input hash
of `(step_type, name, payload)` against what the code is now asking for, and a mismatch
raises `NondeterminismError` instead of proceeding.

Other sources of non-determinism must go through the journaled helpers — `ctx.now()`,
`ctx.uuid()`, `ctx.random()`. **Calling `datetime.now()` or `uuid4()` directly inside an
agent silently breaks replay**, because the replayed run takes a different value than the
original and can branch away from the journal it is meant to be following. Anchor cannot
detect this in general; it is a discipline the agent author must keep.

## Fencing

A worker that stalls past its lease — a long GC pause, `SIGSTOP`, a partition — wakes up
still believing it owns its run. Meanwhile another worker has claimed it.

Every claim increments `runs.fencing_token`. Every journal write carries the writer's token
and is rejected if the run has moved on. A failed heartbeat is not a transient error to
retry: it means this worker has been fenced and must stop touching the run.

Without this, a zombie worker corrupts the journal of a run somebody else is actively
executing — the classic split-brain.

## Known limitations

- **`max_attempts` failures are terminal but uninformative.** A run bounded out at
  `max_attempts` records that fact, but diagnosing *why* it could not finalise means reading
  worker logs.
- **No automatic reconciliation for `needs_review`.** Resolving one means querying the
  downstream system and deciding by hand. A production deployment would want a reconciler
  per tool.
- **Replay cost grows with journal length.** Every attempt re-reads completed steps one at a
  time. For long runs recovered many times this is O(steps) round trips per attempt; batching
  the prefix read would fix it and has not been done.
- **`max_tokens`-style resource limits do not exist.** An agent that loops forever produces
  an unbounded journal.
- **Single database.** The journal is one Postgres instance; its availability is anchor's
  availability. Nothing here addresses multi-region.
- **No step-level timeouts.** A tool that hangs holds its lease only until the heartbeat
  stops, after which the run is reclaimed and the hung call is orphaned rather than cancelled.
- **The demo agent's tools are not real integrations.** They write to a local ledger table
  that stands in for a downstream system; a real payment API brings its own failure modes.
