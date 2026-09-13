# anchor — Build Plan

**Durable Execution Runtime for LLM Agents**
Python 3.12 · PostgreSQL 16 · asyncio · asyncpg · pytest

---

## 0. The thesis

Agent runs are long-lived, non-deterministic, and full of side-effecting tool calls. If the
worker process dies thirty steps into a forty-step run, two things go wrong: the work is
lost, and any tool call that was in flight may have already taken effect — an email sent, a
payment charged, a ticket filed. Retrying the run from scratch does it again.

anchor makes an agent run **crash-recoverable** by journaling every model and tool call to
Postgres and replaying the journal instead of re-executing it, and makes tool side effects
**effectively-once** with a content-derived idempotency barrier.

This is Temporal's durable-execution model, scoped down to agents and implemented small
enough to read in an afternoon.

---

## 1. Non-goals

- Not a general workflow engine. Agent runs only, single process class, no DSL.
- No distributed transactions. The barrier makes effects effectively-once, not atomic with
  the journal write — see §3.4 for the exact guarantee and the window where it's violated.
- No agent framework. Bring your own loop; anchor wraps the calls.
- No UI. The chaos harness output is the demo.

---

## 2. Module layout

```
anchor/
├── anchor/
│   ├── journal/          # schema, migrations, JournalRepo
│   ├── runtime/          # Run, StepContext, executor, replay
│   ├── effects/          # idempotency keys, EffectBarrier
│   ├── leases/           # LeaseManager, heartbeat, reaper, fencing
│   ├── suspend/          # suspend/resume for human-in-the-loop
│   └── worker.py         # the worker entrypoint that chaos kills
├── examples/
│   └── expense_agent.py  # demo agent with genuinely side-effecting tools
├── chaos/
│   └── harness.py        # the headline artifact
├── tests/
└── docs/
    ├── design.md         # the ~500-word design note
    └── guarantees.md     # the failure model, stated precisely
```

---

## 3. Core design

### 3.1 Schema

```sql
CREATE TABLE runs (
    run_id          UUID PRIMARY KEY,
    status          TEXT NOT NULL,          -- pending|running|suspended|completed|failed
    input           JSONB NOT NULL,
    result          JSONB,
    next_step_seq   INT  NOT NULL DEFAULT 0,
    lease_owner     TEXT,
    lease_expires_at TIMESTAMPTZ,
    fencing_token   BIGINT NOT NULL DEFAULT 0,
    resume_after    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE steps (
    run_id        UUID NOT NULL REFERENCES runs(run_id),
    step_seq      INT  NOT NULL,
    step_type     TEXT NOT NULL,            -- model_call|tool_call|suspend
    input_hash    TEXT NOT NULL,
    input         JSONB NOT NULL,
    output        JSONB,
    status        TEXT NOT NULL,            -- started|completed|failed
    fencing_token BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_seq)
);

CREATE TABLE effects (
    idempotency_key TEXT PRIMARY KEY,
    run_id          UUID NOT NULL,
    step_seq        INT  NOT NULL,
    tool_name       TEXT NOT NULL,
    result          JSONB,
    status          TEXT NOT NULL,          -- pending|completed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX runs_claimable ON runs (status, lease_expires_at)
    WHERE status IN ('pending','running');
```

The journal is append-only per run: `(run_id, step_seq)` is written once and never updated
except `started → completed`. `next_step_seq` on `runs` is the replay cursor.

### 3.2 Step sequencing and replay

The executor wraps every model call and tool call:

```python
async def step(ctx, step_type, payload, fn):
    seq = ctx.next_seq()                       # deterministic: monotonic per run
    journaled = await repo.get_step(ctx.run_id, seq)
    if journaled and journaled.status == "completed":
        if journaled.input_hash != hash_payload(payload):
            raise NondeterminismError(ctx.run_id, seq)   # code changed under a live run
        return journaled.output                # REPLAY: no execution, no model call
    await repo.begin_step(ctx.run_id, seq, step_type, payload, ctx.fencing_token)
    out = await fn(payload)
    await repo.complete_step(ctx.run_id, seq, out)
    return out
```

Replay correctness rests on one property: **given the same journaled outputs, the agent
code takes the same path and therefore requests the same step_seq for the same logical
call.** The `input_hash` check enforces this — if the code changed between the original run
and the replay, the hash diverges and anchor refuses to continue rather than silently doing
something else. That refusal is a feature; say so in `guarantees.md`.

The model itself being non-deterministic is irrelevant, because on replay the model is
never called — its output comes from the journal. This is the point people miss, and it's
the thing to be able to explain crisply in an interview.

### 3.3 Sources of non-determinism to intercept

Anything that varies between original execution and replay must go through the journal:
`time.time()`, `uuid4()`, `random`, and any direct network call. Provide journaled helpers
(`ctx.now()`, `ctx.uuid()`, `ctx.random()`), and document that bypassing them breaks replay.
A lint/runtime guard that warns on raw `datetime.now()` inside a run is a nice touch if
hours allow.

### 3.4 The effect barrier

Tool calls have side effects, so "did it run?" must be answerable *by the downstream system*,
not by anchor's journal alone. Derive a deterministic key:

```
idempotency_key = sha256(run_id ‖ step_seq ‖ tool_name ‖ canonical_json(args))
```

Then:

1. `INSERT INTO effects (...) VALUES (..., 'pending') ON CONFLICT DO NOTHING`
2. If the insert took no rows → someone already ran this. Read the row; if `completed`,
   return its result; if `pending`, the previous attempt crashed mid-flight → surface it
   per the tool's declared policy (retry-safe tools re-run, unsafe tools fail loudly).
3. Otherwise execute the tool, passing `idempotency_key` to the downstream API when it
   supports one (Stripe-style), then mark `completed` with the result.

**State the guarantee precisely.** There is a window between the side effect landing and
the `completed` write. A crash inside that window leaves a `pending` row and anchor cannot
know whether the effect happened. Two honest options, and you implement both as a per-tool
policy: re-execute (safe iff the downstream honors the idempotency key) or halt for human
review. This is why the honest framing is **effectively-once**, not exactly-once, and being
able to articulate that distinction is worth more than a bolder claim.

### 3.5 Leases and fencing

Workers claim runs with a time-bounded lease:

```sql
UPDATE runs SET lease_owner = $1,
                lease_expires_at = now() + interval '30 seconds',
                fencing_token = fencing_token + 1,
                status = 'running'
WHERE run_id = (SELECT run_id FROM runs
                WHERE status IN ('pending','running')
                  AND (lease_expires_at IS NULL OR lease_expires_at < now())
                  AND (resume_after IS NULL OR resume_after < now())
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING run_id, fencing_token;
```

`FOR UPDATE SKIP LOCKED` gives contention-free claiming across N workers. The heartbeat
task extends the lease every 10s.

**Fencing.** A worker that stalls (GC pause, SIGSTOP, network partition) may believe it
still holds a lease that has since been reclaimed. Every journal write carries the worker's
`fencing_token` and is rejected if it is lower than the run's current token. Without this,
a zombie worker resurrects and corrupts the journal of a run another worker is actively
executing — the classic split-brain. Demonstrate it: a chaos mode that `SIGSTOP`s a worker
past its lease expiry, lets another claim the run, then `SIGCONT`s the first and asserts
its writes are rejected.

### 3.6 Suspend and resume

A step can suspend: write a `suspend` step, set `status='suspended'`, release the lease,
and return. The process holds **zero** in-memory state for that run. An external
`resume(run_id, payload)` journals the payload as the suspend step's output and flips the
run back to `pending`; any worker picks it up and replays to that point in milliseconds.

This is the clearest demonstration that replay works, because a run that parks for an hour
and completes on a different machine cannot have been holding anything in memory.

---

## 4. Milestones

### M1 — Journal · ~6h
Schema + migrations, `JournalRepo` on asyncpg, `Run`/`StepContext`, a demo agent in
`examples/expense_agent.py` with tools that write to an external side-effect ledger table
(so the chaos harness can count duplicates independently of anchor's own bookkeeping).

**Acceptance:** a run executes end to end; every model and tool call appears in `steps` in
order with `status='completed'`.

### M2 — Deterministic replay · ~8h
`step()` wrapper, replay cursor, `input_hash` guard, `NondeterminismError`, journaled
`ctx.now()`/`ctx.uuid()`/`ctx.random()`.

**Acceptance:** kill a run at step N; restart; assert (a) it completes, (b) steps `0..N-1`
made zero model calls on the second attempt — assert on a call counter in the fake model
client, not on wall-clock time. Mutate the agent code between attempts and assert
`NondeterminismError` is raised rather than silent divergence.

### M3 — Effect barrier · ~6h
Idempotency key derivation, `ON CONFLICT DO NOTHING` claim, pending-row recovery policy
per tool (`retry_safe: bool`), key propagation to the downstream API.

**Acceptance:** a run killed immediately after a tool's side effect but before the journal
write does not duplicate the effect on resume — verified by counting rows in the external
ledger, not by trusting anchor.

### M4 — Leases and fencing · ~6h
`LeaseManager` with `SKIP LOCKED` claiming, heartbeat, expiry reaper, fencing token checks
on every journal write.

**Acceptance:** N=4 workers against 100 queued runs process each exactly once. The
SIGSTOP/SIGCONT split-brain scenario has zombie writes rejected.

### M5 — Suspend/resume · ~4h
Suspend step type, lease release, `resume()` API, `resume_after` for timers.

**Acceptance:** a run suspends, the worker process is stopped entirely, a *different*
worker starts and completes the run after `resume()` is called.

### M6 — Chaos harness · ~6h — **this is the headline artifact**
`chaos/harness.py`: spawn the worker as a subprocess, `SIGKILL` at a uniformly random
offset within the run's expected duration, restart, repeat until all runs complete.

Invariants asserted after every campaign:
1. Every run reaches `completed`.
2. The external side-effect ledger contains **exactly one** row per logical effect.
3. No `steps` row was written with a stale fencing token.
4. Total model calls ≤ (number of logical steps + number of crashes) — i.e. replay actually
   replayed rather than re-executing.

Output a summary table: runs, crashes injected, recoveries, duplicate effects (target: 0).

**Acceptance:** 100 runs, randomized kills, 100% completion, zero duplicates, committed as
`docs/chaos-report.md` with the raw output.

---

## 5. Testing

- **Unit:** key derivation stability across dict ordering and float formatting; DRR of
  nothing here — canonical JSON is the subtle one, pin it.
- **Integration:** testcontainers-postgres; every milestone's acceptance check as a test.
- **Property-based (hypothesis):** generate random crash points over a random agent DAG and
  assert the four invariants. This is what turns "I ran it 100 times" into "I proved it over
  the input space," and it's cheap once the harness exists.
- **Concurrency:** N workers × M runs, assert exactly-once claiming.

---

## 6. Definition of done

- [ ] M1–M6 complete, acceptance checks green
- [ ] `docs/guarantees.md` — the failure model stated precisely, including the pending-row
      window and why the claim is *effectively*-once
- [ ] `docs/design.md` — ~500 words on journal-replay vs. re-execution
- [ ] `docs/chaos-report.md` — the 100-run campaign output
- [ ] README: architecture diagram, quickstart, **Known limitations**
- [ ] Commit history spanning ≥ 3 weeks

---

## 7. Resume bullets (fill blanks from YOUR chaos report)

- Built an event-sourced execution runtime for LLM agents in Python/PostgreSQL that
  journals every model and tool call, enabling crash recovery through deterministic replay
  of completed steps rather than full re-execution.
- Guaranteed effectively-once tool side effects via content-derived idempotency keys and a
  dedup barrier; a chaos harness injecting \_\_\_ random worker kills across \_\_\_ runs
  recovered 100% with zero duplicate effects.
- Implemented lease-based work distribution with `SKIP LOCKED` claiming, heartbeats, and
  fencing tokens that reject zombie-worker writes, plus suspend/resume allowing runs to
  park indefinitely without holding process state.

## 8. Interview talking points

At-least-once vs. exactly-once, and why "effectively-once" is the honest framing · the
pending-row window and the two honest recovery policies · fencing tokens and the
split-brain they prevent · what makes replay deterministic when the model is not · why the
journal is the source of truth rather than process memory · `SKIP LOCKED` as a queue
primitive and when it stops scaling
