# Why journal-and-replay rather than retry

## The problem

An agent run is a sequence of model calls and tool calls that takes minutes, branches on
model output, and does things that cannot be undone — charges a card, sends an email, files
a ticket. A worker executing one can die at any instant.

Retrying the run from the start is the obvious response, and it is wrong twice over. It
re-pays for every model call already made, and it repeats every side effect already
performed. Doing nothing is worse: the work is simply lost, and any effect already issued is
now orphaned with no record of why.

## The approach

Write down what happened, and on recovery read it back instead of doing it again.

1. **Journal every step before and after it executes.** A `started` row means "this was
   attempted"; a `completed` row carries the result. The journal, not process memory, is the
   run's state.
2. **Replay reads, it does not execute.** A recovering attempt walks the journal from step
   zero. Completed steps return their recorded output without invoking anything. Only the
   un-journaled suffix actually runs.
3. **Guard the replay assumption.** Step positions come from a counter, which is only
   meaningful if the same code takes the same path. Every replayed step verifies an input
   hash and raises rather than proceeding when the code has changed underneath.
4. **Put side effects behind a content-keyed barrier.** The journal cannot answer "did this
   tool already reach the outside world?", because the effect lands in someone else's
   system. A claim row keyed on `sha256(run | seq | tool | canonical_args)` can.
5. **Lease work, and fence the loser.** Workers claim runs with `FOR UPDATE SKIP LOCKED` and
   hold a time-bounded lease. Every claim bumps a fencing token so a stalled worker's writes
   are rejected once it has been overtaken.

## Decisions worth defending

**The journal is the source of truth, not a log.** Logs are written after the fact for
humans. This is written before the fact and read by the machine, which is why a `started`
row exists at all: without it, a crash mid-step would be indistinguishable from a step that
never began.

**Reserve position by counter, verify by hash.** Naming steps explicitly (a string key per
call) would survive refactoring better, but it pushes uniqueness onto the agent author and
fails silently when two calls share a name. A counter plus a verified hash detects the
problem instead of hiding it.

**`SKIP LOCKED` rather than a queue table with a status flag.** The flag approach needs a
transaction per poll and serialises workers against each other. `SKIP LOCKED` hands each
worker a different row, so claiming does not become the bottleneck as the pool grows.

**Fencing tokens rather than lease timestamps alone.** Comparing "is my lease still valid?"
against a clock is a race: the check and the write are separate, and the lease can lapse
between them. A token compared *inside* the same statement as the write cannot be raced.

**A distinct `needs_review` state.** The alternative is marking ambiguous runs `failed`,
which is technically true and operationally useless — it buries the one case a human must
look at among ordinary crashes.

**Effectively-once, stated as such.** Claiming exactly-once would require a distributed
transaction across anchor's database and every downstream system, which does not exist for
real APIs. Naming the window and giving each tool a policy for it is the honest design. See
[guarantees.md](guarantees.md).

## What this is not

Not a general workflow engine. Temporal solves a much larger problem — versioning, signals,
child workflows, cron, multi-language SDKs — and if you need that, use it. anchor is the
subset an agent runtime actually needs, small enough to read in an afternoon and understand
completely.
