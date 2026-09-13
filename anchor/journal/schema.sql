-- anchor: durable execution for LLM agents.
--
-- The journal is the source of truth. A worker process holds no state that matters: every
-- decision a run has made is a row here, so any worker can pick up any run at any point.

CREATE TABLE IF NOT EXISTS runs (
    run_id           UUID PRIMARY KEY,
    agent            TEXT        NOT NULL,
    -- 'needs_review' is distinct from 'failed' on purpose: the agent did not crash, the
    -- effect barrier refused to guess whether an unsafe side effect had already landed.
    -- Conflating the two would hide the one case a human actually has to look at.
    status           TEXT        NOT NULL CHECK (status IN
                         ('pending', 'running', 'suspended', 'completed', 'failed',
                          'needs_review')),
    input            JSONB       NOT NULL,
    result           JSONB,
    error            TEXT,

    -- Lease: which worker currently owns this run, and until when.
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,

    -- Monotonic per run. Incremented on every claim; journal writes carrying an older token
    -- are rejected, which is what stops a stalled worker from resurrecting and corrupting a
    -- run another worker has since taken over.
    fencing_token    BIGINT      NOT NULL DEFAULT 0,

    -- Set while suspended (human approval, timer). The claim query will not pick the run up
    -- before this instant.
    resume_after     TIMESTAMPTZ,

    attempts         INT         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial index: the claim query only ever looks at runnable rows, and completed runs
-- accumulate forever. Without the WHERE clause the index degrades as history grows.
CREATE INDEX IF NOT EXISTS runs_claimable_idx
    ON runs (created_at)
    WHERE status IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS steps (
    run_id        UUID        NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    step_seq      INT         NOT NULL,
    step_type     TEXT        NOT NULL,
    name          TEXT        NOT NULL,

    -- Hash of (step_type, name, payload). On replay this must match what the agent asks for
    -- at this position; a mismatch means the code changed under a live run.
    input_hash    TEXT        NOT NULL,
    input         JSONB       NOT NULL,
    output        JSONB,
    status        TEXT        NOT NULL CHECK (status IN ('started', 'completed', 'failed')),

    -- The token held by the worker that wrote this row.
    fencing_token BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,

    -- One row per position per run. Replay reads by this key, and the primary key is what
    -- makes a duplicate journal write impossible rather than merely unlikely.
    PRIMARY KEY (run_id, step_seq)
);

CREATE TABLE IF NOT EXISTS effects (
    -- sha256(run_id | step_seq | tool_name | canonical_json(args)). Derived from content, so
    -- the same logical call computes the same key on every attempt without coordination.
    idempotency_key TEXT        PRIMARY KEY,
    run_id          UUID        NOT NULL,
    step_seq        INT         NOT NULL,
    tool_name       TEXT        NOT NULL,
    result          JSONB,
    status          TEXT        NOT NULL CHECK (status IN ('pending', 'completed')),
    attempts        INT         NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS effects_run_idx ON effects (run_id);

-- ---------------------------------------------------------------------------------------
-- Below this line: demo/verification tables, not part of the runtime.
-- ---------------------------------------------------------------------------------------

-- Stands in for the downstream system a tool actually talks to (the payment processor's
-- ledger, the mail provider's outbox).
--
-- Deliberately has NO unique constraint on (run_id, logical_key). A constraint here would
-- make duplicates impossible at the database level and the chaos harness would prove
-- nothing: it would be testing Postgres, not anchor. Duplicates must be *detectable*, which
-- means they must be *possible*.
CREATE TABLE IF NOT EXISTS side_effect_ledger (
    id          BIGSERIAL   PRIMARY KEY,
    run_id      UUID        NOT NULL,
    logical_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ledger_run_idx ON side_effect_ledger (run_id, logical_key);

-- Counts actual model invocations, so the chaos harness can prove replay really replayed
-- instead of quietly re-running every step.
CREATE TABLE IF NOT EXISTS model_call_log (
    id        BIGSERIAL   PRIMARY KEY,
    run_id    UUID        NOT NULL,
    step_seq  INT         NOT NULL,
    called_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------------------
-- Convergence
-- ---------------------------------------------------------------------------------------
--
-- CREATE TABLE IF NOT EXISTS is a no-op against a database that already has the table, so
-- every change above this line is invisible to an existing deployment. That is a quiet and
-- expensive failure mode: a new status value reaches the code but never the constraint, and
-- the first run that tries to use it fails its finalize write and is retried forever.
--
-- These statements run unconditionally and are idempotent, so applying this file always
-- converges the schema rather than only creating it. Anything that changes an existing
-- object belongs here, not in a CREATE ... IF NOT EXISTS.

ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_status_check;
ALTER TABLE runs ADD CONSTRAINT runs_status_check CHECK (status IN
    ('pending', 'running', 'suspended', 'completed', 'failed', 'needs_review'));

ALTER TABLE steps DROP CONSTRAINT IF EXISTS steps_status_check;
ALTER TABLE steps ADD CONSTRAINT steps_status_check CHECK (status IN
    ('started', 'completed', 'failed'));

ALTER TABLE effects DROP CONSTRAINT IF EXISTS effects_status_check;
ALTER TABLE effects ADD CONSTRAINT effects_status_check CHECK (status IN
    ('pending', 'completed'));
