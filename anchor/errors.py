"""Failure modes callers are expected to distinguish between."""


class AnchorError(Exception):
    """Base class for every error this runtime raises deliberately."""


class NondeterminismError(AnchorError):
    """
    The agent asked for a different step than the journal recorded at this position.

    Raised when a replay reaches step N and finds that the journaled input hash does not
    match what the code is asking for now — almost always because the agent's code changed
    while a run was in flight. Refusing to continue is the point: silently proceeding would
    splice a new code path onto an old run's history and produce a result that matches
    neither version.
    """

    def __init__(self, run_id: str, step_seq: int, expected: str, actual: str) -> None:
        super().__init__(
            f"run {run_id} step {step_seq}: journal recorded input {expected[:12]}… "
            f"but the agent is asking for {actual[:12]}…. The agent's code likely changed "
            f"while this run was in flight; anchor will not guess which version is correct."
        )
        self.run_id = run_id
        self.step_seq = step_seq
        self.expected = expected
        self.actual = actual


class AmbiguousEffectError(AnchorError):
    """
    A side effect may or may not have happened, and the tool is not safe to retry.

    The effect barrier claims a row before running a tool and marks it completed after. A
    crash in between leaves a `pending` row: the tool may have reached the downstream system
    or may not have. For a tool declared `retry_safe` (the downstream honours the
    idempotency key, or the operation is naturally idempotent) anchor re-runs it. For
    anything else — a card charge, an email — it stops here rather than risk doing it twice.

    This is the exact window that makes the guarantee *effectively*-once rather than
    exactly-once. See docs/guarantees.md.
    """

    def __init__(self, key: str, tool_name: str, run_id: str, step_seq: int) -> None:
        super().__init__(
            f"effect {tool_name} (run {run_id} step {step_seq}, key {key[:12]}…) was claimed "
            f"but never completed. It may already have taken effect downstream and the tool "
            f"is not declared retry_safe, so anchor will not run it again."
        )
        self.key = key
        self.tool_name = tool_name
        self.run_id = run_id
        self.step_seq = step_seq


class FencedError(AnchorError):
    """
    This worker's lease was reclaimed while it believed it still held it.

    A worker that stalls past its lease — a long GC pause, SIGSTOP, a network partition —
    wakes up still thinking it owns the run, while another worker has already claimed it.
    Every journal write carries the writer's fencing token and is rejected if the run has
    moved on. Without this check the zombie worker corrupts the journal of a run somebody
    else is actively executing.
    """

    def __init__(self, run_id: str, token: int) -> None:
        super().__init__(
            f"run {run_id}: write rejected, fencing token {token} is stale. "
            f"This worker's lease was reclaimed; it must stop touching this run."
        )
        self.run_id = run_id
        self.token = token


class Suspended(AnchorError):
    """
    Control-flow signal: the run is parking and holds no in-process state.

    Not a failure. The executor catches this, marks the run suspended, releases the lease,
    and returns the worker to the pool. The run resumes on whichever worker picks it up
    after `resume()` is called — possibly on a different machine, possibly hours later.
    """

    def __init__(self, run_id: str, step_seq: int, name: str) -> None:
        super().__init__(f"run {run_id} suspended at step {step_seq} ({name})")
        self.run_id = run_id
        self.step_seq = step_seq
        self.name = name


class UnknownAgentError(AnchorError):
    """A run references an agent this worker does not have registered."""
