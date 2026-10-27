import logging

from .errors import IllegalTransition
from .models import SUBTASK_STATUSES, TERMINAL_SUBTASK_STATUSES

logger = logging.getLogger("fusion_flow")

# Legal job transitions. Anything not listed raises IllegalTransition after the
# transition_denied audit event has been written by the caller (storage layer).
JOB_TRANSITIONS = {
    "queued": {"processing", "cancelled", "superseded"},
    "processing": {"succeeded", "partial_succeeded", "failed", "dead_lettered", "queued"},
    "succeeded": set(),
    "partial_succeeded": set(),
    "failed": set(),
    "dead_lettered": set(),
    "cancelled": set(),
    "superseded": set(),
}

SUBTASK_TRANSITIONS = {
    "pending": {"running", "succeeded", "failed", "skipped"},
    "running": {"succeeded", "failed", "skipped"},
    "succeeded": set(),
    "failed": set(),
    "skipped": set(),
}


def check_job_transition(current: str, target: str) -> None:
    if target not in JOB_TRANSITIONS.get(current, set()):
        logger.warning("illegal job transition denied: %s -> %s", current, target)
        raise IllegalTransition(f"illegal job transition: {current} -> {target}")


def check_subtask_transition(current: str, target: str) -> None:
    if target not in SUBTASK_TRANSITIONS.get(current, set()):
        logger.warning("illegal subtask transition denied: %s -> %s", current, target)
        raise IllegalTransition(f"illegal subtask transition: {current} -> {target}")


def validate_subtask_status(status: str) -> None:
    if status not in SUBTASK_STATUSES:
        raise ValueError(f"invalid subtask status: {status!r}")


def is_subtask_terminal(status: str) -> bool:
    return status in TERMINAL_SUBTASK_STATUSES


def aggregate_subtasks(subtask_statuses: list[str]) -> str:
    # Aggregation rule at ack time:
    #   all succeeded               -> succeeded
    #   mixed succeeded and failed  -> partial_succeeded
    #   all failed                  -> nack path (caller retries); returned as "failed"
    #   empty                       -> succeeded (no subtasks reported, job itself succeeded)
    if not subtask_statuses:
        return "succeeded"
    ok = [s for s in subtask_statuses if s in ("succeeded", "skipped")]
    failed = [s for s in subtask_statuses if s == "failed"]
    if not failed:
        return "succeeded"
    if ok:
        return "partial_succeeded"
    return "failed"
