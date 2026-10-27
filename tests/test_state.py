import pytest

from fusion_flow.errors import IllegalTransition
from fusion_flow.state import (
    aggregate_subtasks,
    check_job_transition,
    check_subtask_transition,
    validate_subtask_status,
)


@pytest.mark.parametrize(
    "current,target",
    [
        ("queued", "processing"),
        ("queued", "cancelled"),
        ("processing", "succeeded"),
        ("processing", "dead_lettered"),
        ("processing", "queued"),
    ],
)
def test_legal_job_transitions(current, target):
    check_job_transition(current, target)


@pytest.mark.parametrize(
    "current,target",
    [
        ("succeeded", "queued"),
        ("failed", "processing"),
        ("cancelled", "processing"),
        ("dead_lettered", "processing"),
    ],
)
def test_illegal_job_transitions(current, target):
    with pytest.raises(IllegalTransition):
        check_job_transition(current, target)


def test_subtask_transitions():
    check_subtask_transition("running", "succeeded")
    with pytest.raises(IllegalTransition):
        check_subtask_transition("succeeded", "running")


def test_invalid_subtask_status():
    with pytest.raises(ValueError):
        validate_subtask_status("exploded")


@pytest.mark.parametrize(
    "statuses,expected",
    [
        ([], "succeeded"),
        (["succeeded"], "succeeded"),
        (["succeeded", "skipped"], "succeeded"),
        (["succeeded", "failed"], "partial_succeeded"),
        (["failed", "failed"], "failed"),
    ],
)
def test_aggregate_subtasks(statuses, expected):
    assert aggregate_subtasks(statuses) == expected
