from datetime import timedelta

import pytest

from fusion_flow.errors import IllegalTransition, QueueFull
from fusion_flow.models import JobSpec


def test_enqueue_and_get(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"file": "a.txt"}))
    assert job.status == "queued"
    view = flow.get_job(job.id)
    assert view is not None
    assert view.job.kind == "import"
    assert view.job.payload == {"file": "a.txt"}


def test_dedup_reuses_active_job(flow):
    a = flow.enqueue(JobSpec(kind="import", payload={"x": 1}, dedup_key="file:1"))
    b = flow.enqueue(JobSpec(kind="import", payload={"x": 2}, dedup_key="file:1"))
    assert b.id == a.id
    assert not b.created


def test_dedup_allows_new_job_after_terminal(flow):
    a = flow.enqueue(JobSpec(kind="import", payload={"x": 1}, dedup_key="file:1"))
    flow.queue.claim("w1")
    flow.queue.ack(a.id, "succeeded", "ref://1")
    b = flow.enqueue(JobSpec(kind="import", payload={"x": 2}, dedup_key="file:1"))
    assert b.id != a.id
    assert b.created


def test_dedup_ttl_reuses_succeeded_result(flow):
    a = flow.enqueue(
        JobSpec(kind="import", payload={"x": 1}, dedup_key="file:1", dedup_ttl=timedelta(minutes=5))
    )
    claimed = flow.queue.claim("w1")
    flow.queue.ack(claimed.id, "succeeded", "ref://cached")
    b = flow.enqueue(
        JobSpec(kind="import", payload={"x": 2}, dedup_key="file:1", dedup_ttl=timedelta(minutes=5))
    )
    assert b.id == a.id
    assert not b.created


def test_dedup_ttl_expires(flow):
    flow.enqueue(
        JobSpec(kind="import", payload={"x": 1}, dedup_key="file:1", dedup_ttl=timedelta(minutes=5))
    )
    claimed = flow.queue.claim("w1")
    flow.queue.ack(claimed.id, "succeeded", "ref://cached")
    flow.clock.advance(301)
    b = flow.enqueue(
        JobSpec(kind="import", payload={"x": 2}, dedup_key="file:1", dedup_ttl=timedelta(minutes=5))
    )
    assert b.created


def test_queue_full_backpressure(flow):
    flow.queue.config.max_queue_depth = 2
    flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.enqueue(JobSpec(kind="import", payload={"x": 2}))
    with pytest.raises(QueueFull):
        flow.enqueue(JobSpec(kind="import", payload={"x": 3}))


def test_claim_respects_priority(flow):
    flow.enqueue(JobSpec(kind="import", payload={"x": 1}, priority=200))
    flow.enqueue(JobSpec(kind="import", payload={"x": 2}, priority=1))
    job = flow.queue.claim("w1")
    assert job.payload["x"] == 2


def test_claim_respects_next_retry_at(flow, fake_clock):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    flow.queue.nack(job.id, "boom")
    assert flow.queue.claim("w1") is None
    fake_clock.advance(3600)
    again = flow.queue.claim("w1")
    assert again is not None
    assert again.id == job.id


def test_ack_denied_when_not_processing(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    with pytest.raises(IllegalTransition):
        flow.queue.ack(job.id, "succeeded")


def test_nack_counts_attempts_and_eventually_dead_letters(flow, fake_clock):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}, max_retries=2))
    flow.queue.claim("w1")
    j = flow.queue.nack(job.id, "boom")
    assert j.status == "queued"
    assert j.attempts == 1
    assert j.last_error == "boom"
    # backoff pushes next_retry_at into the future; advance past it
    fake_clock.advance(3600)
    flow.queue.claim("w1")
    j = flow.queue.nack(job.id, "boom again")
    assert j.status == "dead_lettered"
    assert j.attempts == 2
    assert j.is_terminal


def test_permanent_fail_is_failed_not_dead_letter(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    j = flow.queue.permanent_fail(job.id, "bad input")
    assert j.status == "failed"
    assert j.last_error == "bad input"


def test_permanent_fail_dead_letter_writes_fatal_event(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    flow.queue.permanent_fail(job.id, "fatal!", dead_letter=True)
    events = flow.events(job_id=job.id)
    assert any(e.event == "fatal" for e in events)


def test_cancel_queued_job(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    j = flow.cancel(job.id)
    assert j.status == "cancelled"


def test_cancel_succeeded_denied(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    flow.queue.ack(job.id, "succeeded")
    with pytest.raises(IllegalTransition):
        flow.cancel(job.id)


def test_retry_and_replay_copy(flow):
    ok = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    flow.queue.ack(ok.id, "succeeded", "ref://1")
    replayed = flow.replay(ok.id)
    assert replayed.id != ok.id
    assert replayed.status == "queued"
    assert replayed.attempts == 0
    assert replayed.payload == ok.payload
    assert flow.get_job(ok.id).job.status == "succeeded"

    # retry path: dead-lettered job gets a fresh copy with attempts reset
    flow.queue.claim("w2")
    dead = flow.get_job(replayed.id).job
    flow.queue.nack(dead.id, "boom", retryable=False)
    fresh = flow.retry(dead.id)
    assert fresh.id != dead.id
    assert fresh.attempts == 0
    assert fresh.status == "queued"


def test_retry_denied_for_active_job(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    with pytest.raises(IllegalTransition):
        flow.retry(job.id)


def test_expired_claim_recovery_requeues(flow, fake_clock):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    fake_clock.advance(601)
    recovered = flow.queue.recover_expired_claims()
    assert recovered == [job.id]
    j = flow.get_job(job.id).job
    assert j.status == "queued"
    assert j.attempts == 1


def test_expired_claim_recovery_dead_letters_after_max(flow, fake_clock):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}, max_retries=1))
    flow.queue.claim("w1")
    fake_clock.advance(601)
    recovered = flow.queue.recover_expired_claims()
    assert recovered == []
    j = flow.get_job(job.id).job
    assert j.status == "dead_lettered"


def test_subtask_report_and_aggregation(flow):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    ctx_job = flow.queue.claim("w1")
    flow.queue.report_subtask(ctx_job.id, "download", "succeeded")
    flow.queue.report_subtask(ctx_job.id, "parse", "failed", {"line": 42})
    flow.queue.report_subtask(ctx_job.id, "notify", "running")
    flow.queue.report_subtask(ctx_job.id, "notify", "succeeded")
    j = flow.queue.ack(ctx_job.id, "partial_succeeded", "ref://out")
    assert j.status == "partial_succeeded"
    view = flow.get_job(job.id)
    assert len(view.subtasks) == 3


def test_subtask_terminal_cannot_reopen(flow):
    flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    claimed = flow.queue.claim("w2")
    flow.queue.report_subtask(claimed.id, "step", "succeeded")
    with pytest.raises(IllegalTransition):
        flow.queue.report_subtask(claimed.id, "step", "failed")


def test_purge_removes_old_terminal_jobs(flow, fake_clock):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.queue.claim("w1")
    flow.queue.ack(job.id, "succeeded")
    fake_clock.advance(8 * 86400)
    count = flow.purge(timedelta(days=7), ["succeeded"])
    assert count == 1
    assert flow.get_job(job.id) is None


def test_empty_claim_releases_write_lock(db_path, fake_clock):
    # Issue #1: a worker claiming on an empty queue opens an implicit write
    # txn (legacy isolation: 0-row DML still starts one). Returning without
    # committing leaves the write lock held and blocks same-process enqueue
    # from other threads until the next successful claim.
    import threading

    from fusion_flow.flow import Flow

    flow = Flow(
        db_path,
        clock=fake_clock,
        workers=1,
        sqlite_pragmas={"busy_timeout": 200},
    )
    try:
        claimed = threading.Event()
        release = threading.Event()

        def worker_claim():
            assert flow.queue.claim("w-lock") is None
            claimed.set()
            release.wait(5)

        t = threading.Thread(target=worker_claim)
        t.start()
        try:
            assert claimed.wait(5)
            # Same-process enqueue from another thread must NOT hit
            # "database is locked" while the worker idles between claims.
            flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
        finally:
            release.set()
            t.join()
    finally:
        flow.close()
