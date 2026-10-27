import anyio
import pytest

from fusion_flow.errors import PermanentError, TransientError, WorkerLockHeld
from fusion_flow.flow import Flow
from fusion_flow.models import JobSpec


async def wait_until(predicate, timeout: float = 5.0, interval: float = 0.01):
    deadline = anyio.current_time() + timeout
    while anyio.current_time() < deadline:
        if predicate():
            return
        await anyio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def test_async_handler_success(flow):
    @flow.handler("double")
    async def h(ctx, payload):
        ctx.report_subtask("calc", "succeeded")
        ctx.progress({"halfway": True})
        return "ref://done"

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="double", payload={"x": 2}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "succeeded")
    view = flow.get_job(job.id)
    assert view.job.result_ref == "ref://done"
    assert view.subtasks[0].status == "succeeded"
    events = [e.event for e in flow.events(job_id=job.id)]
    assert "enqueued" in events and "ack" in events and "progress" in events


async def test_sync_handler_runs_in_thread(flow):
    import threading

    seen_threads = []

    @flow.handler("sync_job")
    def h(ctx, payload):
        seen_threads.append(threading.current_thread().name)
        return "sync-result"

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="sync_job", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "succeeded")
    assert flow.get_job(job.id).job.result_ref == "sync-result"
    assert all(name != "MainThread" for name in seen_threads)


async def test_transient_error_retries_then_succeeds(flow):
    calls = []

    @flow.handler("flaky")
    async def h(ctx, payload):
        calls.append(ctx.job.attempts)
        if len(calls) < 2:
            raise TransientError("network hiccup")
        return "ok"

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="flaky", payload={"x": 1}))
        # wait for the FIRST nack to land (attempts increments at nack, not
        # before) — advancing early would shift the nack's own next_retry_at
        await wait_until(lambda: flow.get_job(job.id).job.attempts >= 1)
        # backoff (~24-36s) blocks the re-claim; advance the fake clock past it
        flow.clock.advance(60)
        await wait_until(lambda: flow.get_job(job.id).job.status == "succeeded")
    assert flow.get_job(job.id).job.attempts == 1
    assert len(calls) == 2


async def test_permanent_error_goes_failed(flow):
    @flow.handler("bad_input")
    async def h(ctx, payload):
        raise PermanentError("schema mismatch")

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="bad_input", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "failed")
    j = flow.get_job(job.id).job
    assert j.last_error == "schema mismatch"
    assert j.attempts == 0


async def test_fail_fast_dead_letters_with_fatal_event(flow):
    @flow.handler("bomb", fail_fast=True)
    async def h(ctx, payload):
        raise RuntimeError("explode")

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="bomb", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "dead_lettered")
    events = [e.event for e in flow.events(job_id=job.id)]
    # dead_letter flag routes to dead_lettered status with a FATAL alert event
    assert "fatal" in events


async def test_unknown_kind_dead_letters(flow):
    async with flow.run():
        job = flow.enqueue(JobSpec(kind="no_handler_for_this", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "dead_lettered")
    assert "unknown kind" in flow.get_job(job.id).job.last_error


async def test_subtask_mixed_aggregation(flow):
    @flow.handler("multi_step")
    async def h(ctx, payload):
        ctx.report_subtask("a", "succeeded")
        ctx.report_subtask("b", "failed", {"line": 3})
        ctx.report_subtask("c", "succeeded")

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="multi_step", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "partial_succeeded")


def test_keyboardinterrupt_stops_pool_without_nack(flow):
    # Sync test on purpose: asyncio hijacks KeyboardInterrupt at the Task layer
    # (Task.__step re-raises it straight out of the event loop), so the only
    # place pytest can catch it is outside anyio.run itself.
    @flow.handler("interrupted")
    async def h(ctx, payload):
        raise KeyboardInterrupt()

    holder = {}

    async def scenario():
        async with flow.run():
            job = flow.enqueue(JobSpec(kind="interrupted", payload={"x": 1}))
            holder["id"] = job.id
            await anyio.sleep(0.3)

    with pytest.raises(KeyboardInterrupt):
        anyio.run(scenario)
    # never nacked: job stays processing, claim recovery owns it later
    assert flow.get_job(holder["id"]).job.status == "processing"


async def test_second_flow_run_rejected_by_worker_lock(flow, db_path, fake_clock):
    async with flow.run():
        other = Flow(db_path, clock=fake_clock)
        try:
            with pytest.raises(WorkerLockHeld):
                async with other.run():
                    pass
        finally:
            other.close()


async def test_sync_handler_drain_abandon(flow_factory_drain_short):
    # Sync handler outlives the drain timeout: job is ABANDONED in processing
    # (thread cannot be stopped), never nacked — claim recovery owns it later.
    import time

    flow = flow_factory_drain_short

    @flow.handler("slow_sync")
    def h(ctx, payload):
        time.sleep(1.5)
        return "late"

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="slow_sync", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "processing")
    assert flow.get_job(job.id).job.status == "processing"


async def test_broken_event_callback_cannot_break_processing(flow):
    def bad_callback(event):
        raise RuntimeError("subscriber bug")

    flow.on_event(bad_callback)

    @flow.handler("still_works")
    async def h(ctx, payload):
        return "fine"

    async with flow.run():
        job = flow.enqueue(JobSpec(kind="still_works", payload={"x": 1}))
        await wait_until(lambda: flow.get_job(job.id).job.status == "succeeded")


async def test_rate_limit_gaps_dispatch(flow):
    # 1/s on this kind: two jobs -> second dispatch waits ~1s of REAL time
    flow.rate.set_rate("limited", "1/s")

    starts = []

    @flow.handler("limited")
    async def h(ctx, payload):
        starts.append(anyio.current_time())

    async with flow.run():
        flow.enqueue(JobSpec(kind="limited", payload={"x": 1}))
        flow.enqueue(JobSpec(kind="limited", payload={"x": 2}))
        await wait_until(lambda: len(starts) == 2, timeout=5)
    assert starts[1] - starts[0] >= 0.8
