import anyio
import pytest

from fusion_flow import JobSpec
from fusion_flow.errors import PayloadTooLarge


class FakeConnector:
    # Scriptable connector: pops specs per poll, optional poll exception,
    # optional on_job_done failures (first N calls raise).
    def __init__(self, specs=None, poll_exc=None, done_failures=0):
        self.specs = list(specs or [])
        self.poll_exc = poll_exc
        self.done_failures = done_failures
        self.poll_calls = 0
        self.commit_calls = 0
        self.done_calls = []

    async def poll(self):
        self.poll_calls += 1
        if self.poll_exc is not None:
            raise self.poll_exc
        return list(self.specs)

    async def commit(self):
        # Models the connector cursor: committed specs are never re-delivered.
        self.commit_calls += 1
        self.specs = []

    async def on_job_done(self, job, result_ref):
        self.done_calls.append((job.id, job.kind, result_ref))
        if self.done_failures > 0:
            self.done_failures -= 1
            raise RuntimeError("upload failed")


async def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.01):
    deadline = anyio.current_time() + timeout
    while anyio.current_time() < deadline:
        if predicate():
            return
        await anyio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def test_runner_enqueues_specs_and_commits(flow):
    conn = FakeConnector(
        specs=[JobSpec(kind="photo", payload={"file_id": "f1"}, dedup_key="drive:f1")]
    )
    flow.attach_connector(conn, name="fake", interval_s=0.05, backoff_base_s=0.01)

    @flow.handler("photo")
    async def photo(ctx, payload):
        return "ref://done"

    async with flow.run():
        await _wait_until(lambda: conn.commit_calls >= 1)
        await _wait_until(lambda: flow.stats().by_status.get("succeeded", 0) >= 1)
        assert conn.done_calls and conn.done_calls[0][2] == "ref://done"


async def test_queue_full_fails_round_and_never_commits(db_path, fake_clock):
    from fusion_flow.flow import Flow

    flow = Flow(
        db_path,
        clock=fake_clock,
        workers=1,
        scheduler_tick_interval=0.05,
        poll_interval=0.01,
        max_queue_depth=1,
    )
    try:
        # Depth 1 is already occupied -> every connector enqueue hits QueueFull.
        flow.enqueue(JobSpec(kind="photo", payload={"i": 0}))
        conn = FakeConnector(specs=[JobSpec(kind="photo", payload={"i": 1})])
        flow.attach_connector(conn, name="fake", interval_s=0.02, backoff_base_s=0.01)

        @flow.handler("photo")
        async def photo(ctx, payload):
            await anyio.sleep(0.5)
            return "ref://x"

        async with flow.run():
            await _wait_until(lambda: conn.poll_calls >= 2)
            # Whole round failed: token never advanced, spec never stored —
            # the queue still holds ONLY the manually enqueued job.
            assert conn.commit_calls == 0
            assert len(flow.list_jobs()) == 1
            events = [e.event for e in flow.events(limit=200)]
            assert "connector_error" in events
            assert "queue_full" in events
    finally:
        flow.close()


async def test_b3_poll_failure_does_not_block_workers(flow):
    # B3: connector down (poll always raises) while manually enqueued jobs
    # still get consumed — structural isolation between the two task groups.
    conn = FakeConnector(poll_exc=RuntimeError("drive unreachable"))
    flow.attach_connector(conn, name="fake", interval_s=0.02, backoff_base_s=0.01)

    @flow.handler("local")
    async def local(ctx, payload):
        return "ref://ok"

    async with flow.run():
        flow.enqueue(JobSpec(kind="local", payload={"n": 1}))
        await _wait_until(lambda: flow.stats().by_status.get("succeeded", 0) >= 1)
        assert conn.poll_calls >= 1
        assert conn.commit_calls == 0
        reasons = [e.data.get("reason", "") for e in flow.events(limit=100)]
        assert any("drive unreachable" in r for r in reasons)


async def test_invalid_spec_dropped_round_still_commits(flow):
    oversized = JobSpec(kind="photo", payload={"blob": "x" * (70 * 1024)})
    good = JobSpec(kind="photo", payload={"file_id": "f9"}, dedup_key="drive:f9")
    conn = FakeConnector(specs=[oversized, good])
    flow.attach_connector(conn, name="fake", interval_s=0.05, backoff_base_s=0.01)

    @flow.handler("photo")
    async def photo(ctx, payload):
        return None

    async with flow.run():
        await _wait_until(lambda: conn.commit_calls >= 1)
        # Only the good spec reached the queue; the round still committed.
        assert len(flow.list_jobs()) == 1
        events = [e.event for e in flow.events(limit=100)]
        assert "connector_spec_dropped" in events


async def test_invalid_kind_dropped_not_poison(flow):
    # ValueError from validate_jobspec (bad kind chars) is a spec bug, not a
    # round failure: dropped, commit still happens, loop keeps running.
    bad = JobSpec(kind="bad kind!", payload={})
    good = JobSpec(kind="photo", payload={"file_id": "f10"}, dedup_key="drive:f10")
    conn = FakeConnector(specs=[bad, good])
    flow.attach_connector(conn, name="fake", interval_s=0.05, backoff_base_s=0.01)

    @flow.handler("photo")
    async def photo(ctx, payload):
        return None

    async with flow.run():
        await _wait_until(lambda: conn.commit_calls >= 1)
        assert flow.connectors[0].failures == 0
        events = [e.event for e in flow.events(limit=100)]
        assert "connector_spec_dropped" in events


async def test_on_job_done_kinds_filter(flow):
    conn_a = FakeConnector()
    conn_b = FakeConnector()
    flow.attach_connector(conn_a, name="a", interval_s=5.0, kinds={"photo"})
    flow.attach_connector(conn_b, name="b", interval_s=5.0, kinds={"video"})

    for kind in ("photo", "video"):

        @flow.handler(kind)
        async def make(ctx, payload):
            return f"ref://{ctx.job.kind}"

    async with flow.run():
        flow.enqueue(JobSpec(kind="photo", payload={"n": 1}))
        await _wait_until(lambda: len(conn_a.done_calls) >= 1)
        # Give the filtered runner a moment to (wrongly) react — it must not.
        await anyio.sleep(0.2)
        assert conn_a.done_calls[0][1] == "photo"
        assert conn_b.done_calls == []


async def test_on_job_done_retries_then_recovers(flow):
    conn = FakeConnector(done_failures=1)
    flow.attach_connector(conn, name="fake", interval_s=5.0, on_done_retries=3, backoff_base_s=0.01)

    @flow.handler("photo")
    async def photo(ctx, payload):
        return "ref://done"

    async with flow.run():
        flow.enqueue(JobSpec(kind="photo", payload={"n": 1}))
        await _wait_until(lambda: len(conn.done_calls) >= 2)
        # First attempt raised, second succeeded: job result NOT rolled back.
        assert flow.stats().by_status["succeeded"] == 1
        await anyio.sleep(0.1)
        reasons = [e.data.get("reason", "") for e in flow.events(limit=100)]
        assert not any("exhausted" in r for r in reasons)


async def test_on_job_done_exhausted_alerts_and_keeps_result(flow):
    conn = FakeConnector(done_failures=99)
    flow.attach_connector(conn, name="fake", interval_s=5.0, on_done_retries=2, backoff_base_s=0.01)

    @flow.handler("photo")
    async def photo(ctx, payload):
        return "ref://done"

    async with flow.run():
        flow.enqueue(JobSpec(kind="photo", payload={"n": 1}))
        await _wait_until(
            lambda: any("exhausted" in (e.data.get("reason") or "") for e in flow.events(limit=100))
        )
        # Reverse-path failure never rolls back the terminal state.
        assert flow.stats().by_status["succeeded"] == 1
        assert len(conn.done_calls) == 2


def test_payload_too_large_is_flow_error():
    spec = JobSpec(kind="photo", payload={"blob": "x" * (2 * 1024 * 1024)})
    from fusion_flow.dedup import validate_jobspec

    with pytest.raises(PayloadTooLarge):
        validate_jobspec(spec, 64 * 1024)
