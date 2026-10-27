from datetime import timedelta

from fusion_flow.models import JobSpec


def test_subscriber_receives_events_post_commit(flow):
    seen = []
    flow.on_event(lambda e: seen.append(e))

    flow.enqueue(JobSpec(kind="import", payload={"x": 1}))

    assert [e.event for e in seen] == ["enqueued"]
    assert seen[0].job_id is not None
    assert seen[0].data["kind"] == "import"


def test_subscriber_can_unsubscribe(flow):
    seen = []
    cb = lambda e: seen.append(e)  # noqa: E731
    flow.on_event(cb)
    flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    flow.event_bus.unsubscribe(cb)
    flow.enqueue(JobSpec(kind="import", payload={"x": 2}))
    assert len(seen) == 1


def test_broken_subscriber_does_not_block_enqueue(flow):
    def bad(e):
        raise RuntimeError("hook bug")

    flow.on_event(bad)
    # enqueue must succeed even though the hook blows up
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    assert job.status == "queued"


def test_list_events_filters_and_orders(flow):
    a = flow.enqueue(JobSpec(kind="import", payload={"x": 1}, dedup_key="k1"))
    b = flow.enqueue(JobSpec(kind="import", payload={"x": 2}, dedup_key="k2"))
    flow.queue.claim("w1")
    flow.queue.claim("w1")
    flow.queue.ack(a.id, "succeeded")

    a_events = [e.event for e in flow.events(job_id=a.id)]
    assert "enqueued" in a_events and "ack" in a_events
    b_events = [e.event for e in flow.events(job_id=b.id)]
    assert "ack" not in b_events

    # global feed is newest-first and capped by limit
    feed = flow.events(limit=3)
    assert len(feed) == 3
    assert feed[0].ts >= feed[-1].ts


def test_cleanup_respects_event_retention(flow, fake_clock):
    flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    assert flow.events(job_id=None, limit=10)
    fake_clock.advance(15 * 86400)  # past the 14-day default retention
    deleted = flow.event_bus.cleanup()
    assert deleted >= 1
    # enqueued event purged; only the cleanup-later rows survive
    assert flow.events(limit=10) == []


def test_kv_store_roundtrip_and_ttl(flow, fake_clock):
    flow.event_bus.kv_set("cursor", "page-7")
    assert flow.event_bus.kv_get("cursor") == "page-7"
    flow.event_bus.kv_set("cursor", "page-8", ttl=timedelta(minutes=5))
    assert flow.event_bus.kv_get("cursor") == "page-8"
    fake_clock.advance(301)
    flow.event_bus.cleanup()
    assert flow.event_bus.kv_get("cursor") is None
    flow.event_bus.kv_set("gone", "x")
    flow.event_bus.kv_delete("gone")
    assert flow.event_bus.kv_get("gone") is None
