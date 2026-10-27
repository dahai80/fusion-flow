import pytest

from fusion_flow.models import JobSpec

pytestmark = pytest.mark.bench


def test_enqueue_claim_ack_throughput(flow, fake_clock):
    # CI-only smoke bench (run with -m bench): 500 enqueue + claim + ack
    # cycles must stay well under a second per 100 jobs on any dev machine.
    import time

    n = 500
    flow.queue.config.max_queue_depth = n + 10
    t0 = time.perf_counter()
    for i in range(n):
        flow.enqueue(JobSpec(kind="bench", payload={"i": i}))
    t_enq = time.perf_counter()
    for _ in range(n):
        job = flow.queue.claim("bench-w")
        assert job is not None
        flow.queue.ack(job.id, "succeeded", None)
    t_end = time.perf_counter()

    enq_ms = (t_enq - t0) * 1000
    cycle_ms = (t_end - t_enq) * 1000
    per_100_cycle = cycle_ms / n * 100
    print(f"\nenqueue: {enq_ms:.0f}ms total, cycle: {per_100_cycle:.2f}ms per 100 jobs")
    assert per_100_cycle < 1000.0, f"claim+ack too slow: {per_100_cycle:.2f}ms per 100 jobs"
    assert flow.stats().by_status["succeeded"] == n
