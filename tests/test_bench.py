import time

import pytest

from fusion_flow.models import JobSpec

pytestmark = pytest.mark.bench


def test_enqueue_claim_ack_throughput(flow, fake_clock):
    # CI-only smoke bench (run with -m bench): 1000 enqueue + claim + ack
    # cycles must stay well under a second per 100 jobs on any dev machine.
    n = 1000
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


def test_enqueue_p99_latency(flow, fake_clock):
    # Plan §9.3: enqueue 1000 p99. Per-op wall time, p99 must stay in the
    # single-digit milliseconds (each enqueue is 2 INSERTs in one txn + a
    # dedup index probe).
    n = 1000
    flow.queue.config.max_queue_depth = n + 10
    timings = []
    for i in range(n):
        t0 = time.perf_counter()
        flow.enqueue(JobSpec(kind="bench", payload={"i": i}))
        timings.append(time.perf_counter() - t0)

    timings.sort()
    p50_ms = timings[n // 2] * 1000
    p99_ms = timings[int(n * 0.99)] * 1000
    print(f"\nenqueue p50: {p50_ms:.3f}ms, p99: {p99_ms:.3f}ms")
    assert p99_ms < 10.0, f"enqueue p99 too slow: {p99_ms:.3f}ms"


def test_dequeue_latency_ratio_100k_rows(db_path, fake_clock):
    # Plan §9.3: dequeue latency on a 100k-row database vs near-empty must
    # stay under 2x — that is the whole point of idx_flow_jobs_dequeue.
    from fusion_flow.flow import Flow

    def claim_ms(flow):
        flow.enqueue(JobSpec(kind="bench", payload={"probe": 1}))
        t0 = time.perf_counter()
        job = flow.queue.claim("bench-w")
        elapsed = (time.perf_counter() - t0) * 1000
        assert job is not None
        flow.queue.ack(job.id, "succeeded", None)
        return elapsed

    # Baseline: near-empty table.
    flow = Flow(db_path, clock=fake_clock, workers=1)
    try:
        flow.queue.config.max_queue_depth = 200_000
        empty_ms = claim_ms(flow)
        empty_ms = min(empty_ms, claim_ms(flow))

        # Seed 100k terminal rows directly via storage (bench-only seeding:
        # 100k enqueue() calls would take minutes for the same table shape).
        conn = flow.queue.storage.connection()
        rows = [
            (
                f"flw_seed_{i}",
                "seed",
                "{}",
                None,
                None,
                "succeeded",
                0,
                0,
                0,
                "2026-01-01T00:00:00",
            )
            for i in range(100_000)
        ]
        with conn:
            conn.executemany(
                """INSERT INTO flow_jobs
                   (id, kind, payload, dedup_key, dedup_fingerprint, status,
                    priority, attempts, max_retries, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
        assert flow.stats().by_status["succeeded"] >= 100_000

        full_ms = claim_ms(flow)
        full_ms = min(full_ms, claim_ms(flow))
    finally:
        flow.close()

    ratio = full_ms / empty_ms
    print(f"\ndequeue empty: {empty_ms:.3f}ms, 100k rows: {full_ms:.3f}ms, ratio: {ratio:.2f}x")
    assert ratio < 2.0, f"dequeue degraded with table size: {ratio:.2f}x"
