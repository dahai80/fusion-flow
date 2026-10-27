import fcntl
import subprocess
import sys
import textwrap
import time

from fusion_flow.clock import Clock
from fusion_flow.flow import Flow
from fusion_flow.models import JobSpec

# Child process: claims the job, then dies hard mid-processing — os._exit runs
# no cleanup, no finally, no nack: the closest in-process equivalent of kill -9.
CHILD_SCRIPT = textwrap.dedent(
    """
    import os
    import sys

    import anyio

    from fusion_flow.flow import Flow

    db, marker, timeout = sys.argv[1], sys.argv[2], float(sys.argv[3])
    flow = Flow(db, workers=1, claim_timeout_s=timeout, poll_interval=0.01,
                scheduler_tick_interval=0.05)

    @flow.handler("crash_job")
    async def h(ctx, payload):
        with open(marker, "w") as f:
            f.write(ctx.job.id)
        os._exit(9)

    async def main():
        async with flow.run():
            await anyio.sleep(30)

    anyio.run(main)
    """
)


def test_kill9_mid_processing_self_heals(tmp_path):
    db = str(tmp_path / "crash.db")
    marker = tmp_path / "marker"
    clock = Clock(seed=7)
    flow = Flow(db, clock=clock)
    job = flow.enqueue(JobSpec(kind="crash_job", payload={"x": 1}))

    proc = subprocess.Popen(
        [sys.executable, "-c", CHILD_SCRIPT, db, str(marker), "1.0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        # wait for the child to claim the job and die mid-processing
        deadline = time.time() + 15
        while not marker.exists() and time.time() < deadline:
            time.sleep(0.02)
        assert marker.exists(), "child never reached the handler"
        proc.wait(timeout=10)
        assert proc.returncode == 9

        # orphaned claim: job stuck in processing, owned by a dead worker
        stuck = flow.get_job(job.id).job
        assert stuck.status == "processing"
        assert stuck.claimed_by is not None

        # claim expiry (1s) passes -> recovery requeues with attempts+1
        clock.advance(2)
        recovered = flow.queue.recover_expired_claims()
        assert recovered == [job.id]
        j = flow.get_job(job.id).job
        assert j.status == "queued"
        assert j.attempts == 1
        assert j.last_error == "claim timeout (recovered)"
        assert j.claimed_by is None

        # the healed job is claimable again by a live worker
        reclaimed = flow.queue.claim("survivor")
        assert reclaimed is not None
        assert reclaimed.id == job.id
        flow.queue.ack(reclaimed.id, "succeeded", "ref://after-crash")
        assert flow.get_job(job.id).job.status == "succeeded"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    flow.close()


def test_worker_lock_file_survives_crash_and_is_reacquirable(tmp_path):
    # The sidecar lock lives in a FILE, not the DB: a kill -9 cannot leave a
    # stale row. The OS releases the fd, so the next process acquires it freely.
    db = str(tmp_path / "lock.db")
    lock_path = f"{db}.worker.lock"
    flow = Flow(db, clock=Clock(seed=9))
    flow.lock.acquire()
    assert flow.lock._fh is not None

    # simulate the crash: close the fd WITHOUT releasing (what kill -9 does)
    flow.lock._fh.close()
    flow.lock._fh = None
    # the OS released the flock with the fd — no stale lock, no cleanup step
    assert not _flock_held(lock_path)

    # next process acquires without any cleanup step
    flow.lock.acquire()
    flow.lock.release()
    flow.close()


def _flock_held(lock_path: str) -> bool:
    with open(lock_path, "a+") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            return False
        except OSError:
            return True
