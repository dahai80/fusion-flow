import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import anyio

from .clock import Clock, RealClock
from .errors import WorkerLockHeld
from .event_bus import DEFAULT_EVENT_RETENTION_DAYS, EventBus
from .models import DEFAULT_PAYLOAD_MAX_BYTES, FlowEvent, Job, JobSpec, JobView, StatsSnapshot
from .queue_service import QueueConfig, QueueService
from .rate import RateLimiter
from .retry import DEFAULT_RETRY_BASE_S, DEFAULT_RETRY_CAP_S, Backoff
from .storage import Storage
from .worker_pool import WorkerConfig, WorkerPool

logger = logging.getLogger("fusion_flow")

THROUGHPUT_WINDOW_S = 86400.0


class WorkerLock:
    # Cross-process worker exclusion on <db>.worker.lock: fcntl on Unix,
    # msvcrt.locking on Windows (lazy import). Read-only CLI tools don't take
    # the lock, they only warn when it is held by a live worker.
    #
    # The lock lives in a sidecar FILE, never inside the SQLite database: a
    # kill -9 cannot leave a stale row, the OS releases the fd automatically.

    def __init__(self, db_path: str):
        self.path = f"{db_path}.worker.lock"
        self._fh = None

    def acquire(self) -> None:
        self._fh = open(self.path, "a+")
        try:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            import msvcrt

            try:
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                self._fh.close()
                self._fh = None
                raise WorkerLockHeld(f"worker lock held: {self.path}") from None
        except OSError:
            self._fh.close()
            self._fh = None
            raise WorkerLockHeld(f"worker lock held: {self.path}") from None

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except ImportError:
            import msvcrt

            self._fh.seek(0)
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._fh.close()
            self._fh = None


class Flow:
    # Public facade: build the storage/queue/rate/events/worker graph, wire the
    # event sink, own the worker lock and the run() lifecycle. Handlers are
    # registered with a decorator; enqueue/get_job/stats are sync, run() is the
    # only async entry point.

    def __init__(
        self,
        db_path: str | Path,
        *,
        clock: Clock | None = None,
        workers: int = 2,
        scheduler_tick_interval: float = 5.0,
        poll_interval: float = 0.2,
        drain_timeout_s: float = 30.0,
        thread_pool_size: int = 8,
        max_queue_depth: int = 10000,
        payload_max_bytes: int = DEFAULT_PAYLOAD_MAX_BYTES,
        claim_timeout_s: float = 600.0,
        retry_base_s: float = DEFAULT_RETRY_BASE_S,
        retry_cap_s: float = DEFAULT_RETRY_CAP_S,
        rate_default: str | None = None,
        rate_per_kind: dict[str, str] | None = None,
        sqlite_pragmas: dict | None = None,
        event_retention_days: float = DEFAULT_EVENT_RETENTION_DAYS,
        migrate: bool = True,
    ):
        self.db_path = str(db_path)
        self.clock = clock or RealClock()
        self.storage = Storage(self.db_path, sqlite_pragmas=sqlite_pragmas)
        if migrate:
            self.storage.migrate()
        self.event_bus = EventBus(
            self.storage, clock=self.clock, retention_days=event_retention_days
        )
        self.queue = QueueService(
            self.storage,
            clock=self.clock,
            backoff=Backoff(clock=self.clock, base_s=retry_base_s, cap_s=retry_cap_s),
            config=QueueConfig(
                max_queue_depth=max_queue_depth,
                payload_max_bytes=payload_max_bytes,
                claim_timeout_s=claim_timeout_s,
            ),
            event_sink=self.event_bus.dispatch,
        )
        self.rate = RateLimiter(default_rate=rate_default, per_kind=rate_per_kind, clock=self.clock)
        self.pool = WorkerPool(
            self.queue,
            self.rate,
            self.event_bus,
            clock=self.clock,
            config=WorkerConfig(
                workers=workers,
                scheduler_tick_interval=scheduler_tick_interval,
                poll_interval=poll_interval,
                drain_timeout_s=drain_timeout_s,
                thread_pool_size=thread_pool_size,
            ),
        )
        self.lock = WorkerLock(self.db_path)
        logger.info("flow initialized db=%s", self.db_path)

    def handler(self, kind: str, *, fail_fast: bool = False):
        def decorate(fn):
            self.pool.register(kind, fn, fail_fast=fail_fast)
            return fn

        return decorate

    def enqueue(self, spec: JobSpec) -> Job:
        return self.queue.enqueue(spec)

    def get_job(self, job_id: str) -> JobView | None:
        return self.queue.get_job(job_id)

    def list_jobs(
        self, status: str | None = None, kind: str | None = None, limit: int = 50
    ) -> list[Job]:
        return self.queue.list_jobs(status=status, kind=kind, limit=limit)

    def cancel(self, job_id: str) -> Job:
        return self.queue.cancel(job_id)

    def retry(self, job_id: str) -> Job:
        return self.queue.copy_to_new(job_id, "retried")

    def replay(self, job_id: str) -> Job:
        return self.queue.copy_to_new(job_id, "replayed")

    def purge(self, older_than: timedelta, statuses: list[str]) -> int:
        return self.queue.purge(older_than, statuses)

    def events(self, job_id: str | None = None, limit: int = 100) -> list[FlowEvent]:
        return self.event_bus.list_events(job_id=job_id, limit=limit)

    def on_event(self, callback) -> None:
        self.event_bus.subscribe(callback)

    def stats(self) -> StatsSnapshot:
        by_status = {
            row["status"]: row["c"]
            for row in self.storage.query_all(
                "SELECT status, COUNT(*) AS c FROM flow_jobs GROUP BY status"
            )
        }
        oldest = self.storage.query_one(
            "SELECT MIN(created_at) AS oldest FROM flow_jobs WHERE status='queued'"
        )["oldest"]
        age = None
        if oldest:
            try:
                age = (self.clock.now() - datetime.fromisoformat(oldest)).total_seconds()
            except ValueError:
                age = None
        cutoff = (self.clock.now() - timedelta(seconds=THROUGHPUT_WINDOW_S)).isoformat()
        kind_throughput = {
            row["kind"]: row["c"]
            for row in self.storage.query_all(
                "SELECT kind, COUNT(*) AS c FROM flow_jobs "
                "WHERE finished_at IS NOT NULL AND finished_at >= ? GROUP BY kind",
                (cutoff,),
            )
        }
        return StatsSnapshot(
            by_status=by_status,
            oldest_queued_age_s=age,
            dead_letter_count=by_status.get("dead_lettered", 0),
            kind_throughput=kind_throughput,
        )

    def vacuum(self) -> None:
        conn = self.storage.connection()
        with conn:
            conn.execute("VACUUM")
        logger.info("vacuumed %s", self.db_path)

    def close(self) -> None:
        self.storage.close()

    @asynccontextmanager
    async def run(self):
        # Only ONE process may run workers per database — the sidecar lock makes
        # a second Flow.run() fail fast with WorkerLockHeld instead of racing.
        self.lock.acquire()
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.pool.run)
                try:
                    yield
                finally:
                    self.pool.stop()
                    with anyio.move_on_after(self.pool.config.drain_timeout_s):
                        await self.pool.wait_done()
                    tg.cancel_scope.cancel()
        finally:
            self.lock.release()
            logger.info("flow stopped, worker lock released")
