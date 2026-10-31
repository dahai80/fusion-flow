import json
import logging
import os
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import anyio

from .clock import Clock, RealClock
from .connector import (
    DEFAULT_CONNECTOR_BACKOFF_BASE_S,
    DEFAULT_CONNECTOR_BACKOFF_CAP_S,
    DEFAULT_CONNECTOR_INTERVAL_S,
    DEFAULT_ON_DONE_RETRIES,
    ConnectorRunner,
)
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


def _boot_id() -> str:
    # Identify the current OS boot so a holder record from a previous boot is
    # provably stale: every process of that boot is gone, its flock too.
    try:
        with open("/proc/sys/kernel/random/boot_id") as fh:
            return fh.read().strip()
    except OSError:
        pass
    try:
        import re
        import subprocess

        out = subprocess.run(
            ["sysctl", "-n", "kern.boottime"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        # macOS format: '{ sec = 1793438761, usec = 613405 } Sat Oct 31 ...'
        match = re.search(r"sec\s*=\s*(\d+)", out.stdout)
        if match:
            return f"boottime:{match.group(1)}"
    except Exception:
        pass
    return "unknown"


def _pid_alive(pid: int) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


class WorkerLock:
    # Cross-process worker exclusion on <db>.worker.lock: fcntl on Unix,
    # msvcrt.locking on Windows (lazy import). Read-only CLI tools don't take
    # the lock, they only warn when it is held by a live worker.
    #
    # The lock lives in a sidecar FILE, never inside the SQLite database: a
    # kill -9 cannot leave a stale row, the OS releases the fd automatically.
    #
    # Since 0.2.2 a successful acquire also writes a holder record (pid, host,
    # boot_id, timestamp) INTO the file, so a failed acquire can report WHO
    # holds the lock and whether that holder is still alive; a holder from a
    # previous boot is auto-reclaimed (provably dead), everything else stays
    # a hard error with diagnostics — `flow unlock` is the manual escape.

    def __init__(self, db_path: str):
        self.path = f"{db_path}.worker.lock"
        self._fh = None

    @property
    def is_held(self) -> bool:
        return self._fh is not None

    def _try_flock(self) -> bool:
        fh = open(self.path, "a+")
        try:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            import msvcrt

            try:
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                fh.close()
                return False
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True

    def _write_holder(self) -> None:
        record = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "boot_id": _boot_id(),
            "acquired_at": datetime.now().isoformat(),
        }
        try:
            self._fh.seek(0)
            self._fh.truncate()
            self._fh.write(json.dumps(record))
            self._fh.flush()
        except OSError:
            logger.warning("could not write holder record to %s", self.path, exc_info=True)

    def _read_holder(self) -> dict | None:
        try:
            with open(self.path) as fh:
                data = fh.read().strip()
            if not data:
                return None
            holder = json.loads(data)
            return holder if isinstance(holder, dict) else None
        except (OSError, ValueError):
            return None

    def _holder_stale(self, holder: dict) -> bool:
        # Only a boot change PROVES staleness. A same-boot dead pid can be a
        # reused pid or an inherited fd in a still-live child process, so it
        # is never auto-reclaimed — operator runs `flow unlock` instead.
        boot = holder.get("boot_id")
        return boot not in (None, "unknown") and boot != _boot_id()

    def _holder_detail(self, holder: dict | None) -> str:
        if not holder:
            return (
                "no holder record in file (lock file without identity); "
                "run `flow unlock --force` if stale"
            )
        pid = holder.get("pid")
        host = holder.get("host", "?")
        since = holder.get("acquired_at", "?")
        alive = holder.get("boot_id") == _boot_id() and _pid_alive(pid)
        state = "alive" if alive else "dead or from a previous boot"
        return (
            f"held by pid={pid} on {host} since {since} ({state}); "
            f"run `flow unlock` if the holder is gone"
        )

    def acquire(self) -> None:
        if self._try_flock():
            self._write_holder()
            return
        holder = self._read_holder()
        if holder is not None and self._holder_stale(holder):
            logger.warning(
                "reclaiming stale worker lock %s (holder pid=%s from a previous boot)",
                self.path,
                holder.get("pid"),
            )
            try:
                os.unlink(self.path)
            except OSError:
                logger.warning("failed to unlink stale lock %s", self.path, exc_info=True)
                raise WorkerLockHeld(
                    f"worker lock held: {self.path}; {self._holder_detail(holder)}"
                ) from None
            if self._try_flock():
                self._write_holder()
                return
        raise WorkerLockHeld(f"worker lock held: {self.path}; {self._holder_detail(holder)}")

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

    @classmethod
    def unlock(cls, db_path: str, *, force: bool = False) -> dict:
        # Operator escape hatch for a lock file whose holder is provably gone.
        # Refuses while a live holder holds it, unless force=True.
        path = f"{db_path}.worker.lock"
        if not os.path.exists(path):
            return {"removed": False, "reason": "no lock file"}
        holder = None
        try:
            with open(path) as fh:
                data = fh.read().strip()
            if data:
                parsed = json.loads(data)
                holder = parsed if isinstance(parsed, dict) else None
        except (OSError, ValueError):
            holder = None
        flock_free = False
        try:
            with open(path, "a+") as fh:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                flock_free = True
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except (OSError, ImportError):
            flock_free = False
        if flock_free:
            reason = "stale lock file (flock free)"
        elif holder is not None and (
            holder.get("boot_id") != _boot_id() or not _pid_alive(holder.get("pid"))
        ):
            # Holder record proves the original holder is gone. If the flock
            # is STILL taken, an inherited fd in a live child holds the old
            # inode — removing the file lets the next worker use a fresh one.
            reason = "holder gone (flock possibly held by an inherited fd)"
        elif force:
            reason = "forced"
        else:
            return {
                "removed": False,
                "reason": "lock appears held by a live holder; use --force to override",
                "holder": holder,
            }
        try:
            os.unlink(path)
        except OSError:
            logger.warning("unlock failed to unlink %s", path, exc_info=True)
            return {"removed": False, "reason": f"unlink failed: {path}"}
        logger.warning("worker lock %s removed (%s)", path, reason)
        return {"removed": True, "reason": reason, "holder": holder}


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
        self.connectors = []
        logger.info("flow initialized db=%s", self.db_path)

    def attach_connector(
        self,
        connector,
        *,
        name: str | None = None,
        interval_s: float = DEFAULT_CONNECTOR_INTERVAL_S,
        kinds: set[str] | None = None,
        backoff_base_s: float = DEFAULT_CONNECTOR_BACKOFF_BASE_S,
        backoff_cap_s: float = DEFAULT_CONNECTOR_BACKOFF_CAP_S,
        on_done_retries: int = DEFAULT_ON_DONE_RETRIES,
    ) -> None:
        # Register before run(); run() spawns one ConnectorRunner task per
        # connector inside the worker task group (same lock, same drain).
        # kinds=None means on_job_done sees EVERY acked job — scope it to the
        # kinds this connector creates to avoid useless reverse-path calls.
        runner = ConnectorRunner(
            name or type(connector).__name__,
            connector,
            self.queue,
            self.event_bus,
            clock=self.clock,
            interval_s=interval_s,
            kinds=kinds,
            backoff_base_s=backoff_base_s,
            backoff_cap_s=backoff_cap_s,
            on_done_retries=on_done_retries,
        )
        self.connectors.append(runner)
        logger.info("attached connector %s interval=%ss", runner.name, interval_s)

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

    @property
    def workers_active(self) -> bool:
        # True only between run() entry and run() exit. A health check can
        # watch this: enqueue keeps succeeding after a worker thread dies,
        # this flag is what says the worker loop is actually gone.
        return self.lock.is_held

    @asynccontextmanager
    async def run(self):
        # Only ONE process may run workers per database — the sidecar lock makes
        # a second Flow.run() fail fast with WorkerLockHeld instead of racing.
        self.lock.acquire()
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.pool.run)
                for runner in self.connectors:
                    tg.start_soon(runner.run)
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
