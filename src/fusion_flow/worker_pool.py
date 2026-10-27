import inspect
import logging

import anyio

from .clock import Clock, RealClock
from .errors import PermanentError, TransientError
from .event_bus import EventBus
from .models import Job, SubTask
from .queue_service import QueueService
from .rate import RateLimiter
from .state import aggregate_subtasks

logger = logging.getLogger("fusion_flow")

DEFAULT_WORKERS = 2
DEFAULT_SCHEDULER_TICK_S = 5.0
DEFAULT_POLL_INTERVAL_S = 0.2
DEFAULT_DRAIN_TIMEOUT_S = 30.0
MAX_WORKERS = 16


class WorkerConfig:
    def __init__(
        self,
        workers: int = DEFAULT_WORKERS,
        scheduler_tick_interval: float = DEFAULT_SCHEDULER_TICK_S,
        poll_interval: float = DEFAULT_POLL_INTERVAL_S,
        drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_S,
        thread_pool_size: int = 8,
    ):
        self.workers = min(workers, MAX_WORKERS)
        self.scheduler_tick_interval = scheduler_tick_interval
        self.poll_interval = poll_interval
        self.drain_timeout_s = drain_timeout_s
        self.thread_pool_size = max(thread_pool_size, self.workers)


class TaskContext:
    # Per-job context handed to handlers. Payload is deep-copied in; handlers
    # report subtask progress and result references through here.

    def __init__(self, job: Job, queue: QueueService):
        self.job = job
        self._queue = queue
        self.result_ref = None

    def report_subtask(self, name: str, status: str, detail: dict | None = None) -> SubTask:
        return self._queue.report_subtask(self.job.id, name, status, detail)

    def set_result_ref(self, ref: str) -> None:
        self.result_ref = ref

    def progress(self, detail: dict) -> None:
        conn = self._queue.storage.connection()
        with conn:
            self._queue._emit(conn, "progress", job_id=self.job.id, data=detail)


class _InterruptMarker(BaseException):
    # to_thread cannot carry KeyboardInterrupt out of the thread group cleanly,
    # so the sync wrapper translates it and the async side re-raises.
    pass


class WorkerPool:
    # Single event loop, N worker tasks. Sync handlers run in a bounded thread
    # pool (anyio CapacityLimiter); each thread gets its own SQLite connection
    # from Storage. Exception classification is THE contract:
    #   TransientError / unexpected Exception -> nack (backoff + retry)
    #   PermanentError                        -> failed (business-terminal)
    #   fail_fast handler, any exception      -> dead_lettered + fatal event
    #   KeyboardInterrupt / SystemExit        -> stop + re-raise, NEVER nack
    # Drain (Flow cancels the task group after drain_timeout): async handlers
    # are cancelled then nacked (attempts+1, worker_drain_timeout:<reason>);
    # sync handlers are ABANDONED — the thread cannot be stopped and may still
    # write, so the job stays processing and claim-timeout recovery picks it up.

    def __init__(
        self,
        queue: QueueService,
        rate: RateLimiter,
        event_bus: EventBus,
        clock: Clock | None = None,
        config: WorkerConfig | None = None,
    ):
        self.queue = queue
        self.rate = rate
        self.event_bus = event_bus
        self.clock = clock or RealClock()
        self.config = config or WorkerConfig()
        self.handlers = {}
        self.fail_fast_kinds = set()
        self._stop = False
        self._limiter = None
        self._done = None

    def register(self, kind: str, handler, fail_fast: bool = False) -> None:
        self.handlers[kind] = handler
        if fail_fast:
            self.fail_fast_kinds.add(kind)
        logger.info("registered handler kind=%s fail_fast=%s", kind, fail_fast)

    def stop(self) -> None:
        self._stop = True

    async def run(self) -> None:
        # Blocks until stop() AND every in-flight job has settled. Flow waits on
        # this with drain_timeout, then cancels the surrounding scope.
        self._stop = False
        self._done = anyio.Event()
        self._limiter = anyio.CapacityLimiter(self.config.thread_pool_size)
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._scheduler_loop)
                for i in range(self.config.workers):
                    tg.start_soon(self._worker_loop, f"worker-{i}")
        finally:
            self._done.set()

    async def wait_done(self) -> None:
        if self._done is not None:
            await self._done.wait()

    async def _scheduler_loop(self) -> None:
        while not self._stop:
            await anyio.sleep(self.config.scheduler_tick_interval)
            if self._stop:
                return
            try:
                recovered = self.queue.recover_expired_claims()
                if recovered:
                    logger.info("scheduler recovered %d expired claims", len(recovered))
                self.event_bus.cleanup()
            except Exception:
                logger.exception("scheduler tick failed")

    async def _worker_loop(self, worker_id: str) -> None:
        while not self._stop:
            try:
                job = self.queue.claim(worker_id)
            except Exception:
                logger.exception("claim failed (worker %s)", worker_id)
                await anyio.sleep(self.config.poll_interval)
                continue
            if job is None:
                await anyio.sleep(self.config.poll_interval)
                continue
            await self._process_job(job)

    async def _process_job(self, job: Job) -> None:
        handler = self.handlers.get(job.kind)
        if handler is None:
            logger.error("no handler for kind=%s, job %s dead-lettered", job.kind, job.id)
            self._nack_or_fail(
                job.id, f"unknown kind: {job.kind}", permanent=True, dead_letter=True
            )
            return
        await self._await_rate_token(job.kind)
        ctx = TaskContext(job, self.queue)
        fail_fast = job.kind in self.fail_fast_kinds
        is_async = inspect.iscoroutinefunction(handler)
        try:
            if is_async:
                result = await handler(ctx, ctx.job.payload)
            else:
                result = await anyio.to_thread.run_sync(
                    self._run_sync_handler, handler, ctx, limiter=self._limiter
                )
            # Cancellation delivery point: anyio SHIELDS the thread wait, so a
            # drain-timeout cancel that fired mid-handler surfaces at this
            # checkpoint, not inside the await above.
            await anyio.lowlevel.checkpoint()
        except _InterruptMarker:
            logger.warning("interrupt in sync handler for job %s, draining", job.id)
            self.stop()
            raise KeyboardInterrupt from None
        except (KeyboardInterrupt, SystemExit) as e:
            logger.warning(
                "interrupt in handler for job %s (%s), draining", job.id, type(e).__name__
            )
            self.stop()
            raise
        except anyio.get_cancelled_exc_class():
            # Drain timeout cancellation. Async: nack so the job retries soon.
            # Sync: the thread is still running — abandon, claim recovery owns it.
            if is_async:
                self._nack_or_fail(job.id, "worker_drain_timeout:cancelled", retryable=True)
            else:
                logger.warning("abandoning sync job %s at drain (thread still running)", job.id)
            return
        except PermanentError as e:
            self._nack_or_fail(job.id, str(e) or "PermanentError", permanent=True)
            return
        except TransientError as e:
            self._nack_or_fail(job.id, str(e) or "TransientError", retryable=True)
            return
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            if fail_fast:
                self._nack_or_fail(job.id, reason, permanent=True, dead_letter=True)
            else:
                self._nack_or_fail(job.id, reason, retryable=True)
            return
        self._finish_success(ctx, result)

    def _run_sync_handler(self, handler, ctx):
        try:
            return handler(ctx, ctx.job.payload)
        except (KeyboardInterrupt, SystemExit) as e:
            raise _InterruptMarker() from e

    def _finish_success(self, ctx: TaskContext, result) -> None:
        result_ref = ctx.result_ref
        if isinstance(result, str):
            result_ref = result
        view = self.queue.get_job(ctx.job.id)
        if view is not None and view.subtasks:
            final = aggregate_subtasks([s.status for s in view.subtasks])
        else:
            final = "succeeded"
        try:
            self.queue.ack(ctx.job.id, final, result_ref)
        except Exception:
            # Ack denied: job was reclaimed during a drain race — it is already
            # re-queued or dead-lettered. Log loudly; audit trail has the truth.
            logger.exception("ack denied for job %s (reclaimed during drain?)", ctx.job.id)

    def _nack_or_fail(
        self,
        job_id: str,
        reason: str,
        retryable: bool = False,
        permanent: bool = False,
        dead_letter: bool = False,
    ) -> None:
        try:
            if permanent:
                self.queue.permanent_fail(job_id, reason, dead_letter=dead_letter)
            else:
                self.queue.nack(job_id, reason, retryable=retryable)
        except Exception:
            logger.exception("terminal transition denied for job %s", job_id)

    async def _await_rate_token(self, kind: str) -> None:
        while True:
            wait = self.rate.try_acquire(kind)
            if wait <= 0:
                return
            await anyio.sleep(wait)
