import logging
from collections import deque
from typing import Protocol

import anyio

from .clock import Clock, RealClock
from .errors import PayloadTooLarge
from .event_bus import EventBus
from .models import Job, JobSpec
from .queue_service import QueueService
from .retry import Backoff

logger = logging.getLogger("fusion_flow")

DEFAULT_CONNECTOR_INTERVAL_S = 60.0
DEFAULT_CONNECTOR_BACKOFF_BASE_S = 5.0
DEFAULT_CONNECTOR_BACKOFF_CAP_S = 300.0
DEFAULT_ON_DONE_RETRIES = 3


class Connector(Protocol):
    # Side-channel source/sink adapters. poll() returns specs to enqueue;
    # commit() advances the connector's durable cursor (e.g. a Drive
    # pageToken) and is ONLY called by the runner after every spec of the
    # round settled — that is what makes QueueFull zero-loss. on_job_done()
    # is the reverse path (upload results / move rejected files).
    async def poll(self) -> list[JobSpec]: ...
    async def commit(self) -> None: ...
    async def on_job_done(self, job: Job, result_ref: str | None) -> None: ...


class ConnectorRunner:
    # One anyio task per connector, parallel to the worker pool. Structural
    # isolation: a failing connector never blocks job consumption (B3) — the
    # two task groups never await each other. Backpressure contract:
    #   QueueFull       -> whole poll round failed, commit() NOT called, so
    #                      the connector's cursor stays put and the round is
    #                      retried later (specs are never dropped).
    #   PayloadTooLarge -> this one spec is invalid, drop it, round continues
    #                      and may still commit.
    #   poll()/commit() exception -> connector backoff, connector_error event.

    def __init__(
        self,
        name: str,
        connector,
        queue: QueueService,
        event_bus: EventBus,
        clock: Clock | None = None,
        *,
        interval_s: float = DEFAULT_CONNECTOR_INTERVAL_S,
        kinds: set[str] | None = None,
        backoff_base_s: float = DEFAULT_CONNECTOR_BACKOFF_BASE_S,
        backoff_cap_s: float = DEFAULT_CONNECTOR_BACKOFF_CAP_S,
        on_done_retries: int = DEFAULT_ON_DONE_RETRIES,
    ):
        self.name = name
        self.connector = connector
        self.queue = queue
        self.event_bus = event_bus
        self.clock = clock or RealClock()
        self.interval_s = interval_s
        self.kinds = kinds
        self.on_done_retries = max(1, on_done_retries)
        self.backoff = Backoff(clock=self.clock, base_s=backoff_base_s, cap_s=backoff_cap_s)
        self.failures = 0
        self.poll_count = 0
        self.commit_count = 0
        self._pending: deque = deque()
        self._wake = None
        self._event_handler = self._on_event

    def notify_done(self, job: Job, result_ref: str | None) -> None:
        # Sync, called from the event sink thread (worker loop context).
        self._pending.append((job, result_ref))
        if self._wake is not None:
            try:
                self._wake.set()
            except Exception:
                # Cross-thread wake race at shutdown: the pending entry is
                # still drained by the next loop iteration or dropped with
                # the task. Never let a hook break the queue.
                logger.debug("connector %s wake failed (shutting down?)", self.name)

    def _on_event(self, event) -> None:
        if event.event != "ack" or event.job_id is None:
            return
        try:
            job = self.queue.get_job_row(event.job_id)
        except Exception:
            return
        if self.kinds is not None and job.kind not in self.kinds:
            return
        result_ref = (event.data or {}).get("result_ref")
        self.notify_done(job, result_ref)

    async def run(self) -> None:
        self._wake = anyio.Event()
        self.event_bus.subscribe(self._event_handler)
        try:
            while True:
                # First poll immediately, then pace by interval; failures
                # replace the pace with connector backoff.
                await self._poll_round()
                await self._handle_pending()
                await self._sleep_until_next()
        finally:
            self.event_bus.unsubscribe(self._event_handler)
            logger.info("connector runner %s stopped", self.name)

    async def _sleep_until_next(self) -> None:
        delay = self.interval_s
        if self.failures > 0:
            delay = self.backoff.delay(self.failures - 1).total_seconds()
        with anyio.move_on_after(delay):
            await self._wake.wait()
            self._wake = anyio.Event()
            # Woken by an ack notification: deliver it fast, then keep the
            # remaining poll delay instead of polling the source immediately.
            await self._handle_pending()

    async def _poll_round(self) -> None:
        try:
            specs = await self.connector.poll()
        except Exception as e:
            self._fail(f"poll failed: {type(e).__name__}: {e}")
            return
        self.poll_count += 1
        dropped = 0
        for spec in specs:
            try:
                self.queue.enqueue(spec)
            except (PayloadTooLarge, ValueError) as e:
                # Invalid spec (oversized payload, bad kind/dedup_key): the
                # business callback built a broken JobSpec. Drop this one,
                # the round continues — a poison spec must not wedge the
                # connector in a permanent failure loop.
                dropped += 1
                self._emit_error("connector_spec_dropped", {"reason": str(e), "kind": spec.kind})
                logger.warning(
                    "connector %s dropped invalid spec kind=%s: %s", self.name, spec.kind, e
                )
            except Exception as e:
                # QueueFull or an unexpected enqueue failure: the whole round
                # failed. Do NOT commit — the cursor stays and the specs come
                # back on the next poll. Zero-loss beats zero-duplicates.
                self._fail(f"enqueue failed: {type(e).__name__}: {e}")
                return
        commit = getattr(self.connector, "commit", None)
        if commit is None:
            self.failures = 0
            return
        try:
            await commit()
        except Exception as e:
            self._fail(f"commit failed: {type(e).__name__}: {e}")
            return
        self.commit_count += 1
        self.failures = 0
        if specs:
            logger.info(
                "connector %s round ok: %d enqueued, %d dropped",
                self.name,
                len(specs) - dropped,
                dropped,
            )

    async def _handle_pending(self) -> None:
        while self._pending:
            job, result_ref = self._pending.popleft()
            delivered = False
            for attempt in range(self.on_done_retries):
                try:
                    await self.connector.on_job_done(job, result_ref)
                    delivered = True
                    break
                except Exception as e:
                    logger.warning(
                        "connector %s on_job_done failed for %s (attempt %d/%d): %s",
                        self.name,
                        job.id,
                        attempt + 1,
                        self.on_done_retries,
                        e,
                    )
                    if attempt + 1 < self.on_done_retries:
                        await anyio.sleep(self.backoff.delay(attempt).total_seconds())
            if not delivered:
                # Reverse-path failure never rolls back the job's terminal
                # state — it is already local truth. Alert via event for
                # manual/automated compensation.
                self._emit_error(
                    "connector_error",
                    {"reason": "on_job_done retries exhausted", "job_id": job.id},
                )
                logger.error("connector %s on_job_done exhausted for job %s", self.name, job.id)

    def _fail(self, reason: str) -> None:
        self.failures += 1
        self._emit_error("connector_error", {"reason": reason, "failures": self.failures})
        logger.warning("connector %s failure #%d: %s", self.name, self.failures, reason)

    def _emit_error(self, event: str, data: dict) -> None:
        self.event_bus.emit(event, data={**data, "connector": self.name})
