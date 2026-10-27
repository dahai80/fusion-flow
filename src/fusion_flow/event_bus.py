import json
import logging
from datetime import timedelta

from .clock import Clock, RealClock
from .models import FlowEvent
from .storage import Storage

logger = logging.getLogger("fusion_flow")

DEFAULT_EVENT_RETENTION_DAYS = 14.0


class EventBus:
    # Dispatches FlowEvents to user callbacks AFTER the queue transaction has
    # committed: a broken hook logs loudly (exc_info) but can never roll back
    # queue state or break the worker loop. Also owns rolling cleanup of the
    # flow_events audit trail and expired flow_kv rows.

    def __init__(
        self,
        storage: Storage,
        clock: Clock | None = None,
        retention_days: float = DEFAULT_EVENT_RETENTION_DAYS,
        callbacks=None,
    ):
        self.storage = storage
        self.clock = clock or RealClock()
        self.retention_days = retention_days
        self.callbacks = list(callbacks or [])

    def subscribe(self, callback) -> None:
        if callback not in self.callbacks:
            self.callbacks.append(callback)

    def unsubscribe(self, callback) -> None:
        if callback in self.callbacks:
            self.callbacks.remove(callback)

    def dispatch(self, event: FlowEvent) -> None:
        for callback in self.callbacks:
            try:
                callback(event)
            except Exception:
                logger.exception(
                    "event callback %r failed for %s (job=%s)", callback, event.event, event.job_id
                )

    def list_events(self, job_id: str | None = None, limit: int = 100) -> list[FlowEvent]:
        if job_id:
            rows = self.storage.query_all(
                "SELECT * FROM flow_events WHERE job_id=? ORDER BY id DESC LIMIT ?", (job_id, limit)
            )
        else:
            rows = self.storage.query_all(
                "SELECT * FROM flow_events ORDER BY id DESC LIMIT ?", (limit,)
            )
        events = []
        for row in rows:
            try:
                data = json.loads(row["data"]) if row["data"] else {}
            except (TypeError, ValueError):
                data = {}
            events.append(
                FlowEvent(
                    event=row["event"],
                    job_id=row["job_id"],
                    subtask_id=row["subtask_id"],
                    data=data,
                    ts=row["ts"],
                )
            )
        return events

    def cleanup(self) -> int:
        # Rolling retention on the audit trail + expired flow_kv rows.
        # Called by the scheduler tick; cheap enough at second granularity.
        cutoff = (self.clock.now() - timedelta(days=self.retention_days)).isoformat()
        conn = self.storage.connection()
        with conn:
            cur = conn.execute("DELETE FROM flow_events WHERE ts < ?", (cutoff,))
            events_deleted = cur.rowcount
            cur = conn.execute(
                "DELETE FROM flow_kv WHERE expires_at IS NOT NULL AND expires_at < ?",
                (self.clock.now_iso(),),
            )
            kv_deleted = cur.rowcount
        if events_deleted or kv_deleted:
            logger.info("cleanup removed %d events, %d expired kv rows", events_deleted, kv_deleted)
        return events_deleted + kv_deleted

    def kv_set(self, key: str, value: str, ttl: timedelta | None = None) -> None:
        expires = (self.clock.now() + ttl).isoformat() if ttl is not None else None
        conn = self.storage.connection()
        with conn:
            conn.execute(
                """INSERT INTO flow_kv(key, value, expires_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE
                   SET value=excluded.value, expires_at=excluded.expires_at""",
                (key, value, expires),
            )

    def kv_get(self, key: str) -> str | None:
        row = self.storage.query_one("SELECT value FROM flow_kv WHERE key=?", (key,))
        return row["value"] if row is not None else None

    def kv_delete(self, key: str) -> None:
        conn = self.storage.connection()
        with conn:
            conn.execute("DELETE FROM flow_kv WHERE key=?", (key,))
