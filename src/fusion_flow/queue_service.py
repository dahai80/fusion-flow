import json
import logging
import sqlite3
from datetime import timedelta

from .clock import Clock, RealClock
from .dedup import fingerprint, validate_jobspec
from .errors import IllegalTransition, QueueFull
from .models import TERMINAL_JOB_STATUSES, Job, JobSpec, JobView, SubTask, new_job_id
from .retry import Backoff
from .state import check_job_transition
from .storage import Storage, row_to_job, row_to_subtask

logger = logging.getLogger("fusion_flow")

ACTIVE_STATUSES = ("queued", "processing")


class QueueConfig:
    def __init__(
        self,
        max_queue_depth: int = 10000,
        payload_max_bytes: int = 64 * 1024,
        claim_timeout_s: float = 600.0,
    ):
        self.max_queue_depth = max_queue_depth
        self.payload_max_bytes = payload_max_bytes
        self.claim_timeout_s = claim_timeout_s


class QueueService:
    # All queue state transitions live here. Every transition + its audit event
    # are written in one short transaction; event_sink (wired by Flow) is called
    # AFTER commit so a broken consumer hook can never roll back queue state.

    def __init__(
        self,
        storage: Storage,
        clock: Clock | None = None,
        backoff: Backoff | None = None,
        config: QueueConfig | None = None,
        event_sink=None,
    ):
        self.storage = storage
        self.clock = clock or RealClock()
        self.backoff = backoff or Backoff(clock=self.clock)
        self.config = config or QueueConfig()
        self.event_sink = event_sink

    def _emit(self, conn, event, job_id=None, subtask_id=None, data=None):
        conn.execute(
            "INSERT INTO flow_events(job_id, subtask_id, event, data, ts) VALUES(?,?,?,?,?)",
            (job_id, subtask_id, event, json.dumps(data or {}), self.clock.now_iso()),
        )
        if self.event_sink is not None:
            from .models import FlowEvent

            self.event_sink(
                FlowEvent(
                    event=event,
                    job_id=job_id,
                    subtask_id=subtask_id,
                    data=data,
                    ts=self.clock.now_iso(),
                )
            )

    def enqueue(self, spec: JobSpec) -> Job:
        validate_jobspec(spec, self.config.payload_max_bytes)
        fp = fingerprint(spec)
        existing = self._find_active(fp)
        if existing is not None:
            logger.info("enqueue dedup hit, reuse job %s (kind=%s)", existing["id"], spec.kind)
            return Job.from_dict({**row_to_job(existing), "created": False})
        reused = self._find_ttl_reuse(spec, fp)
        if reused is not None:
            logger.info("enqueue dedup_ttl reuse job %s", reused["id"])
            return Job.from_dict({**row_to_job(reused), "created": False})
        depth = self.storage.query_one(
            "SELECT COUNT(*) AS c FROM flow_jobs WHERE status IN ('queued','processing')"
        )["c"]
        if depth >= self.config.max_queue_depth:
            self._emit(self.storage.connection(), "queue_full", data={"depth": depth})
            raise QueueFull(f"queue depth {depth} >= max_queue_depth {self.config.max_queue_depth}")
        job = self._insert(spec, fp)
        logger.info("enqueued job %s kind=%s priority=%s", job.id, job.kind, job.priority)
        return job

    def _insert(self, spec: JobSpec, fp: str) -> Job:
        job_id = new_job_id()
        now = self.clock.now_iso()
        conn = self.storage.connection()
        try:
            with conn:
                conn.execute(
                    """INSERT INTO flow_jobs
                    (id, kind, payload, dedup_key, dedup_fingerprint, status, priority,
                     attempts, max_retries, created_at)
                    VALUES(?,?,?,?,?,'queued',?,0,?,?)""",
                    (
                        job_id,
                        spec.kind,
                        spec.payload_json(),
                        spec.dedup_key,
                        fp,
                        spec.priority,
                        spec.max_retries,
                        now,
                    ),
                )
                self._emit(
                    conn,
                    "enqueued",
                    job_id=job_id,
                    data={"kind": spec.kind, "priority": spec.priority},
                )
        except sqlite3.IntegrityError:
            row = self._find_active(fp)
            if row is not None:
                logger.info("enqueue race dedup hit, reuse job %s", row["id"])
                return Job.from_dict({**row_to_job(row), "created": False})
            raise
        return self.get_job_row(job_id)

    def _find_active(self, fp: str):
        return self.storage.query_one(
            "SELECT * FROM flow_jobs "
            "WHERE dedup_fingerprint=? AND status IN ('queued','processing') LIMIT 1",
            (fp,),
        )

    def _find_ttl_reuse(self, spec: JobSpec, fp: str):
        if spec.dedup_ttl is None:
            return None
        row = self.storage.query_one(
            """SELECT * FROM flow_jobs WHERE dedup_fingerprint=? AND status='succeeded'
               AND result_ref IS NOT NULL AND finished_at IS NOT NULL
               ORDER BY finished_at DESC LIMIT 1""",
            (fp,),
        )
        if row is None:
            return None
        from datetime import datetime

        finished = datetime.fromisoformat(row["finished_at"])
        if self.clock.now() - finished <= spec.dedup_ttl:
            return row
        return None

    def claim(self, worker_id: str) -> Job | None:
        now = self.clock.now()
        expires = now + timedelta(seconds=self.config.claim_timeout_s)
        conn = self.storage.connection()
        rows = conn.execute(
            """UPDATE flow_jobs
               SET status='processing', claimed_by=?, claimed_at=?, claim_expires_at=?,
               started_at=COALESCE(started_at, ?)
               WHERE id = (
                 SELECT id FROM flow_jobs
                 WHERE status='queued' AND (next_retry_at IS NULL OR next_retry_at <= ?)
                 ORDER BY priority ASC, created_at ASC LIMIT 1
               ) RETURNING *""",
            (worker_id, now.isoformat(), expires.isoformat(), now.isoformat(), now.isoformat()),
        ).fetchall()
        if not rows:
            # Legacy isolation: a 0-row UPDATE still opened an implicit write
            # txn. Returning without ending it would hold the write lock until
            # the next successful claim and block same-process enqueue from
            # other threads (issue #1). Nothing to preserve — roll back.
            conn.rollback()
            return None
        job = Job.from_dict(row_to_job(rows[0]))
        with conn:
            self._emit(conn, "claimed", job_id=job.id, data={"worker": worker_id})
        logger.info("claimed job %s by %s", job.id, worker_id)
        return job

    def ack(self, job_id: str, final_status: str, result_ref: str | None = None) -> Job:
        check_job_transition("processing", final_status)
        now = self.clock.now_iso()
        conn = self.storage.connection()
        with conn:
            cur = conn.execute(
                """UPDATE flow_jobs SET status=?, result_ref=COALESCE(?, result_ref), finished_at=?,
                   claimed_by=NULL, claimed_at=NULL, claim_expires_at=NULL
                   WHERE id=? AND status='processing'""",
                (final_status, result_ref, now, job_id),
            )
            if cur.rowcount == 0:
                self._emit(
                    conn,
                    "transition_denied",
                    job_id=job_id,
                    data={"from": "processing", "to": final_status},
                )
                raise IllegalTransition(f"job {job_id} is not processing (ack denied)")
            self._emit(
                conn, "ack", job_id=job_id, data={"status": final_status, "result_ref": result_ref}
            )
        logger.info("ack job %s status=%s", job_id, final_status)
        return self.get_job_row(job_id)

    def nack(self, job_id: str, reason: str, retryable: bool = True) -> Job:
        # Job-level retry counting. attempts+1 then: below max_retries -> queued
        # with backoff; exhausted -> dead_lettered. KeyboardInterrupt/SystemExit
        # never reach here (worker pool re-raises them).
        conn = self.storage.connection()
        with conn:
            row = conn.execute(
                "SELECT * FROM flow_jobs WHERE id=? AND status='processing'", (job_id,)
            ).fetchone()
            if row is None:
                self._emit(
                    conn,
                    "transition_denied",
                    job_id=job_id,
                    data={"from": "?", "to": "queued", "reason": reason},
                )
                raise IllegalTransition(f"job {job_id} is not processing (nack denied)")
            attempts = row["attempts"] + 1
            if retryable and attempts < row["max_retries"]:
                delay = self.backoff.delay(attempts - 1)
                next_at = (self.clock.now() + delay).isoformat()
                conn.execute(
                    """UPDATE flow_jobs SET status='queued', attempts=?, next_retry_at=?,
                       claimed_by=NULL, claimed_at=NULL, claim_expires_at=NULL, last_error=?
                       WHERE id=?""",
                    (attempts, next_at, reason, job_id),
                )
                self._emit(
                    conn,
                    "nack",
                    job_id=job_id,
                    data={"reason": reason, "attempts": attempts, "retry_at": next_at},
                )
                status = "queued"
            else:
                conn.execute(
                    """UPDATE flow_jobs SET status='dead_lettered', attempts=?, finished_at=?,
                       claimed_by=NULL, claimed_at=NULL, claim_expires_at=NULL, last_error=?
                       WHERE id=?""",
                    (attempts, self.clock.now_iso(), reason, job_id),
                )
                self._emit(
                    conn,
                    "dead_letter",
                    job_id=job_id,
                    data={"reason": reason, "attempts": attempts},
                )
                status = "dead_lettered"
        logger.info("nack job %s reason=%s status=%s", job_id, reason, status)
        return self.get_job_row(job_id)

    def permanent_fail(self, job_id: str, reason: str, dead_letter: bool = False) -> Job:
        # PermanentError -> failed (business-terminal); fail_fast -> dead_lettered
        # with FATAL event for alerting. Neither consumes retries.
        target = "dead_lettered" if dead_letter else "failed"
        check_job_transition("processing", target)
        conn = self.storage.connection()
        with conn:
            cur = conn.execute(
                """UPDATE flow_jobs SET status=?, finished_at=?, last_error=?,
                   claimed_by=NULL, claimed_at=NULL, claim_expires_at=NULL
                   WHERE id=? AND status='processing'""",
                (target, self.clock.now_iso(), reason, job_id),
            )
            if cur.rowcount == 0:
                self._emit(
                    conn,
                    "transition_denied",
                    job_id=job_id,
                    data={"from": "processing", "to": target},
                )
                raise IllegalTransition(f"job {job_id} is not processing (permanent_fail denied)")
            self._emit(
                conn,
                "fatal" if dead_letter else "permanent_fail",
                job_id=job_id,
                data={"reason": reason},
            )
        logger.warning("job %s permanently failed: %s -> %s", job_id, reason, target)
        return self.get_job_row(job_id)

    def cancel(self, job_id: str) -> Job:
        conn = self.storage.connection()
        with conn:
            row = conn.execute("SELECT status FROM flow_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            check_job_transition(row["status"], "cancelled")
            conn.execute(
                """UPDATE flow_jobs SET status='cancelled', finished_at=?,
                   claimed_by=NULL, claimed_at=NULL, claim_expires_at=NULL WHERE id=?""",
                (self.clock.now_iso(), job_id),
            )
            self._emit(conn, "cancelled", job_id=job_id)
        logger.info("cancelled job %s", job_id)
        return self.get_job_row(job_id)

    def copy_to_new(self, job_id: str, event_name: str) -> Job:
        # Copy semantics for retry/replay: brand-new job, attempts reset; the
        # original record and its audit trail stay untouched. Source must be
        # terminal — copying an active job would collide with the partial
        # dedup unique index.
        row = self.storage.query_one("SELECT * FROM flow_jobs WHERE id=?", (job_id,))
        if row is None:
            raise KeyError(job_id)
        if row["status"] not in TERMINAL_JOB_STATUSES:
            raise IllegalTransition(
                f"job {job_id} is {row['status']}; only terminal jobs can be retried or replayed"
            )
        new_id = new_job_id()
        conn = self.storage.connection()
        with conn:
            conn.execute(
                """INSERT INTO flow_jobs
                   (id, kind, payload, dedup_key, dedup_fingerprint, status, priority,
                    attempts, max_retries, created_at)
                   SELECT ?, kind, payload, dedup_key, dedup_fingerprint, 'queued', priority,
                          0, max_retries, ? FROM flow_jobs WHERE id=?""",
                (new_id, self.clock.now_iso(), job_id),
            )
            self._emit(conn, event_name, job_id=new_id, data={"source": job_id})
        logger.info("%s job %s -> new job %s", event_name, job_id, new_id)
        return self.get_job_row(new_id)

    def recover_expired_claims(self) -> list[str]:
        # Crash recovery: claims past their expiry go back to queued, attempts
        # unchanged — kill -9 self-healing.
        now = self.clock.now_iso()
        conn = self.storage.connection()
        rows = conn.execute(
            "SELECT id FROM flow_jobs "
            "WHERE status='processing' AND claim_expires_at IS NOT NULL AND claim_expires_at < ?",
            (now,),
        ).fetchall()
        recovered = []
        with conn:
            for r in rows:
                # Crash recovery counts as an attempt: without this, a handler
                # that always hangs would be re-claimed forever.
                job = conn.execute(
                    "SELECT attempts, max_retries FROM flow_jobs WHERE id=?", (r["id"],)
                ).fetchone()
                attempts = job["attempts"] + 1
                if attempts >= job["max_retries"]:
                    conn.execute(
                        """UPDATE flow_jobs SET status='dead_lettered', attempts=?, finished_at=?,
                           claimed_by=NULL, claimed_at=NULL, claim_expires_at=NULL,
                           last_error='claim timeout (recovered)' WHERE id=?""",
                        (attempts, self.clock.now_iso(), r["id"]),
                    )
                    self._emit(
                        conn,
                        "dead_letter",
                        job_id=r["id"],
                        data={"reason": "claim timeout (recovered)", "attempts": attempts},
                    )
                    logger.warning("claim timeout exhausted retries, job %s dead-lettered", r["id"])
                else:
                    conn.execute(
                        """UPDATE flow_jobs SET status='queued', attempts=?, claimed_by=NULL,
                           claimed_at=NULL, claim_expires_at=NULL,
                           last_error='claim timeout (recovered)' WHERE id=?""",
                        (attempts, r["id"]),
                    )
                    self._emit(conn, "claim_recovered", job_id=r["id"])
                    recovered.append(r["id"])
        for job_id in recovered:
            logger.warning("recovered expired claim for job %s", job_id)
        return recovered

    def report_subtask(
        self, job_id: str, name: str, status: str, detail: dict | None = None
    ) -> SubTask:
        from .state import is_subtask_terminal, validate_subtask_status

        validate_subtask_status(status)
        conn = self.storage.connection()
        existing = conn.execute(
            "SELECT * FROM flow_subtasks WHERE job_id=? AND name=?", (job_id, name)
        ).fetchone()
        now = self.clock.now_iso()
        with conn:
            if existing is not None:
                if is_subtask_terminal(existing["status"]) and status != existing["status"]:
                    self._emit(
                        conn,
                        "transition_denied",
                        job_id=job_id,
                        subtask_id=existing["id"],
                        data={"subtask": name, "from": existing["status"], "to": status},
                    )
                    msg = (
                        f"subtask {name!r} already terminal "
                        f"({existing['status']}), cannot set {status}"
                    )
                    raise IllegalTransition(msg)
                subtask_id = existing["id"]
                conn.execute(
                    "UPDATE flow_subtasks "
                    "SET status=?, detail=?, attempts=attempts+1, updated_at=? WHERE id=?",
                    (status, json.dumps(detail) if detail is not None else None, now, subtask_id),
                )
            else:
                from ulid import ULID

                subtask_id = f"sub_{ULID()}"
                conn.execute(
                    """INSERT INTO flow_subtasks
                       (id, job_id, name, status, detail, attempts, updated_at)
                       VALUES(?,?,?,?,?,1,?)""",
                    (
                        subtask_id,
                        job_id,
                        name,
                        status,
                        json.dumps(detail) if detail is not None else None,
                        now,
                    ),
                )
            self._emit(
                conn,
                "subtask_reported",
                job_id=job_id,
                subtask_id=subtask_id,
                data={"name": name, "status": status},
            )
        row = conn.execute("SELECT * FROM flow_subtasks WHERE id=?", (subtask_id,)).fetchone()
        return SubTask(**row_to_subtask(row))

    def get_job_row(self, job_id: str) -> Job:
        row = self.storage.query_one("SELECT * FROM flow_jobs WHERE id=?", (job_id,))
        if row is None:
            raise KeyError(job_id)
        return Job.from_dict(row_to_job(row))

    def get_job(self, job_id: str) -> JobView | None:
        row = self.storage.query_one("SELECT * FROM flow_jobs WHERE id=?", (job_id,))
        if row is None:
            return None
        job = Job.from_dict(row_to_job(row))
        subs = self.storage.query_all(
            "SELECT * FROM flow_subtasks WHERE job_id=? ORDER BY name", (job_id,)
        )
        return JobView(job=job, subtasks=tuple(SubTask(**row_to_subtask(r)) for r in subs))

    def list_jobs(
        self, status: str | None = None, kind: str | None = None, limit: int = 50
    ) -> list[Job]:
        sql = "SELECT * FROM flow_jobs"
        clauses, params = [], []
        if status:
            clauses.append("status=?")
            params.append(status)
        if kind:
            clauses.append("kind=?")
            params.append(kind)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return [Job.from_dict(row_to_job(r)) for r in self.storage.query_all(sql, tuple(params))]

    def purge(self, older_than: timedelta, statuses: list[str]) -> int:
        cutoff = (self.clock.now() - older_than).isoformat()
        placeholders = ",".join("?" for _ in statuses)
        conn = self.storage.connection()
        with conn:
            rows = conn.execute(
                f"SELECT id FROM flow_jobs WHERE status IN ({placeholders}) AND created_at < ?",
                (*statuses, cutoff),
            ).fetchall()
            for r in rows:
                conn.execute("DELETE FROM flow_subtasks WHERE job_id=?", (r["id"],))
                conn.execute("DELETE FROM flow_events WHERE job_id=?", (r["id"],))
                conn.execute("DELETE FROM flow_jobs WHERE id=?", (r["id"],))
            self._emit(conn, "purged", data={"count": len(rows), "cutoff": cutoff})
        logger.info("purged %d jobs older than %s", len(rows), cutoff)
        return len(rows)
