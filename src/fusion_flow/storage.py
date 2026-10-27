import json
import logging
import sqlite3
import threading
import time
from pathlib import Path

from .migrations import MIGRATIONS

logger = logging.getLogger("fusion_flow")

DEFAULT_PRAGMAS = {
    "journal_mode": "WAL",
    "busy_timeout": "5000",
    "synchronous": "NORMAL",
    "foreign_keys": "ON",
}

OPEN_RETRIES = 5
OPEN_RETRY_DELAY_S = 0.2


class Storage:
    # SQLite layer: the ONLY module allowed to execute SQL. Every thread gets its
    # own connection (sqlite3.Connection is not thread-safe; never pass one across
    # threads). WAL recovery after a crash may briefly hold an exclusive lock on
    # first open, hence the bounded retry below.
    #
    # synchronous=NORMAL: kill -9 does not lose committed transactions, but a
    # physical power loss may lose the most recent ones. Callers can override to
    # FULL via sqlite_pragmas for zero-loss-at-any-cost, at lower write throughput.

    def __init__(self, db_path: str | Path, sqlite_pragmas: dict | None = None):
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self.pragmas = dict(DEFAULT_PRAGMAS)
        if sqlite_pragmas:
            for key, value in sqlite_pragmas.items():
                if key == "journal_mode":
                    raise ValueError("journal_mode=WAL is required and cannot be overridden")
                self.pragmas[key] = value

    def connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._open_with_retry()
            self._local.conn = conn
        return conn

    def _open_with_retry(self) -> sqlite3.Connection:
        last_error = None
        for attempt in range(OPEN_RETRIES):
            try:
                conn = sqlite3.connect(self.db_path, timeout=5.0)
                conn.row_factory = sqlite3.Row
                for pragma, value in self.pragmas.items():
                    conn.execute(f"PRAGMA {pragma}={value}")
                return conn
            except sqlite3.OperationalError as e:
                last_error = e
                logger.warning("db open failed (attempt %d/%d): %s", attempt + 1, OPEN_RETRIES, e)
                time.sleep(OPEN_RETRY_DELAY_S * (attempt + 1))
        raise last_error

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def migrate(self) -> int:
        applied = self.applied_versions()
        current = max((v for v, _, _, _ in MIGRATIONS), default=0)
        if applied == set(v for v, _, _, _ in MIGRATIONS):
            return current
        conn = self.connection()
        for version, name, up_sql, _down in MIGRATIONS:
            if version in applied:
                continue
            with conn:
                conn.executescript(up_sql)
                conn.execute(
                    "INSERT INTO flow_migrations(version, name, applied_at) VALUES(?,?,?)",
                    (version, name, _now_iso()),
                )
            logger.info("applied migration %03d %s", version, name)
        return current

    def migrate_down(self, target: int) -> None:
        applied = self.applied_versions()
        for version, name, _up, down_sql in sorted(MIGRATIONS, reverse=True):
            if version <= target or version not in applied:
                continue
            conn = self.connection()
            with conn:
                conn.executescript(down_sql)
                conn.execute("DELETE FROM flow_migrations WHERE version=?", (version,))
            logger.info("rolled back migration %03d %s", version, name)

    def applied_versions(self) -> set[int]:
        conn = self.connection()
        try:
            rows = conn.execute("SELECT version FROM flow_migrations").fetchall()
        except sqlite3.OperationalError:
            return set()
        return {r["version"] for r in rows}

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.connection().execute(sql, params)

    def query_all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self.connection().execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        return self.connection().execute(sql, params).fetchone()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


def row_to_job(row: sqlite3.Row) -> dict:
    data = dict(row)
    try:
        data["payload"] = json.loads(data.get("payload") or "{}")
    except (TypeError, ValueError):
        data["payload"] = {}
    return data


def row_to_subtask(row: sqlite3.Row) -> dict:
    data = dict(row)
    try:
        data["detail"] = json.loads(data["detail"]) if data.get("detail") else None
    except (TypeError, ValueError):
        data["detail"] = None
    return data
