import sqlite3
import threading

import pytest

from fusion_flow.storage import Storage


def test_migrate_creates_schema(db_path):
    storage = Storage(db_path)
    storage.migrate()
    assert storage.applied_versions() == {1}
    tables = {
        r["name"] for r in storage.query_all("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {"flow_jobs", "flow_subtasks", "flow_events", "flow_kv", "flow_migrations"} <= tables


def test_migrate_idempotent(db_path):
    storage = Storage(db_path)
    storage.migrate()
    storage.migrate()
    assert storage.applied_versions() == {1}


def test_migrate_down_symmetry(db_path):
    storage = Storage(db_path)
    storage.migrate()
    storage.migrate_down(0)
    assert storage.applied_versions() == set()
    storage.migrate()
    assert storage.applied_versions() == {1}


def test_wal_journal_mode(db_path):
    storage = Storage(db_path)
    storage.migrate()
    mode = storage.query_one("PRAGMA journal_mode")["journal_mode"]
    assert mode == "wal"


def test_journal_mode_override_rejected(db_path):
    with pytest.raises(ValueError):
        Storage(db_path, sqlite_pragmas={"journal_mode": "DELETE"})


def test_per_thread_connections(db_path):
    storage = Storage(db_path)
    storage.migrate()
    conns = []
    results = {}

    def touch(n):
        conns.append(storage.connection())
        storage.connection().execute(
            "INSERT INTO flow_kv(key, value) VALUES(?,?)", (f"k{n}", str(n))
        )
        with storage.connection():
            pass
        results[n] = storage.query_one("SELECT value FROM flow_kv WHERE key=?", (f"k{n}",))["value"]

    threads = [threading.Thread(target=touch, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len({id(c) for c in conns}) == 4
    assert results == {0: "0", 1: "1", 2: "2", 3: "3"}


def test_connection_rejects_cross_thread_use(db_path):
    # sqlite3 connections created by one thread must fail in another; Storage's
    # per-thread design exists precisely because of this guard.
    storage = Storage(db_path)
    storage.migrate()
    conn = storage.connection()
    box = {}

    def run():
        try:
            conn.execute("SELECT 1")
            box["ok"] = True
        except sqlite3.ProgrammingError:
            box["ok"] = False

    t = threading.Thread(target=run)
    t.start()
    t.join()
    assert box["ok"] is False
