import json
import os
import subprocess
import sys

import pytest

import fusion_flow.flow as flow_mod
from fusion_flow import Flow, JobSpec, WorkerLockHeld
from fusion_flow.cli import main as cli_main
from fusion_flow.flow import WorkerLock


def _read_holder(lock_path):
    with open(lock_path) as fh:
        return json.loads(fh.read())


def test_holder_record_written_on_acquire(db_path):
    lock = WorkerLock(db_path)
    lock.acquire()
    try:
        holder = _read_holder(lock.path)
        assert holder["pid"] == flow_mod.os.getpid()
        assert holder["host"]
        assert holder["boot_id"]
        assert holder["acquired_at"]
    finally:
        lock.release()


def test_live_second_holder_rejected_with_detail(db_path):
    first = WorkerLock(db_path)
    first.acquire()
    try:
        second = WorkerLock(db_path)
        with pytest.raises(WorkerLockHeld) as exc:
            second.acquire()
        msg = str(exc.value)
        assert f"pid={flow_mod.os.getpid()}" in msg
        assert "alive" in msg
        assert "flow unlock" in msg
    finally:
        first.release()


def test_previous_boot_lock_auto_reclaimed(db_path, monkeypatch):
    # Hold the lock, then pretend the machine rebooted: the holder record's
    # boot_id no longer matches, so the next acquire must unlink the old inode
    # and succeed instead of raising.
    first = WorkerLock(db_path)
    first.acquire()
    try:
        monkeypatch.setattr(flow_mod, "_boot_id", lambda: "some-other-boot")
        second = WorkerLock(db_path)
        second.acquire()
        second.release()
    finally:
        monkeypatch.undo()
        first.release()


def test_same_boot_dead_pid_not_auto_reclaimed(db_path, monkeypatch):
    # A dead same-boot pid is NOT provably stale (pid reuse / inherited fd),
    # so acquire must fail with diagnostics, never auto-unlink.
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    first = WorkerLock(db_path)
    first.acquire()
    try:
        with open(first.path, "w") as fh:
            json.dump(
                {
                    "pid": dead.pid,
                    "host": flow_mod.socket.gethostname(),
                    "boot_id": flow_mod._boot_id(),
                    "acquired_at": "2026-10-31T00:00:00",
                },
                fh,
            )
        second = WorkerLock(db_path)
        with pytest.raises(WorkerLockHeld) as exc:
            second.acquire()
        assert "flow unlock" in str(exc.value)
    finally:
        first.release()


def test_unlock_removes_stale_free_lock(db_path):
    lock_path = f"{db_path}.worker.lock"
    lock = WorkerLock(db_path)
    lock.acquire()
    lock.release()  # flock free, file left behind with the holder record
    assert cli_main(["--db", db_path, "unlock"]) == 0
    assert not os.path.exists(lock_path)


def test_unlock_refuses_live_holder(db_path):
    lock = WorkerLock(db_path)
    lock.acquire()
    try:
        assert cli_main(["--db", db_path, "unlock"]) == 1
        assert os.path.exists(lock.path)
        # --force overrides the live-holder refusal
        assert cli_main(["--db", db_path, "unlock", "--force"]) == 0
        assert not os.path.exists(lock.path)
    finally:
        lock.release()


def test_unlock_no_file_is_ok(db_path):
    assert cli_main(["--db", db_path, "unlock"]) == 1
    assert cli_main(["--db", db_path, "--json", "unlock"]) == 1


def test_locks_shows_holder(db_path, capsys):
    lock = WorkerLock(db_path)
    lock.acquire()
    try:
        assert cli_main(["--db", db_path, "--json", "locks"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["worker_holding"] is True
        assert out["holder"]["pid"] == flow_mod.os.getpid()
    finally:
        lock.release()


async def test_workers_active_tracks_run_lifecycle(db_path, fake_clock):
    flow = Flow(db_path, clock=fake_clock, workers=1)

    @flow.handler("noop")
    async def noop(ctx, payload):
        return "ref://ok"

    assert flow.workers_active is False
    async with flow.run():
        assert flow.workers_active is True
        flow.enqueue(JobSpec(kind="noop", payload={}))
    assert flow.workers_active is False
    flow.close()


def test_boot_id_is_stable_string():
    assert isinstance(flow_mod._boot_id(), str)
    assert flow_mod._boot_id() == flow_mod._boot_id()
