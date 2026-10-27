import json

from fusion_flow.cli import main
from fusion_flow.models import JobSpec

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_NOT_FOUND = 2


def run_cli(db_path, *args):
    return main(["--db", db_path, *args])


def test_ls_empty_db(db_path, capsys):
    assert run_cli(db_path, "ls") == EXIT_OK
    assert "no jobs" in capsys.readouterr().out


def test_ls_and_show_roundtrip(flow, db_path, capsys):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))

    assert run_cli(db_path, "ls") == EXIT_OK
    out = capsys.readouterr().out
    assert job.id in out and "queued" in out

    assert run_cli(db_path, "show", job.id) == EXIT_OK
    out = capsys.readouterr().out
    assert "kind:      import" in out
    assert "enqueued" in out  # audit trail included


def test_show_not_found_exit_2(db_path, capsys):
    assert run_cli(db_path, "show", "flw_missing") == EXIT_NOT_FOUND
    assert "not found" in capsys.readouterr().err


def test_json_output_is_parseable(flow, db_path, capsys):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))

    assert run_cli(db_path, "--json", "ls") == EXIT_OK
    data = json.loads(capsys.readouterr().out)
    assert data[0]["id"] == job.id
    assert data[0]["status"] == "queued"

    assert run_cli(db_path, "--json", "stats") == EXIT_OK
    stats = json.loads(capsys.readouterr().out)
    assert stats["by_status"]["queued"] == 1

    assert run_cli(db_path, "--json", "show", job.id) == EXIT_OK
    detail = json.loads(capsys.readouterr().out)
    assert detail["job"]["id"] == job.id
    assert detail["payload"] == {"x": 1}

    assert run_cli(db_path, "--json", "locks") == EXIT_OK
    locks = json.loads(capsys.readouterr().out)
    assert locks["exists"] is False
    assert locks["worker_holding"] is False


def test_retry_active_job_is_flow_error_exit_1(flow, db_path, capsys):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    assert run_cli(db_path, "retry", job.id) == EXIT_ERROR
    assert "error:" in capsys.readouterr().err


def test_retry_not_found_exit_2(db_path, capsys):
    assert run_cli(db_path, "retry", "flw_missing") == EXIT_NOT_FOUND


def test_cancel_and_replay_via_cli(flow, db_path, capsys):
    job = flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    assert run_cli(db_path, "cancel", job.id) == EXIT_OK
    capsys.readouterr()
    assert flow.get_job(job.id).job.status == "cancelled"

    assert run_cli(db_path, "replay", job.id) == EXIT_OK
    out = capsys.readouterr().out
    assert "replayed" in out
    fresh = flow.list_jobs(status="queued")
    assert len(fresh) == 1
    assert fresh[0].id != job.id


def test_lease_reclaim_no_expired(flow, db_path, capsys):
    assert run_cli(db_path, "lease-reclaim") == EXIT_OK
    assert "no expired claims" in capsys.readouterr().out


def test_vacuum(flow, db_path, capsys):
    flow.enqueue(JobSpec(kind="import", payload={"x": 1}))
    assert run_cli(db_path, "vacuum") == EXIT_OK
    assert "vacuumed" in capsys.readouterr().out
