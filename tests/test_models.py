import pytest

from fusion_flow.models import Job, JobSpec, SubTask, new_job_id


def test_jobspec_deep_copies_payload():
    src = {"nested": {"a": 1}}
    spec = JobSpec(kind="k", payload=src)
    src["nested"]["a"] = 999
    assert spec.payload["nested"]["a"] == 1


def test_jobspec_rejects_non_timedelta_ttl():
    with pytest.raises(ValueError):
        JobSpec(kind="k", dedup_ttl="30s")


def test_payload_json_is_stable():
    a = JobSpec(kind="k", payload={"z": 1, "a": 2})
    b = JobSpec(kind="k", payload={"a": 2, "z": 1})
    assert a.payload_json() == b.payload_json()


def test_job_id_prefix():
    assert new_job_id().startswith("flw_")


def test_job_from_dict_ignores_unknown_keys():
    job = Job.from_dict(
        {
            "id": "j1",
            "kind": "k",
            "status": "queued",
            "unknown_future_field": {"x": 1},
            "payload": {"a": 1},
        }
    )
    assert job.id == "j1"
    assert job.status == "queued"
    assert job.payload == {"a": 1}


def test_job_is_terminal():
    assert Job(id="j", kind="k", status="succeeded").is_terminal
    assert not Job(id="j", kind="k", status="queued").is_terminal


def test_job_payload_never_shared():
    src = {"a": {"b": 1}}
    job = Job.from_dict({"id": "j", "kind": "k", "payload": src})
    src["a"]["b"] = 2
    assert job.payload["a"]["b"] == 1


def test_subtask_detail_deep_copy():
    src = {"x": [1, 2]}
    sub = SubTask(id="s", job_id="j", name="n", status="running", detail=src)
    src["x"].append(3)
    assert sub.detail["x"] == [1, 2]
