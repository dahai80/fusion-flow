from datetime import timedelta

import pytest

from fusion_flow.dedup import fingerprint, validate_jobspec
from fusion_flow.errors import PayloadTooLarge
from fusion_flow.models import JobSpec


def test_fingerprint_uses_dedup_key_when_present():
    a = fingerprint(JobSpec(kind="import", payload={"x": 1}, dedup_key="file:1"))
    b = fingerprint(JobSpec(kind="import", payload={"x": 999}, dedup_key="file:1"))
    c = fingerprint(JobSpec(kind="import", payload={"x": 1}, dedup_key="file:2"))
    assert a == b
    assert a != c


def test_fingerprint_payload_key_order_independent():
    a = fingerprint(JobSpec(kind="import", payload={"a": 1, "b": 2}))
    b = fingerprint(JobSpec(kind="import", payload={"b": 2, "a": 1}))
    assert a == b


def test_fingerprint_kind_separates():
    a = fingerprint(JobSpec(kind="a", payload={"x": 1}))
    b = fingerprint(JobSpec(kind="b", payload={"x": 1}))
    assert a != b


def test_validate_rejects_bad_kind():
    with pytest.raises(ValueError):
        validate_jobspec(JobSpec(kind="bad kind!"), 1024)
    with pytest.raises(ValueError):
        validate_jobspec(JobSpec(kind=""), 1024)
    with pytest.raises(ValueError):
        validate_jobspec(JobSpec(kind="x" * 201), 1024)


def test_validate_rejects_bad_dedup_key():
    with pytest.raises(ValueError):
        validate_jobspec(JobSpec(kind="k", dedup_key="has space"), 1024)
    with pytest.raises(ValueError):
        validate_jobspec(JobSpec(kind="k", dedup_key="k" * 513), 1024)


def test_validate_rejects_oversize_payload():
    with pytest.raises(PayloadTooLarge):
        validate_jobspec(JobSpec(kind="k", payload={"blob": "x" * 2048}), 1024)


def test_validate_accepts_normal_spec():
    validate_jobspec(
        JobSpec(kind="import:file", payload={"a": 1}, dedup_ttl=timedelta(minutes=5)), 1024
    )
