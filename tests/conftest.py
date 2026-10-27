import pytest

from fusion_flow.clock import Clock
from fusion_flow.flow import Flow


@pytest.fixture()
def fake_clock():
    return Clock(seed=42)


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture()
def flow(db_path, fake_clock):
    # Fresh Flow per test: file-backed SQLite (never :memory:, which would hide
    # WAL/crash behavior), seeded fake clock so backoff/claim timeouts never sleep.
    f = Flow(
        db_path,
        clock=fake_clock,
        workers=2,
        scheduler_tick_interval=0.05,
        poll_interval=0.01,
        drain_timeout_s=1.0,
        claim_timeout_s=600.0,
    )
    yield f
    f.close()


@pytest.fixture()
def flow_factory_drain_short(db_path, fake_clock):
    # Short drain timeout so the abandon path (sync handler outliving the drain)
    # is reachable without slowing the suite down.
    f = Flow(
        db_path,
        clock=fake_clock,
        workers=1,
        scheduler_tick_interval=0.05,
        poll_interval=0.01,
        drain_timeout_s=0.3,
        claim_timeout_s=600.0,
    )
    yield f
    f.close()
