from datetime import timedelta

import pytest

from fusion_flow.clock import Clock, parse_duration
from fusion_flow.rate import RateLimiter, TokenBucket, parse_rate
from fusion_flow.retry import Backoff


def test_parse_rate():
    assert parse_rate("2/min") == pytest.approx(2 / 60)
    assert parse_rate("1/s") == 1.0
    assert parse_rate("10/h") == pytest.approx(10 / 3600)
    assert parse_rate("100/d") == pytest.approx(100 / 86400)
    with pytest.raises(ValueError):
        parse_rate("2 per minute")


def test_parse_duration():
    assert parse_duration("30s") == timedelta(seconds=30)
    assert parse_duration("2h") == timedelta(hours=2)
    assert parse_duration(timedelta(minutes=1)) == timedelta(minutes=1)


def test_backoff_grows_and_caps(fake_clock):
    backoff = Backoff(clock=fake_clock, base_s=30.0, cap_s=1800.0)
    d0 = backoff.delay(0).total_seconds()
    d3 = backoff.delay(3).total_seconds()
    assert 30.0 <= d0 <= 36.0
    assert 180.0 <= d3 <= 216.0
    d10 = backoff.delay(10).total_seconds()
    assert d10 <= 1800.0 * 1.2


def test_token_bucket_refills_with_fake_clock():
    clock = Clock(seed=1)
    bucket = TokenBucket(rate_per_s=1.0, capacity=1.0, clock=clock)
    assert bucket.try_take() == 0.0
    wait = bucket.try_take()
    assert wait > 0.0
    clock.advance(wait)
    assert bucket.try_take() == 0.0


def test_rate_limiter_passthrough_without_rate():
    limiter = RateLimiter(clock=Clock(seed=1))
    assert limiter.try_acquire("any_kind") == 0.0


def test_rate_limiter_enforces_per_kind():
    clock = Clock(seed=1)
    limiter = RateLimiter(per_kind={"slow": "2/min"}, clock=clock)
    assert limiter.try_acquire("slow") == 0.0
    wait = limiter.try_acquire("slow")
    assert wait >= 29.0
    assert limiter.try_acquire("fast") == 0.0
    clock.advance(wait)
    assert limiter.try_acquire("slow") == 0.0
