import logging
import re
import threading

from .clock import Clock, RealClock

logger = logging.getLogger("fusion_flow")

_UNIT_SECONDS = {
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
}
_RATE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*/\s*([a-zA-Z]+)\s*$")

DEFAULT_CAPACITY = 1.0


def parse_rate(text: str) -> float:
    # "2/min" -> events per second. Raises ValueError on anything else.
    match = _RATE_RE.match(text)
    if match is None:
        raise ValueError(
            f"invalid rate {text!r}; expected forms like '2/min', '10/h', '1/s', '100/d'"
        )
    unit = match.group(2).lower()
    if unit not in _UNIT_SECONDS:
        raise ValueError(
            f"invalid rate unit {unit!r}; expected forms like '2/min', '10/h', '1/s', '100/d'"
        )
    count = float(match.group(1))
    return count / _UNIT_SECONDS[unit]


class TokenBucket:
    def __init__(
        self, rate_per_s: float, capacity: float = DEFAULT_CAPACITY, clock: Clock | None = None
    ):
        self.rate_per_s = rate_per_s
        self.capacity = capacity
        self.clock = clock or RealClock()
        self.tokens = capacity
        self.updated_at = self.clock.now()

    def try_take(self) -> float:
        # Returns 0.0 when a token was taken, else the seconds to wait before
        # one token is available. Wall-clock refills via the injectable clock.
        now = self.clock.now()
        elapsed = max((now - self.updated_at).total_seconds(), 0.0)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_s)
        self.updated_at = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        return (1.0 - self.tokens) / self.rate_per_s


class RateLimiter:
    # Process-level token buckets keyed by job kind. In-memory by design:
    # one Flow instance = one process, and cross-process limits are the
    # connector's or caller's concern, not the library's.

    def __init__(
        self,
        default_rate: str | None = None,
        per_kind: dict[str, str] | None = None,
        clock: Clock | None = None,
    ):
        self.clock = clock or RealClock()
        self._default_rate = parse_rate(default_rate) if default_rate else None
        self._per_kind = {kind: parse_rate(rate) for kind, rate in (per_kind or {}).items()}
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def set_rate(self, kind: str, rate: str) -> None:
        self._per_kind[kind] = parse_rate(rate)
        with self._lock:
            self._buckets.pop(kind, None)
        logger.info("rate limit for kind=%s set to %s", kind, rate)

    def try_acquire(self, kind: str) -> float:
        # 0.0 = granted; > 0.0 = seconds to wait. Unrated kinds pass through.
        rate = self._per_kind.get(kind, self._default_rate)
        if rate is None:
            return 0.0
        with self._lock:
            bucket = self._buckets.get(kind)
            if bucket is None or bucket.rate_per_s != rate:
                bucket = TokenBucket(rate, clock=self.clock)
                self._buckets[kind] = bucket
            return bucket.try_take()
