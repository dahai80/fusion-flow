import logging
from datetime import timedelta

from .clock import Clock, RealClock

logger = logging.getLogger("fusion_flow")

DEFAULT_RETRY_BASE_S = 30.0
DEFAULT_RETRY_CAP_S = 1800.0
JITTER_LOW = 0.8
JITTER_HIGH = 1.2


class Backoff:
    # delay = min(base * 2**attempt, cap) * jitter(0.8..1.2); jitter source is the
    # injectable Clock so tests never sleep.

    def __init__(
        self,
        clock: Clock | None = None,
        base_s: float = DEFAULT_RETRY_BASE_S,
        cap_s: float = DEFAULT_RETRY_CAP_S,
    ):
        self.clock = clock or RealClock()
        self.base_s = base_s
        self.cap_s = cap_s

    def delay(self, attempt: int) -> timedelta:
        raw = min(self.base_s * (2**attempt), self.cap_s)
        jittered = raw * self.clock.jitter(JITTER_LOW, JITTER_HIGH)
        return timedelta(seconds=jittered)
