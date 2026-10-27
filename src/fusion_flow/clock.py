import random
import time
from datetime import UTC, datetime, timedelta


class Clock:
    # Injectable time/jitter source so backoff/claim-timeout/rate tests never really sleep.

    def __init__(self, seed=None):
        self._offset = 0.0
        self._rand = random.Random(seed)

    def now(self) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=self._offset)

    def now_iso(self) -> str:
        return self.now().isoformat()

    def advance(self, seconds: float) -> None:
        self._offset += seconds

    def jitter(self, low: float = 0.8, high: float = 1.2) -> float:
        return self._rand.uniform(low, high)

    def sleep(self, seconds: float) -> None:
        # Test clock: advancing instead of blocking.
        self.advance(seconds)


class RealClock(Clock):
    def __init__(self):
        super().__init__()
        self._rand = random.SystemRandom()

    def now(self) -> datetime:
        return datetime.now(UTC)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def parse_duration(value: str | timedelta) -> timedelta:
    # Accepts "30s", "5m", "2h", "1d" or a timedelta.
    if isinstance(value, timedelta):
        return value
    text = value.strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text and text[-1] in units:
        return timedelta(seconds=float(text[:-1]) * units[text[-1]])
    return timedelta(seconds=float(text))
