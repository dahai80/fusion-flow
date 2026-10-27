import copy
import json
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime, timedelta

from ulid import ULID

JOB_STATUSES = (
    "queued",
    "processing",
    "succeeded",
    "partial_succeeded",
    "failed",
    "dead_lettered",
    "cancelled",
    "superseded",
)
TERMINAL_JOB_STATUSES = (
    "succeeded",
    "partial_succeeded",
    "failed",
    "dead_lettered",
    "cancelled",
    "superseded",
)

SUBTASK_STATUSES = ("pending", "running", "succeeded", "failed", "skipped")
TERMINAL_SUBTASK_STATUSES = ("succeeded", "failed", "skipped")

# Hard ceiling regardless of configuration: a single payload may never exceed 1 MiB.
PAYLOAD_HARD_LIMIT = 1024 * 1024
DEFAULT_PAYLOAD_MAX_BYTES = 64 * 1024


def new_job_id() -> str:
    return f"flw_{ULID()}"


@dataclass(frozen=True)
class JobSpec:
    kind: str
    payload: dict = field(default_factory=dict)
    dedup_key: str | None = None
    dedup_ttl: timedelta | None = None
    max_retries: int = 5
    priority: int = 100

    def __post_init__(self):
        # Deep-copy inputs: frozen=True does not stop in-place mutation of nested dicts,
        # so we own our own copy and never hand out the internal reference.
        object.__setattr__(self, "payload", copy.deepcopy(dict(self.payload)))
        if self.dedup_ttl is not None and not isinstance(self.dedup_ttl, timedelta):
            raise ValueError("dedup_ttl must be a timedelta")

    def payload_bytes(self) -> bytes:
        return json.dumps(self.payload, sort_keys=True, separators=(",", ":")).encode()

    def payload_json(self) -> str:
        return self.payload_bytes().decode()


@dataclass(frozen=True)
class SubTask:
    id: str
    job_id: str
    name: str
    status: str
    detail: dict | None = None
    attempts: int = 0
    updated_at: str | None = None

    def __post_init__(self):
        if self.detail is not None:
            object.__setattr__(self, "detail", copy.deepcopy(dict(self.detail)))


def _from_dict(cls, data: dict):
    # Forward-compatible deserialization: unknown keys are ignored, missing keys fall
    # back to dataclass defaults, field access never trusts raw input.
    known = {f.name for f in fields(cls)}
    kwargs = {k: copy.deepcopy(v) for k, v in data.items() if k in known}
    try:
        return cls(**kwargs)
    except TypeError:
        return cls(**{k: v for k, v in kwargs.items() if k in known})


@dataclass(frozen=True)
class Job:
    id: str
    kind: str
    payload: dict = field(default_factory=dict)
    dedup_key: str | None = None
    status: str = "queued"
    priority: int = 100
    attempts: int = 0
    max_retries: int = 5
    next_retry_at: str | None = None
    claimed_by: str | None = None
    claimed_at: str | None = None
    claim_expires_at: str | None = None
    created_at: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    last_error: str | None = None
    result_ref: str | None = None
    dedup_fingerprint: str | None = None
    # True when this very enqueue call created the row; False on a dedup /
    # dedup_ttl reuse hit. Not a database column.
    created: bool = True

    def __post_init__(self):
        object.__setattr__(self, "payload", copy.deepcopy(dict(self.payload or {})))

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_JOB_STATUSES

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        return _from_dict(cls, data)


@dataclass(frozen=True)
class JobView:
    job: Job
    subtasks: tuple[SubTask, ...] = ()


@dataclass(frozen=True)
class StatsSnapshot:
    by_status: dict
    oldest_queued_age_s: float | None
    dead_letter_count: int
    kind_throughput: dict


@dataclass
class FlowEvent:
    event: str
    job_id: str | None = None
    subtask_id: str | None = None
    data: dict | None = None
    ts: str = ""

    def __post_init__(self):
        if not self.ts:
            self.ts = datetime.now(UTC).isoformat()
        if self.data is not None:
            self.data = copy.deepcopy(dict(self.data))


__all__ = [
    "JOB_STATUSES",
    "TERMINAL_JOB_STATUSES",
    "SUBTASK_STATUSES",
    "TERMINAL_SUBTASK_STATUSES",
    "PAYLOAD_HARD_LIMIT",
    "DEFAULT_PAYLOAD_MAX_BYTES",
    "new_job_id",
    "JobSpec",
    "SubTask",
    "Job",
    "JobView",
    "StatsSnapshot",
    "FlowEvent",
]
