import hashlib
import json

from .models import JobSpec

MAX_KIND_LEN = 200
MAX_DEDUP_KEY_LEN = 512
_ALLOWED_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:@/#+=,;")


def fingerprint(spec: JobSpec) -> str:
    # sha256(kind + dedup_key) when a dedup_key is declared, otherwise the whole
    # serialized payload participates. sort_keys + compact separators keep the
    # fingerprint stable across processes.
    if spec.dedup_key:
        basis = f"{spec.kind}\x00{spec.dedup_key}"
    else:
        basis = f"{spec.kind}\x00{json.dumps(spec.payload, sort_keys=True, separators=(',', ':'))}"
    return hashlib.sha256(basis.encode()).hexdigest()


def validate_jobspec(spec: JobSpec, payload_max_bytes: int) -> None:
    # Single validation entry used by BOTH enqueue() and ConnectorRunner —
    # connector-produced specs must never bypass these checks.
    if not spec.kind or not isinstance(spec.kind, str):
        raise ValueError("kind must be a non-empty string")
    if len(spec.kind) > MAX_KIND_LEN:
        raise ValueError(f"kind too long (>{MAX_KIND_LEN} chars)")
    if any(ch not in _ALLOWED_CHARS for ch in spec.kind):
        raise ValueError("kind contains characters outside the whitelist")
    if spec.dedup_key is not None:
        if len(spec.dedup_key) > MAX_DEDUP_KEY_LEN:
            raise ValueError(f"dedup_key too long (>{MAX_DEDUP_KEY_LEN} chars)")
        if any(ch not in _ALLOWED_CHARS for ch in spec.dedup_key):
            raise ValueError("dedup_key contains characters outside the whitelist")
    size = len(spec.payload_bytes())
    if size > payload_max_bytes:
        raise _payload_error(size, payload_max_bytes)


def _payload_error(size: int, limit: int):
    from .errors import PayloadTooLarge

    return PayloadTooLarge(
        f"payload is {size} bytes, exceeds limit {limit}; "
        "keep ids/uris in payload and reference large objects externally"
    )
