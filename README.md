# fusion-flow

Persistent in-process job orchestration for the Fusion ecosystem: a durable queue, an
auditable state machine, per-kind rate limiting, retry with exponential backoff, and
crash-safe workers — all on one SQLite file, with zero internal `fusion-*` dependencies.

Runtime dependencies: `anyio` and `python-ulid`. That is the whole list.

- **Durable by default** — every transition commits to SQLite (WAL mode); a `kill -9`
  mid-job self-heals via claim-timeout recovery.
- **Single-writer, no race** — one process per database runs workers, enforced by a
  sidecar file lock; claim/ack/nack are atomic conditional updates.
- **Auditable** — every transition writes an event row in the same transaction; event
  hooks fire post-commit and can never roll back queue state.
- **Resumable** — retry (dead-lettered) and replay (succeeded) copy the job to a fresh
  row with `attempts` reset, leaving the original audit trail untouched.

## Quick Start (5 Minutes)

```bash
cd /path/to/fusion && source .venv/bin/activate
cd fusion-flow && pip install -e ".[test]"
```

```python
import anyio
from fusion_flow import Flow, JobSpec

flow = Flow("jobs.db", workers=2)


@flow.handler("resize_image")
async def resize(ctx, payload):
    # ... do the work ...
    return "s3://results/out.png"  # a reference, not the bytes (see below)


async def main():
    async with flow.run():  # acquires the worker lock, starts the pool
        job = flow.enqueue(JobSpec(kind="resize_image", payload={"uri": "..."}))
        print(job.id, job.status)  # flw_... queued


anyio.run(main)
```

Sync handlers work too — they run in a bounded thread pool:

```python
@flow.handler("ocr")
def ocr(ctx, payload):  # plain def, not async def
    return "ref://ocr-result"
```

Key facts:

- `flow.enqueue()` / `flow.get_job()` / `flow.stats()` are **sync** — call them from
  business code freely. `flow.run()` is the only async entry point.
- Pass `dedup_key="file:42"` to collapse duplicate enqueues of the same logical work
  while a copy is still queued or processing; add `dedup_ttl=timedelta(minutes=5)` to
  also reuse a recent **succeeded** result (returned job comes back with
  `job.created=False` and the old `result_ref`).
- `clock=Clock(seed=...)` is injectable for tests: backoff, claim expiry, and rate
  limits all read from it, so tests never sleep.

## Handler Exception Classification

The worker's exception handling is a fixed contract. Pick the branch deliberately:

```python
from fusion_flow.errors import PermanentError, TransientError


@flow.handler("mockup_batch", fail_fast=True)
async def mockup_handler(ctx, payload):
    try:
        ...
    except (InputShapeError, DegenerateQuadError, UnsupportedModeError) as e:
        # Business verdict: the INPUT is bad. Retrying cannot help.
        raise PermanentError(f"bad asset parameters, not retryable: {e}") from e
    except OSError as e:
        # Infrastructure hiccup: retry with backoff.
        raise TransientError("temporary IO failure, retrying") from e
    # Anything else (unexpected Exception):
    #   normal handler  -> nack + backoff retry, dead-letter when exhausted
    #   fail_fast=True  -> dead_lettered immediately + FATAL event for alerting
```

Decision rule:

| Exception | Result | Retries consumed? |
| --- | --- | --- |
| `TransientError` | nack, backoff, re-queued | yes |
| any unexpected `Exception` (normal handler) | nack, backoff, re-queued | yes |
| `PermanentError` | `failed` (business-terminal) | no |
| any `Exception` with `fail_fast=True` | `dead_lettered` + `fatal` event | no |
| `KeyboardInterrupt` / `SystemExit` | pool stops, exception re-raised; job is **never** nacked | no |

- `failed` = the business decided this job can never succeed.
- `dead_lettered` = retries exhausted (or fatal). Use `flow retry <id>` to copy it to a
  fresh job after fixing the cause.

## Drain Timeout Consumes Retries

**When `flow.run()` exits, in-flight async handlers get a grace period
(`drain_timeout_s`, default 30s). If an async handler is cancelled at drain timeout,
the job is nacked with `last_error="worker_drain_timeout:cancelled"` —
that nack increments `attempts` and consumes one retry.**

Sync handlers cannot be cancelled at all: the thread keeps running and the job is
abandoned in `processing`; claim-timeout recovery requeues it later, and that recovery
also counts as an attempt. Set `max_retries` and `drain_timeout_s` with this in mind.

## Rate Limiting Is Best-Effort, In-Memory

```python
flow.rate.set_rate("drive_poll", "2/min")  # or Flow(..., rate_per_kind={...})
```

The token bucket lives in process memory. Consequences:

- **Restarting the process resets the bucket** — a fresh burst may exceed the intended
  rate right after restart.
- **Multiple worker processes are impossible by design** (one writer per database,
  enforced by the lock), but multiple *enqueueing* processes each get their own bucket
  view. Cross-process limiting is the caller's concern, not the library's.

## SQLite Durability Tradeoff (`synchronous=NORMAL`)

Default pragmas: `journal_mode=WAL` (not overridable), `synchronous=NORMAL`,
`busy_timeout=5000`, `foreign_keys=ON`.

- **Process crash (`kill -9`): no committed transaction is lost.** This is the crash
  mode the claim-recovery machinery is designed around.
- **Physical power loss: the most recent commits may be lost** (WAL not yet fsynced).

If you need zero loss at any cost, override at open time:

```python
flow = Flow("jobs.db", sqlite_pragmas={"synchronous": "FULL"})  # lower write throughput
```

## Payload Constraints: IDs and URIs Only

`payload` is capped (64 KiB default, 1 MiB hard ceiling) and dedup fingerprints hash it
byte-for-byte. Put **only identifiers and references** in it:

```python
JobSpec(kind="resize", payload={"uri": "file:///data/in.png"})  # good
JobSpec(kind="resize", payload=my_8mb_image_bytes)  # PayloadTooLarge, and wrong
```

Large inputs and outputs live in object storage / the filesystem; the queue carries
pointers. Handler return values follow the same rule: return a `result_ref` string
(`ref://...`, `s3://...`), not the data itself.

## Sizing Workers and Writing Handlers That Survive Drain

- **Start with `workers=2`.** Budget: `workers × peak-memory-of-one-job < available RAM`.
  A memory-heavy consumer (e.g. image pipelines) must do this arithmetic before raising
  the number, or the OOM killer will raise it for you.
- **Prefer async handlers.** A sync handler doing long CPU work cannot be cancelled by
  the drain timeout (threads are not killable) and blocks `flow.run()` from returning
  until it finishes. Async handlers cancel cleanly and get nacked for retry.
- Every handler must be **idempotent**: a job can run more than once (retry after
  crash, retry after drain cancel, claim-timeout recovery).

## Connectors (Side-Channel Sources)

A connector bridges an external source (e.g. Google Drive) into the queue. Register
before `run()`; the runner is a separate task next to the worker pool:

```python
from fusion_flow import DriveConnector


def on_new_file(folder_id, file):
    # Business mapping lives HERE, not in the library.
    return JobSpec(kind="photo", payload={"file_id": file["id"]}, dedup_key=f"drive:{file['id']}")


conn = DriveConnector(
    drive_client,
    on_new_file=on_new_file,
    event_bus=flow.event_bus,
    inbox_dir="./inbox",
    output_folder_id="...",
    rejected_folder_id="...",
)
flow.attach_connector(conn, name="drive", interval_s=60, kinds={"photo"})
```

The `Connector` protocol is three methods: `poll() -> list[JobSpec]`, `commit()`,
`on_job_done(job, result_ref)`. Contracts worth memorizing:

- **Zero-loss backpressure**: if any enqueue raises `QueueFull`, the WHOLE poll round
  is treated as failed — `commit()` is not called, so the connector's cursor (Drive
  pageToken) stays put and the next round re-pulls the same files. Specs are never
  dropped on a full queue; the upstream will not re-deliver them for you.
- **Invalid specs are dropped, not fatal**: an oversized payload or malformed kind
  from a business callback emits a `connector_spec_dropped` event and the round
  continues (a poison spec must not wedge the connector forever).
- **Full isolation**: a raising `poll()` never touches the workers — it logs a
  `connector_error` event and backs off (5s base, 300s cap). Jobs already in the
  queue keep flowing.
- **Reverse path**: `on_job_done` fires post-ack (scoped by `kinds`), retried with
  backoff; when retries are exhausted a `connector_error` event is emitted for
  compensation — the job's terminal state is never rolled back.
- `DriveConnector` stores its pageToken in `flow_kv` with **no TTL** (durable cursor,
  not a cache) and takes an injected transport client — the library has zero Google
  imports; `pip install "fusion-flow[drive]"` pulls the client libraries for the
  caller-side adapter.

## Troubleshooting Quick Reference

| Symptom | Check |
| --- | --- |
| Job stuck in `processing` | `claim_expires_at` in `flow show <id>` — recovery fires once it passes; force it with `flow lease-reclaim` |
| `WorkerLockHeld` on `flow.run()` | Another live process owns the workers for this DB: `flow locks` shows who holds `<db>.worker.lock` |
| `database is locked` errors | More than one process writing; only one process may run `flow.run()` per database — route reads through the CLI instead |
| Rate limit "not working" | Process restarted (bucket reset) or multiple enqueueing processes; the limiter is in-memory by design |
| `dead_lettered` job you fixed | `flow retry <id>` copies it to a fresh queued job with `attempts=0` |
| Connector stopped enqueuing | `connector_error` events in `flow show <id> --events` / event list; the runner backs off exponentially and keeps retrying |
| Disk growing | `flow purge --older-than 7d` for terminal jobs, `flow vacuum` to reclaim space |

## CLI Reference

```text
flow --db jobs.db <command>            # --db defaults to $FLOW_DB or flow.db; --json everywhere

  ls [--status S] [--kind K] [--limit N]   list jobs
  show <job_id> [--events N]               job detail + subtasks + audit trail
  retry <job_id>                           copy dead-lettered job to a fresh queued job
  replay <job_id>                          copy succeeded job as a new run
  cancel <job_id>                          cancel a queued job
  purge --older-than 7d --statuses succeeded,cancelled
  stats                                    queue snapshot (by status, ages, throughput)
  vacuum                                   VACUUM the database
  locks                                    worker-lock state
  lease-reclaim                            requeue jobs whose claims expired
```

Exit codes: `0` ok, `1` FlowError, `2` not found. The CLI never takes the worker lock;
it warns on stderr when a live worker holds it.

## Development

```bash
cd /path/to/fusion && source .venv/bin/activate
cd fusion-flow
pip install -e ".[test]"
pytest tests/ -q                # full suite (crash/kill -9 tests included)
pytest tests/ -q -m bench       # throughput smoke bench (CI-only marker)
ruff check . && ruff format --check .
```

- Python >= 3.12, 4-space indent, no docstrings (comments instead), logging everywhere.
- SQLite is the only storage: `storage.py` is the single module that executes SQL.
- Migrations: one entry per version in `migrations.py`, each with up AND down SQL;
  `flow_migrations` bookkeeping survives a rollback.

Apache-2.0. See [README_CN.md](README_CN.md) for the Chinese documentation.
