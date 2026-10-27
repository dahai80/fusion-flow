# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fusion-flow is a persistent in-process job orchestration **library** (not a service):
durable queue + auditable state machine + per-kind rate limiting + retry with
exponential backoff + crash-safe workers, all on one SQLite (WAL) file. Zero internal
`fusion-*` dependencies; runtime deps are only `anyio` + `python-ulid`. No port, no
daemon — consumer apps embed it.

## Build / Test / Lint

```bash
cd /Users/dahai/fusion && source .venv/bin/activate   # REQUIRED first
cd fusion-flow
pip install -e ".[test]"          # editable install with test extras
pytest tests/ -q                  # full suite (94 tests, incl. kill -9 crash recovery)
pytest tests/ -q -m bench         # throughput smoke bench (excluded by default via addopts)
ruff check . && ruff format --check .
```

CLI entry point: `flow` (installed by pip): `flow --db jobs.db stats|ls|show|retry|replay|cancel|purge|vacuum|locks|lease-reclaim`. Exit codes 0/1/2 (not-found=2). CLI never takes the worker lock.

## Architecture

```
src/fusion_flow/
├── models.py          # frozen dataclasses: JobSpec/Job/SubTask/JobView/StatsSnapshot/FlowEvent
├── state.py           # transition whitelists + subtask aggregation -> IllegalTransition
├── clock.py           # injectable Clock (fake clock: advance(), seedable jitter)
├── storage.py         # ONLY module that executes SQL; per-thread connections; WAL pragmas
├── migrations.py      # versioned up+down SQL registry (flow_ prefix tables)
├── dedup.py           # sha256 fingerprint + validate_jobspec (payload cap)
├── retry.py           # exponential backoff via injectable clock
├── rate.py            # in-memory token buckets per kind; "2/min" parser
├── queue_service.py   # enqueue (dedup/backpressure), atomic claim/ack/nack, recovery, purge
├── event_bus.py       # post-commit callbacks + event retention + flow_kv TTL
├── worker_pool.py     # N anyio workers, exception classification contract, drain semantics
├── flow.py            # Flow facade + WorkerLock (sidecar file lock) + run() lifecycle
└── cli.py             # argparse ops surface
```

## Non-Negotiable Contracts

- **Exception classification** (worker_pool): `TransientError`/unexpected -> nack+retry;
  `PermanentError` -> failed; `fail_fast` handler -> dead_lettered + fatal event;
  `KeyboardInterrupt`/`SystemExit` -> stop + re-raise, NEVER nack.
- **Drain**: async handler cancelled at drain timeout -> nack (`worker_drain_timeout:cancelled`),
  consumes a retry. Sync handler -> ABANDONED in processing (thread unkillable); claim
  recovery owns it. A `anyio.lowlevel.checkpoint()` after the handler await is the
  cancellation delivery point — do not remove.
- **Copy semantics**: retry/replay COPY to a brand-new job (attempts=0); source must be
  terminal or `IllegalTransition` (active copy would collide with the partial dedup index).
- **Crash safety**: claim timeout recovery counts as attempt+1 (poison-loop prevention);
  worker lock is a sidecar FILE (`<db>.worker.lock`, fcntl/msvcrt), never a DB row.
- **Events**: written in the same transaction as the transition; callbacks fire
  post-commit; broken callbacks log exc_info and never propagate.
- `synchronous=NORMAL` default: kill -9 loses nothing, power loss may; caller overrides
  via `sqlite_pragmas={"synchronous": "FULL"}`. `journal_mode=WAL` is NOT overridable.

## Known Pitfalls

- `sqlite3.Row` does NOT unpack with `{**row}` — use `dict(row)` / `row_to_job()`.
- Fake `Clock.now()` is real wall time + offset: tests advance it explicitly instead of sleeping.
- asyncio hijacks KeyboardInterrupt at the Task layer (Task.__step re-raises it out of the
  loop) — the KI test must wrap `anyio.run()` in a sync test to catch it.
- Only one process may run `Flow.run()` per database (`WorkerLockHeld` otherwise).

## Pending Work (post-M1)

- M2: `connector.py` ConnectorRunner + `connectors/drive.py` (QueueFull = whole poll round
  fails, pageToken NOT advanced) — see `/Users/dahai/fusion/architecture/fusion-flow-archi-plan-1027.md` §M2.

## Monorepo Context

One of the `fusion-*` sub-projects in `/Users/dahai/fusion` (local-first Apple Silicon AI
ecosystem). Rules inherited from the workspace:

- **Own git repo**: commit/branch/PR here, never at the workspace root. Remote:
  `github.com/dahai80/fusion-flow` (public).
- **Shared venv**: `cd /Users/dahai/fusion && source .venv/bin/activate` before any Python
  work. Python 3.12 (workspace `.python-version`).
- **No port registration needed** — library, no service.
- **Code style**: 4-space-multiple indentation, no docstrings (comments instead), always
  include logging. README.md English-only; README_CN.md Chinese. GitHub operations in English.
