import argparse
import json
import logging
import os
import sys

from .clock import parse_duration
from .errors import FlowError
from .flow import Flow, WorkerLock
from .models import Job, StatsSnapshot

logger = logging.getLogger("fusion_flow")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_NOT_FOUND = 2

DEFAULT_DB = os.environ.get("FLOW_DB", "flow.db")
BUSY_RETRIES = 3
BUSY_RETRY_DELAY_S = 1.0


def _flow(args) -> Flow:
    return Flow(args.db, migrate=True)


def _warn_if_locked(args) -> None:
    # Read-only/maintenance CLI never takes the worker lock; it only warns so
    # operators notice a live worker is mid-flight on the same database.
    lock_path = f"{args.db}.worker.lock"
    if not os.path.exists(lock_path):
        return
    try:
        with open(lock_path, "a+") as fh:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except (OSError, ImportError):
        print(
            f"WARNING: a live worker holds {lock_path}; data below may change under you",
            file=sys.stderr,
        )


def _job_dict(job: Job) -> dict:
    data = {
        "id": job.id,
        "kind": job.kind,
        "status": job.status,
        "priority": job.priority,
        "attempts": job.attempts,
        "max_retries": job.max_retries,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
        "last_error": job.last_error,
        "result_ref": job.result_ref,
    }
    if hasattr(job, "created"):
        data["created"] = job.created
    return data


def _print_jobs(jobs, as_json: bool) -> None:
    if as_json:
        print(json.dumps([_job_dict(j) for j in jobs], indent=2))
        return
    if not jobs:
        print("no jobs")
        return
    for j in jobs:
        err = f" err={j.last_error}" if j.last_error else ""
        print(
            f"{j.id}  {j.status:<16} kind={j.kind} prio={j.priority} "
            f"attempts={j.attempts}/{j.max_retries}{err}"
        )


def cmd_ls(args) -> int:
    _warn_if_locked(args)
    flow = _flow(args)
    jobs = flow.list_jobs(status=args.status, kind=args.kind, limit=args.limit)
    _print_jobs(jobs, args.json)
    return EXIT_OK


def cmd_show(args) -> int:
    _warn_if_locked(args)
    flow = _flow(args)
    view = flow.get_job(args.job_id)
    if view is None:
        print(f"job not found: {args.job_id}", file=sys.stderr)
        return EXIT_NOT_FOUND
    if args.json:
        print(
            json.dumps(
                {
                    "job": _job_dict(view.job),
                    "payload": view.job.payload,
                    "subtasks": [
                        {
                            "id": s.id,
                            "name": s.name,
                            "status": s.status,
                            "attempts": s.attempts,
                            "detail": s.detail,
                        }
                        for s in view.subtasks
                    ],
                    "events": [
                        {
                            "event": e.event,
                            "ts": e.ts,
                            "job_id": e.job_id,
                            "data": e.data,
                        }
                        for e in flow.events(job_id=view.job.id, limit=args.events)
                    ],
                },
                indent=2,
                default=str,
            )
        )
        return EXIT_OK
    j = view.job
    print(f"id:        {j.id}")
    print(f"kind:      {j.kind}")
    print(f"status:    {j.status}")
    print(f"payload:   {json.dumps(j.payload, ensure_ascii=False)}")
    print(f"attempts:  {j.attempts}/{j.max_retries}")
    print(f"created:   {j.created_at}   finished: {j.finished_at}")
    if j.last_error:
        print(f"last_err:  {j.last_error}")
    if j.result_ref:
        print(f"result:    {j.result_ref}")
    if view.subtasks:
        print("subtasks:")
        for s in view.subtasks:
            print(f"  {s.name:<24} {s.status:<10} attempts={s.attempts}")
    print("events:")
    for e in flow.events(job_id=j.id, limit=args.events):
        print(f"  {e.ts}  {e.event}  {json.dumps(e.data or {}, ensure_ascii=False)}")
    return EXIT_OK


def _mutating_cmd(fn) -> int:
    # Mutation commands race a live worker at most at the SQLite level;
    # busy_timeout absorbs contention, FlowError maps to exit code 1.

    def run(args) -> int:
        _warn_if_locked(args)
        flow = _flow(args)
        try:
            return fn(flow, args)
        except KeyError:
            print(f"job not found: {args.job_id}", file=sys.stderr)
            return EXIT_NOT_FOUND
        except FlowError as e:
            print(f"error: {e}", file=sys.stderr)
            return EXIT_ERROR

    return run


@_mutating_cmd
def cmd_retry(flow, args) -> int:
    job = flow.retry(args.job_id)
    print(f"retried {args.job_id} -> {job.id} (queued)")
    return EXIT_OK


@_mutating_cmd
def cmd_replay(flow, args) -> int:
    job = flow.replay(args.job_id)
    print(f"replayed {args.job_id} -> {job.id} (queued)")
    return EXIT_OK


@_mutating_cmd
def cmd_cancel(flow, args) -> int:
    job = flow.cancel(args.job_id)
    print(f"cancelled {job.id} (was {job.status})")
    return EXIT_OK


@_mutating_cmd
def cmd_purge(flow, args) -> int:
    statuses = [s.strip() for s in args.statuses.split(",") if s.strip()]
    if not statuses:
        print("error: no statuses given", file=sys.stderr)
        return EXIT_ERROR
    count = flow.purge(parse_duration(args.older_than), statuses)
    print(f"purged {count} jobs")
    return EXIT_OK


def cmd_stats(args) -> int:
    _warn_if_locked(args)
    flow = _flow(args)
    snap = flow.stats()
    if args.json:
        print(json.dumps(_stats_dict(snap), indent=2))
        return EXIT_OK
    print("status counts:")
    for status in sorted(snap.by_status):
        print(f"  {status:<18} {snap.by_status[status]}")
    if snap.oldest_queued_age_s is not None:
        print(f"oldest queued age: {snap.oldest_queued_age_s:.0f}s")
    print(f"dead letters: {snap.dead_letter_count}")
    if snap.kind_throughput:
        print("throughput (24h, finished jobs):")
        for kind in sorted(snap.kind_throughput):
            print(f"  {kind:<24} {snap.kind_throughput[kind]}")
    return EXIT_OK


def _stats_dict(snap: StatsSnapshot) -> dict:
    return {
        "by_status": snap.by_status,
        "oldest_queued_age_s": snap.oldest_queued_age_s,
        "dead_letter_count": snap.dead_letter_count,
        "kind_throughput": snap.kind_throughput,
    }


def cmd_vacuum(args) -> int:
    _warn_if_locked(args)
    flow = _flow(args)
    flow.vacuum()
    print(f"vacuumed {args.db}")
    return EXIT_OK


def _read_holder(lock_path: str) -> dict | None:
    # Holder record written by the lock owner (0.2.2+); None when the file is
    # empty or pre-dates holder records.
    try:
        with open(lock_path) as fh:
            data = fh.read().strip()
        if not data:
            return None
        parsed = json.loads(data)
        return parsed if isinstance(parsed, dict) else None
    except (OSError, ValueError):
        return None


def cmd_locks(args) -> int:
    lock_path = f"{args.db}.worker.lock"
    exists = os.path.exists(lock_path)
    held = False
    if exists:
        try:
            with open(lock_path, "a+") as fh:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except (OSError, ImportError):
            held = True
    holder = _read_holder(lock_path) if exists else None
    if args.json:
        print(
            json.dumps(
                {
                    "lock_file": lock_path,
                    "exists": exists,
                    "worker_holding": held,
                    "holder": holder,
                },
                indent=2,
            )
        )
        return EXIT_OK
    print(f"worker lock: {lock_path}")
    print(f"  file exists:  {exists}")
    print(f"  held by live worker: {held}")
    if holder:
        print(
            f"  holder:       pid={holder.get('pid')} host={holder.get('host')} "
            f"since={holder.get('acquired_at')}"
        )
    return EXIT_OK


def cmd_unlock(args) -> int:
    result = WorkerLock.unlock(args.db, force=args.force)
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    elif result["removed"]:
        print(f"removed worker lock {args.db}.worker.lock ({result['reason']})")
    else:
        print(f"lock not removed: {result['reason']}", file=sys.stderr)
    return EXIT_OK if result["removed"] else EXIT_ERROR


def cmd_lease_reclaim(args) -> int:
    _warn_if_locked(args)
    flow = _flow(args)
    recovered = flow.queue.recover_expired_claims()
    if args.json:
        print(json.dumps({"recovered": recovered}, indent=2))
        return EXIT_OK
    if recovered:
        for job_id in recovered:
            print(f"reclaimed {job_id} -> queued")
    else:
        print("no expired claims")
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flow", description="fusion-flow job queue CLI")
    parser.add_argument(
        "--db", default=DEFAULT_DB, help=f"SQLite database path (default: {DEFAULT_DB})"
    )
    parser.add_argument("--json", action="store_true", help="machine-readable JSON output")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ls", help="list jobs")
    p.add_argument("--status", default=None)
    p.add_argument("--kind", default=None)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_ls)

    p = sub.add_parser("show", help="show job detail + events")
    p.add_argument("job_id")
    p.add_argument("--events", type=int, default=20)
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("retry", help="copy a dead-lettered job to a fresh queued job")
    p.add_argument("job_id")
    p.set_defaults(func=cmd_retry)

    p = sub.add_parser("replay", help="copy a succeeded job as a new run")
    p.add_argument("job_id")
    p.set_defaults(func=cmd_replay)

    p = sub.add_parser("cancel", help="cancel a queued or dead-lettered job")
    p.add_argument("job_id")
    p.set_defaults(func=cmd_cancel)

    p = sub.add_parser("purge", help="delete old jobs in given terminal statuses")
    p.add_argument("--older-than", default="7d", help="e.g. 30s, 5m, 2h, 7d")
    p.add_argument(
        "--statuses", default="succeeded,cancelled", help="comma-separated statuses to purge"
    )
    p.set_defaults(func=cmd_purge)

    p = sub.add_parser("stats", help="queue statistics")
    p.set_defaults(func=cmd_stats)

    p = sub.add_parser("vacuum", help="VACUUM the SQLite database")
    p.set_defaults(func=cmd_vacuum)

    p = sub.add_parser("locks", help="show worker lock state + holder identity")
    p.set_defaults(func=cmd_locks)

    p = sub.add_parser(
        "unlock", help="remove a stale worker lock file (refuses while a live holder holds it)"
    )
    p.add_argument("--force", action="store_true", help="remove even if the holder looks alive")
    p.set_defaults(func=cmd_unlock)

    p = sub.add_parser("lease-reclaim", help="requeue jobs whose claims expired")
    p.set_defaults(func=cmd_lease_reclaim)

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FlowError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
