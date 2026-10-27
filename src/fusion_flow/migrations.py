SCHEMA_001 = """
CREATE TABLE IF NOT EXISTS flow_jobs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  dedup_key TEXT,
  dedup_fingerprint TEXT,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 5,
  next_retry_at TEXT,
  claimed_by TEXT, claimed_at TEXT, claim_expires_at TEXT,
  created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
  last_error TEXT,
  result_ref TEXT
);
CREATE INDEX IF NOT EXISTS idx_flow_jobs_dequeue ON flow_jobs(status, priority, next_retry_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_flow_jobs_dedup ON flow_jobs(dedup_fingerprint)
  WHERE status IN ('queued','processing');
CREATE INDEX IF NOT EXISTS idx_flow_jobs_dedup_ttl
  ON flow_jobs(dedup_fingerprint, status, finished_at);

CREATE TABLE IF NOT EXISTS flow_subtasks (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES flow_jobs(id),
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  detail TEXT,
  attempts INTEGER DEFAULT 0,
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_flow_subtasks_job ON flow_subtasks(job_id);

CREATE TABLE IF NOT EXISTS flow_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT, subtask_id TEXT,
  event TEXT NOT NULL,
  data TEXT, ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_flow_events_job ON flow_events(job_id);
CREATE INDEX IF NOT EXISTS idx_flow_events_ts ON flow_events(ts);

CREATE TABLE IF NOT EXISTS flow_kv (
  key TEXT PRIMARY KEY,
  value TEXT,
  expires_at TEXT
);

CREATE TABLE IF NOT EXISTS flow_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT,
  applied_at TEXT NOT NULL
);
"""

# Each migration is (version, name, up_sql, down_sql). Every entry must have a
# working down script — test_migrations enforces the symmetry.
MIGRATIONS = [
    (
        1,
        "init_schema",
        SCHEMA_001,
        # flow_migrations itself survives a rollback — it is bookkeeping, not
        # user data; migrate_down deletes the version row separately.
        """
        DROP TABLE IF EXISTS flow_subtasks;
        DROP TABLE IF EXISTS flow_events;
        DROP TABLE IF EXISTS flow_kv;
        DROP TABLE IF EXISTS flow_jobs;
        """,
    ),
]
