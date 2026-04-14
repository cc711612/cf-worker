CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'done', 'failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_run_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  dedupe_key TEXT,
  locked_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_next_run_at ON jobs(status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_status_locked_at ON jobs(status, locked_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_type_dedupe_key
  ON jobs(type, dedupe_key)
  WHERE dedupe_key IS NOT NULL;
