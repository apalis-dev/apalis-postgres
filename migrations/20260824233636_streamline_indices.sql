DROP INDEX IF EXISTS apalis.TIdx;

DROP INDEX IF EXISTS apalis.SIdx;

DROP INDEX IF EXISTS apalis.JTIdx;

DROP INDEX IF EXISTS apalis.Idx;

CREATE INDEX IF NOT EXISTS idx_apalis_jobs_fetch_partial ON apalis.jobs (
    job_type,
    priority DESC,
    run_at ASC
)
WHERE
    status = 'Pending'
    OR (
        status = 'Failed'
        AND attempts < max_attempts
    );

CREATE INDEX IF NOT EXISTS idx_apalis_jobs_lock_by ON apalis.jobs (lock_by);

-- Keeps worker heartbeats fast
CREATE INDEX IF NOT EXISTS idx_apalis_workers_last_seen ON apalis.workers (last_seen);

CREATE INDEX IF NOT EXISTS idx_apalis_workers_worker_type ON apalis.workers (worker_type);
