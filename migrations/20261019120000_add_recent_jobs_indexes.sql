CREATE INDEX IF NOT EXISTS idx_apalis_jobs_status_type_recent
    ON apalis.jobs (
        status,
        job_type,
        done_at DESC,
        run_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_apalis_jobs_status_recent
    ON apalis.jobs (
        status,
        done_at DESC,
        run_at DESC
    );
