ALTER TABLE
    apalis.jobs
ADD
    COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON apalis.jobs(job_type, idempotency_key);
