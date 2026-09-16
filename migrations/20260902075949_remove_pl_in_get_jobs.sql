CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
)
RETURNS SETOF apalis.jobs
LANGUAGE sql
VOLATILE
AS $$
    WITH jobs AS (
        SELECT id
        FROM apalis.jobs
        WHERE
            (
                status = 'Pending'
                OR (
                    status = 'Failed'
                    AND attempts < max_attempts
                )
            )
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs j
    SET
        status = 'Queued',
        lock_by = worker_id,
        lock_at = now()
    FROM jobs
    WHERE j.id = jobs.id
    RETURNING j.*;
$$;
