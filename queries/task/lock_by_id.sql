UPDATE
    apalis.jobs
SET
    status = 'Running',
    lock_at = now(),
    lock_by = $2
WHERE
    (
        status = 'Pending'
        OR status = 'Queued'
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
    AND run_at < now()
    AND id = ANY($1)
RETURNING
    job,
    id,
    job_type,
    status,
    attempts,
    max_attempts,
    run_at as "run_at: chrono::DateTime<chrono::Utc>",
    last_result,
    lock_at as "lock_at: chrono::DateTime<chrono::Utc>",
    lock_by,
    done_at as "done_at: chrono::DateTime<chrono::Utc>",
    priority,
    metadata;
