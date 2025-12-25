SELECT
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
    metadata
FROM
    apalis.jobs
WHERE
    id = $1
LIMIT
    1;
