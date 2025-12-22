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
    status = $1
ORDER BY
    done_at DESC,
    run_at DESC
LIMIT
    $2 OFFSET $3
