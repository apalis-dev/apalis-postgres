WITH stale AS (
    SELECT
        jobs.id
    FROM
        apalis.jobs
        INNER JOIN apalis.workers ON jobs.lock_by = workers.id
    WHERE
        (
            jobs.status = 'Running'
            OR jobs.status = 'Queued'
        )
        AND NOW() - workers.last_seen >= $1
        AND workers.worker_type = $2 FOR
    UPDATE
        OF jobs SKIP LOCKED
)
UPDATE
    apalis.jobs
SET
    status = 'Pending',
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    attempts = attempts + 1,
    last_result = '{"Err": "Re-enqueued due to worker heartbeat timeout."}'
FROM
    stale
WHERE
    apalis.jobs.id = stale.id;
