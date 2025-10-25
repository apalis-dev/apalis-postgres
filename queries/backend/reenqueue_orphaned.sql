UPDATE
    apalis.jobs
SET
    status = 'Pending',
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    attempts = attempts + 1,
    last_result = '{"Err": "Re-enqueued due to worker heartbeat timeout."}'
WHERE
    id IN (
        SELECT
            jobs.id
        FROM
            apalis.jobs
            INNER JOIN apalis.workers ON lock_by = workers.id
        WHERE
            (
                status = 'Running'
                OR status = 'Queued'
            )
            AND NOW() - apalis.workers.last_seen >= $1
            AND apalis.workers.worker_type = $2
    );
