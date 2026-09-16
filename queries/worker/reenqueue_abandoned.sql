UPDATE
    apalis.jobs
SET
    status = 'Pending',
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    attempts = attempts + 1,
    last_result = '{"Err": "Re-enqueued due to worker shutdown"}' :: jsonb
FROM
    apalis.workers
WHERE
    apalis.jobs.lock_by = apalis.workers.id
    AND (apalis.jobs.status = 'Queued' OR apalis.jobs.status = 'Running')
    AND apalis.workers.worker_type = $1
    AND apalis.workers.id = $2
    AND apalis.jobs.id = ANY($3);
