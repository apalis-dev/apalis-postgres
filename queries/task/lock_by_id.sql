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
    AND id = ANY($1) RETURNING *;
