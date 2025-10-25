UPDATE apalis.jobs
SET 
    status = 'Running',
    lock_at = now(),
    lock_by = $2
WHERE 
    status = 'Queued'
    AND run_at < now()
    AND id = ANY($1)
RETURNING *;
