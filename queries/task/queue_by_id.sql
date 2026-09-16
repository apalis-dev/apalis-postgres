UPDATE apalis.jobs
SET 
    status = 'Queued',
    lock_at = now()
WHERE 
    status = 'Pending'
    AND run_at < now()
    AND id = ANY($1)
RETURNING *;
