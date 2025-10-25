UPDATE
    apalis.jobs
SET
    status = $4,
    attempts = $2,
    last_result = $3,
    done_at = NOW()
WHERE
    id = $1
    AND lock_by = $5
