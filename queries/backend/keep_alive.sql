UPDATE
    apalis.workers
SET
    last_seen = NOW()
WHERE
    id = $1 AND worker_type = $2;
