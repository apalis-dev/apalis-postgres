UPDATE
    apalis.workers w
SET
    last_seen = NOW()
WHERE
    w.id = $1
    AND w.worker_type = $2;
