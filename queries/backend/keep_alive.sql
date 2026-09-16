UPDATE apalis.workers w
SET last_seen = NOW()
WHERE
    w.id = $1
    AND w.worker_type = $2
    AND (
        SELECT COUNT(*)
        FROM apalis.jobs j
        WHERE j.lock_by = w.id
          AND j.id = ANY($3::text[])
    ) = cardinality($3::text[]);
