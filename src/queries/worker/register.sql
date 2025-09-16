INSERT INTO
    apalis.workers (id, worker_type, storage_name, layers, last_seen)
VALUES
    ($1, $2, $3, $4, $5) ON CONFLICT (id) DO
UPDATE
SET
    worker_type = EXCLUDED.worker_type,
    storage_name = EXCLUDED.storage_name,
    layers = EXCLUDED.layers,
    last_seen = NOW()
WHERE
    pg_try_advisory_lock(hashtext(workers.id));
