SELECT
    id,
    worker_type,
    storage_name,
    layers,
    last_seen as "last_seen: chrono::DateTime<chrono::Utc>",
    started_at as "started_at: chrono::DateTime<chrono::Utc>"
FROM
    apalis.workers
ORDER BY
    last_seen DESC
LIMIT
    $1 OFFSET $2
