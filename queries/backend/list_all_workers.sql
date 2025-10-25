SELECT
    *
FROM
    apalis.workers
ORDER BY
    last_seen DESC
LIMIT
    $1 OFFSET $2
