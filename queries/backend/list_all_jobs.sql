SELECT
    *
FROM
    apalis.jobs
WHERE
    status = $1
ORDER BY
    done_at DESC,
    run_at DESC
LIMIT
    $2 OFFSET $3
