SELECT
    *
FROM
    apalis.jobs
WHERE
    id = $1
LIMIT
    1;
