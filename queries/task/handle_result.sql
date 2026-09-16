WITH j AS (
    SELECT
        (value ->> 'task_id')::text AS task_id,
        (value ->> 'attempt')::integer AS attempt,
        value -> 'result' AS result,
        value ->> 'status' AS status
    FROM jsonb_array_elements($1::jsonb) AS value
),
locked AS (
    SELECT jobs.id
    FROM apalis.jobs AS jobs
    INNER JOIN j ON j.task_id = jobs.id
    WHERE jobs.lock_by = $2
    ORDER BY jobs.id
    FOR UPDATE
)
UPDATE apalis.jobs AS jobs
SET
    status = j.status,
    attempts = j.attempt,
    last_result = j.result,
    done_at = NOW()
FROM j
INNER JOIN locked ON locked.id = j.task_id
WHERE jobs.id = locked.id;
