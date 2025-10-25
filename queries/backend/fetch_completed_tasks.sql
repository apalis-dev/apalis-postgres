SELECT
    id,
    status,
    last_result AS result
FROM
    apalis.jobs
WHERE
    id IN (
        SELECT
            value::text
        FROM
            jsonb_array_elements_text($1) AS value
    )
    AND (
        status = 'Done'
        OR (
            status = 'Failed'
            AND attempts >= max_attempts
        )
        OR status = 'Killed'
    );
