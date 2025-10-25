INSERT INTO
    apalis.jobs (
        id,
        job_type,
        job,
        status,
        attempts,
        max_attempts,
        run_at,
        priority
    )
SELECT
    unnest($1::text[]) as id,
    $2::text as job_type,
    unnest($3::jsonb[]) as job,
    'Pending' as status,
    0 as attempts,
    unnest($4::integer []) as max_attempts,
    unnest($5::timestamptz []) as run_at,
    unnest($6::integer []) as priority
