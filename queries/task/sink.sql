INSERT INTO apalis.jobs (
    id,
    job_type,
    job,
    status,
    attempts,
    max_attempts,
    run_at,
    priority,
    metadata,
    idempotency_key
)
SELECT
    unnest($1::text[]) AS id,
    $2::text AS job_type,
    unnest($3::bytea[]) AS job,
    'Pending' AS status,
    0 AS attempts,
    unnest($4::integer[]) AS max_attempts,
    to_timestamp(unnest($5::bigint[])) AS run_at,
    unnest($6::integer[]) AS priority,
    unnest($7::hstore[]) AS metadata,
    unnest($8::text[]) AS idempotency_key
