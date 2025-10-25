ALTER TABLE
    apalis.jobs
ALTER COLUMN
    job TYPE bytea USING convert_to(job::text, 'UTF8');
