CREATE EXTENSION IF NOT EXISTS hstore;

ALTER TABLE
    apalis.jobs
ADD
    COLUMN metadata_hstore hstore;

UPDATE
    apalis.jobs
SET
    metadata_hstore = (
        SELECT
            hstore(array_agg(key), array_agg(value))
        FROM
            jsonb_each_text(metadata)
    );

ALTER TABLE
    apalis.jobs DROP COLUMN metadata;

ALTER TABLE
    apalis.jobs RENAME COLUMN metadata_hstore TO metadata;
