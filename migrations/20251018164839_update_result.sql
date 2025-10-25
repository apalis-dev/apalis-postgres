ALTER TABLE apalis.jobs 
RENAME COLUMN last_error TO last_result;

ALTER TABLE apalis.jobs 
    ALTER COLUMN last_result
SET DATA TYPE jsonb
USING last_result::jsonb;
