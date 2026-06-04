-- Confine generate_ulid() to the apalis schema and drop its pgcrypto dependency.
--
-- The ULID's 10 random bytes now come from core gen_random_uuid() (pg_catalog,
-- available since PostgreSQL 13) instead of pgcrypto's gen_random_bytes(). This
-- means the function no longer needs the pgcrypto extension or a pinned
-- search_path, and nothing apalis creates from here on touches `public`.
--
-- pgcrypto is intentionally left untouched: apalis no longer uses it, but an
-- earlier apalis version installed it in `public` and it may be relied upon
-- elsewhere. If nothing else in your database needs it you can remove it with
-- `DROP EXTENSION pgcrypto;`.

CREATE OR REPLACE FUNCTION apalis.generate_ulid()
RETURNS TEXT
AS $$
DECLARE
  -- Crockford's Base32
  encoding   BYTEA = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  timestamp  BYTEA = E'\\000\\000\\000\\000\\000\\000';
  output     TEXT = '';

  unix_time  BIGINT;
  ulid       BYTEA;
  rand       BYTEA;
BEGIN
  -- 6 timestamp bytes
  unix_time = (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;
  timestamp = SET_BYTE(timestamp, 0, (unix_time >> 40)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 1, (unix_time >> 32)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 2, (unix_time >> 24)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 3, (unix_time >> 16)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 4, (unix_time >> 8)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 5, unix_time::BIT(8)::INTEGER);

  -- 10 entropy bytes, taken from a core gen_random_uuid(). Bytes 1-6 and 11-14
  -- of the UUID are fully random (we skip the version/variant bytes at offsets
  -- 7 and 9), preserving the full 80 bits of ULID entropy.
  rand = uuid_send(gen_random_uuid());
  ulid = timestamp || substr(rand, 1, 6) || substr(rand, 11, 4);

  -- Encode the timestamp
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 224) >> 5));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 1) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 1) & 7) << 2) | ((GET_BYTE(ulid, 2) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 2) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 2) & 1) << 4) | ((GET_BYTE(ulid, 3) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 3) & 15) << 1) | ((GET_BYTE(ulid, 4) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 4) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 4) & 3) << 3) | ((GET_BYTE(ulid, 5) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 5) & 31)));

  -- Encode the entropy
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 6) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 6) & 7) << 2) | ((GET_BYTE(ulid, 7) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 7) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 7) & 1) << 4) | ((GET_BYTE(ulid, 8) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 8) & 15) << 1) | ((GET_BYTE(ulid, 9) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 9) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 9) & 3) << 3) | ((GET_BYTE(ulid, 10) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 10) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 11) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 11) & 7) << 2) | ((GET_BYTE(ulid, 12) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 12) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 12) & 1) << 4) | ((GET_BYTE(ulid, 13) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 13) & 15) << 1) | ((GET_BYTE(ulid, 14) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 14) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 14) & 3) << 3) | ((GET_BYTE(ulid, 15) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 15) & 31)));

  RETURN output;
END
$$
LANGUAGE plpgsql
VOLATILE;

-- Repoint the (legacy) push_job helper at the confined function. push_job is not
-- used by the driver itself (tasks are inserted via queries/task/sink.sql); this
-- only updates its generate_ulid() reference and otherwise leaves it unchanged.
CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer,
    priority integer DEFAULT 0 :: integer
) RETURNS apalis.jobs AS $$

        DECLARE
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            apalis.generate_ulid() INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL,
                priority
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;

-- Remove the public copy now that nothing references it.
DROP FUNCTION IF EXISTS public.generate_ulid();
