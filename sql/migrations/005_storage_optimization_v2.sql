\set ON_ERROR_STOP on

-- Online-compatible migration for an existing Phase 2 database. The base DDL
-- already contains these objects for a clean rebuild.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS control.source_file_version (
    file_version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file varchar(255) NOT NULL,
    source_url text NULL,
    source_year_month char(7) NOT NULL,
    source_file_sha256 char(64) NOT NULL,
    schema_fingerprint char(64) NOT NULL,
    schema_version varchar(20) NOT NULL,
    source_last_modified timestamptz NULL,
    content_length bigint NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_source_file_version_identity UNIQUE (source_file, source_file_sha256),
    CONSTRAINT ck_source_file_version_year_month CHECK (
        source_year_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
    ),
    CONSTRAINT ck_source_file_version_sha256 CHECK (
        source_file_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_source_file_version_schema_fingerprint CHECK (
        schema_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_source_file_version_content_length CHECK (content_length > 0)
);

ALTER TABLE control.etl_file
    ADD COLUMN IF NOT EXISTS file_version_id bigint NULL;

INSERT INTO control.source_file_version (
    source_file, source_url, source_year_month, source_file_sha256,
    schema_fingerprint, schema_version, source_last_modified, content_length,
    verified_at
)
SELECT DISTINCT ON (source_file, source_file_sha256)
    source_file, source_url, source_year_month, source_file_sha256,
    schema_fingerprint, schema_version, source_last_modified, content_length,
    coalesce(validated_at, clock_timestamp())
FROM control.etl_file
WHERE source_file_sha256 IS NOT NULL
  AND schema_fingerprint IS NOT NULL
  AND schema_version IS NOT NULL
  AND content_length IS NOT NULL
ORDER BY source_file, source_file_sha256, etl_file_id DESC
ON CONFLICT (source_file, source_file_sha256) DO NOTHING;

UPDATE control.etl_file f
SET file_version_id = v.file_version_id
FROM control.source_file_version v
WHERE f.file_version_id IS NULL
  AND v.source_file = f.source_file
  AND v.source_file_sha256 = f.source_file_sha256;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_etl_file_file_version'
          AND conrelid = 'control.etl_file'::regclass
    ) THEN
        ALTER TABLE control.etl_file
            ADD CONSTRAINT fk_etl_file_file_version
            FOREIGN KEY (file_version_id)
            REFERENCES control.source_file_version(file_version_id);
    END IF;
END;
$$;

ALTER TABLE stage.yellow_taxi_raw ADD COLUMN IF NOT EXISTS file_version_id bigint NULL;
ALTER TABLE stage.yellow_taxi_canonical ADD COLUMN IF NOT EXISTS file_version_id bigint NULL;
ALTER TABLE dq.technical_quarantine ADD COLUMN IF NOT EXISTS file_version_id bigint NULL;

-- Do not rewrite already-staged multi-million-row pilot tables. dbt derives a
-- missing legacy file_version_id through control.source_file_version. All new
-- Mage rows carry the physical ID directly.

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT * FROM (VALUES
            ('stage.yellow_taxi_raw', 'fk_yellow_taxi_raw_file_version'),
            ('stage.yellow_taxi_canonical', 'fk_yellow_taxi_canonical_file_version'),
            ('dq.technical_quarantine', 'fk_technical_quarantine_file_version')
        ) AS x(table_name, constraint_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = item.constraint_name
              AND conrelid = item.table_name::regclass
        ) THEN
            EXECUTE format(
                'ALTER TABLE %s ADD CONSTRAINT %I FOREIGN KEY (file_version_id) '
                'REFERENCES control.source_file_version(file_version_id)',
                item.table_name, item.constraint_name
            );
        END IF;
    END LOOP;
END;
$$;

-- Existing legacy rows may remain NULL until the successful pilot cleanup.
-- The clean-build DDL declares these columns NOT NULL.

CREATE TABLE IF NOT EXISTS control.file_reconciliation_history (
    file_version_id bigint PRIMARY KEY REFERENCES control.source_file_version(file_version_id),
    successful_batch_id bigint NOT NULL REFERENCES control.etl_batch(batch_id),
    source_rows bigint NOT NULL,
    canonical_rows bigint NOT NULL,
    technical_rows bigint NOT NULL,
    fact_rows bigint NOT NULL,
    business_rows bigint NOT NULL,
    duplicate_rows bigint NOT NULL,
    source_amount_eligible_rows bigint NOT NULL,
    canonical_amount_eligible_rows bigint NOT NULL,
    technical_amount_eligible_rows bigint NOT NULL,
    fact_amount_eligible_rows bigint NOT NULL,
    business_amount_eligible_rows bigint NOT NULL,
    duplicate_amount_eligible_rows bigint NOT NULL,
    source_amount numeric NOT NULL,
    canonical_amount numeric NOT NULL,
    technical_amount numeric NOT NULL,
    fact_amount numeric NOT NULL,
    business_amount numeric NOT NULL,
    duplicate_amount numeric NOT NULL,
    source_row_delta bigint NOT NULL,
    trusted_row_delta bigint NOT NULL,
    source_amount_delta numeric NOT NULL,
    trusted_amount_delta numeric NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_file_reconciliation_nonnegative CHECK (
        source_rows >= 0 AND canonical_rows >= 0 AND technical_rows >= 0
        AND fact_rows >= 0 AND business_rows >= 0 AND duplicate_rows >= 0
    ),
    CONSTRAINT ck_file_reconciliation_balanced CHECK (
        source_row_delta = 0 AND trusted_row_delta = 0
    )
);

CREATE TABLE IF NOT EXISTS control.etl_backfill_run (
    backfill_run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name varchar(100) NOT NULL,
    source_name varchar(100) NOT NULL,
    source_month_from char(7) NOT NULL,
    source_month_to char(7) NOT NULL,
    expected_file_count integer NOT NULL,
    completed_file_count integer NOT NULL DEFAULT 0,
    status varchar(16) NOT NULL DEFAULT 'RUNNING',
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ended_at timestamptz NULL,
    error_message text NULL,
    CONSTRAINT ck_backfill_run_months CHECK (
        source_month_from ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
        AND source_month_to ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
        AND source_month_from <= source_month_to
    ),
    CONSTRAINT ck_backfill_run_counts CHECK (
        expected_file_count > 0 AND completed_file_count >= 0
        AND completed_file_count <= expected_file_count
    ),
    CONSTRAINT ck_backfill_run_status CHECK (
        status IN ('RUNNING', 'SUCCESS', 'FAILED')
    )
);

CREATE TABLE IF NOT EXISTS control.etl_backfill_file (
    backfill_run_id bigint NOT NULL REFERENCES control.etl_backfill_run(backfill_run_id),
    source_year_month char(7) NOT NULL,
    file_version_id bigint NULL REFERENCES control.source_file_version(file_version_id),
    batch_id bigint NULL REFERENCES control.etl_batch(batch_id),
    status varchar(16) NOT NULL DEFAULT 'PENDING',
    completed_at timestamptz NULL,
    error_message text NULL,
    CONSTRAINT pk_etl_backfill_file PRIMARY KEY (backfill_run_id, source_year_month),
    CONSTRAINT ck_etl_backfill_file_month CHECK (
        source_year_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
    ),
    CONSTRAINT ck_etl_backfill_file_status CHECK (
        status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')
    )
);

CREATE TABLE IF NOT EXISTS audit.trip_fingerprint_history (
    source_record_key bytea PRIMARY KEY,
    file_version_id bigint NOT NULL REFERENCES control.source_file_version(file_version_id),
    source_row_number bigint NOT NULL,
    source_file_sha256 bytea NOT NULL,
    raw_exact_fingerprint bytea NOT NULL,
    candidate_trip_fingerprint bytea NOT NULL,
    primary_disposition varchar(24) NOT NULL,
    last_batch_id bigint NOT NULL REFERENCES control.etl_batch(batch_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_trip_fingerprint_source_key_bytes CHECK (octet_length(source_record_key) = 32),
    CONSTRAINT ck_trip_fingerprint_file_sha_bytes CHECK (octet_length(source_file_sha256) = 32),
    CONSTRAINT ck_trip_fingerprint_raw_bytes CHECK (octet_length(raw_exact_fingerprint) = 32),
    CONSTRAINT ck_trip_fingerprint_candidate_bytes CHECK (octet_length(candidate_trip_fingerprint) = 32),
    CONSTRAINT ck_trip_fingerprint_row_number CHECK (source_row_number > 0),
    CONSTRAINT ck_trip_fingerprint_disposition CHECK (
        primary_disposition IN ('ACCEPTED', 'WARNING', 'BUSINESS_QUARANTINE', 'DUPLICATE')
    )
);

CREATE TABLE IF NOT EXISTS audit.cross_file_fingerprint_match (
    fingerprint_type varchar(16) NOT NULL,
    fingerprint bytea NOT NULL,
    first_file_version_id bigint NOT NULL REFERENCES control.source_file_version(file_version_id),
    matched_file_version_id bigint NOT NULL REFERENCES control.source_file_version(file_version_id),
    detected_batch_id bigint NOT NULL REFERENCES control.etl_batch(batch_id),
    detected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_cross_file_fingerprint_match PRIMARY KEY (
        fingerprint_type, fingerprint, first_file_version_id, matched_file_version_id
    ),
    CONSTRAINT ck_cross_file_fingerprint_type CHECK (
        fingerprint_type IN ('RAW_EXACT', 'CANDIDATE')
    ),
    CONSTRAINT ck_cross_file_fingerprint_bytes CHECK (octet_length(fingerprint) = 32),
    CONSTRAINT ck_cross_file_distinct_versions CHECK (
        first_file_version_id <> matched_file_version_id
    )
);

CREATE INDEX IF NOT EXISTS ix_source_file_version_month
    ON control.source_file_version (source_year_month, file_version_id);
CREATE INDEX IF NOT EXISTS ix_trip_fingerprint_history_raw
    ON audit.trip_fingerprint_history (raw_exact_fingerprint, file_version_id);
CREATE INDEX IF NOT EXISTS ix_trip_fingerprint_history_candidate
    ON audit.trip_fingerprint_history (candidate_trip_fingerprint, file_version_id);
CREATE INDEX IF NOT EXISTS ix_trip_fingerprint_history_file
    ON audit.trip_fingerprint_history (file_version_id, source_row_number);

-- These indexes duplicate transient one-file Staging access patterns and were
-- the largest avoidable indexes in the 2024-05 baseline. The unique source-key
-- constraints and file-SHA indexes remain.
DROP INDEX IF EXISTS stage.ix_yellow_taxi_raw_batch;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_raw_year_month;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_raw_file_row;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_canonical_batch;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_canonical_year_month;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_canonical_pickup;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_canonical_pu_location;
DROP INDEX IF EXISTS stage.ix_yellow_taxi_canonical_do_location;

\ir ../control/003_control_functions.sql
