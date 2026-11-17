\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.etl_batch (
    batch_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name varchar(100) NOT NULL,
    source_name varchar(100) NOT NULL,
    run_type varchar(20) NOT NULL,
    parent_batch_id bigint NULL REFERENCES control.etl_batch(batch_id),
    triggered_by varchar(100) NOT NULL DEFAULT 'manual',
    worker_id varchar(150) NULL,

    lset timestamptz NOT NULL,
    cet timestamptz NOT NULL,

    status varchar(24) NOT NULL DEFAULT 'CREATED',
    discovery_status varchar(16) NOT NULL DEFAULT 'PENDING',
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ended_at timestamptz NULL,

    catalog_expected_count integer NOT NULL DEFAULT 0,
    catalog_checked_count integer NOT NULL DEFAULT 0,
    files_selected integer NOT NULL DEFAULT 0,
    files_discovered integer NOT NULL DEFAULT 0,
    files_validated integer NOT NULL DEFAULT 0,

    rows_raw bigint NOT NULL DEFAULT 0,
    rows_canonical bigint NOT NULL DEFAULT 0,
    rows_technical_quarantined bigint NOT NULL DEFAULT 0,
    rows_business_quarantined bigint NOT NULL DEFAULT 0,
    rows_duplicate_quarantined bigint NOT NULL DEFAULT 0,
    rows_fact_loaded bigint NOT NULL DEFAULT 0,
    rows_amount_reconciliation_eligible bigint NOT NULL DEFAULT 0,

    error_code varchar(100) NULL,
    error_message text NULL,

    CONSTRAINT ck_etl_batch_run_type
        CHECK (run_type IN ('BACKFILL', 'INCREMENTAL', 'RETRY', 'TEST')),
    CONSTRAINT ck_etl_batch_status
        CHECK (status IN (
            'CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING',
            'RECONCILING', 'STALE', 'SUCCESS', 'FAILED', 'CANCELLED'
        )),
    CONSTRAINT ck_etl_batch_discovery_status
        CHECK (discovery_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT ck_etl_batch_window CHECK (lset <= cet),
    CONSTRAINT ck_etl_batch_retry_parent
        CHECK (run_type <> 'RETRY' OR parent_batch_id IS NOT NULL),
    CONSTRAINT ck_etl_batch_catalog_counts CHECK (
        catalog_expected_count >= 0
        AND catalog_checked_count >= 0
        AND files_selected >= 0
        AND files_discovered >= 0
        AND files_validated >= 0
        AND catalog_checked_count <= catalog_expected_count
        AND files_selected <= catalog_checked_count
    ),
    CONSTRAINT ck_etl_batch_row_counts CHECK (
        rows_raw >= 0
        AND rows_canonical >= 0
        AND rows_technical_quarantined >= 0
        AND rows_business_quarantined >= 0
        AND rows_duplicate_quarantined >= 0
        AND rows_fact_loaded >= 0
        AND rows_amount_reconciliation_eligible >= 0
    ),
    CONSTRAINT ck_etl_batch_end_time CHECK (
        ended_at IS NULL OR ended_at >= started_at
    ),
    CONSTRAINT ck_etl_batch_terminal_time CHECK (
        (status IN ('SUCCESS', 'FAILED', 'CANCELLED') AND ended_at IS NOT NULL)
        OR
        (status NOT IN ('SUCCESS', 'FAILED', 'CANCELLED') AND ended_at IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS control.etl_watermark (
    pipeline_name varchar(100) NOT NULL,
    source_name varchar(100) NOT NULL,
    last_successful_extraction_time timestamptz NOT NULL,
    last_successful_batch_id bigint NULL REFERENCES control.etl_batch(batch_id),
    watermark_version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_etl_watermark PRIMARY KEY (pipeline_name, source_name),
    CONSTRAINT ck_etl_watermark_version CHECK (watermark_version >= 0),
    CONSTRAINT ck_etl_watermark_updated CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS control.etl_file (
    etl_file_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES control.etl_batch(batch_id),
    source_file varchar(255) NOT NULL,
    source_url text NULL,
    source_year_month char(7) NOT NULL,
    source_last_modified timestamptz NULL,

    content_length bigint NULL,
    source_file_sha256 char(64) NULL,
    schema_fingerprint char(64) NULL,
    schema_version varchar(20) NULL,

    file_status varchar(40) NOT NULL DEFAULT 'DISCOVERED',
    approval_status varchar(16) NOT NULL DEFAULT 'NOT_REQUIRED',
    previous_sha256 char(64) NULL,
    approved_by varchar(100) NULL,
    approved_at timestamptz NULL,
    approval_reason text NULL,

    rows_read bigint NOT NULL DEFAULT 0,
    rows_staged bigint NOT NULL DEFAULT 0,
    rows_rejected bigint NOT NULL DEFAULT 0,

    discovered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    download_started_at timestamptz NULL,
    validated_at timestamptz NULL,
    staged_at timestamptz NULL,
    error_message text NULL,

    CONSTRAINT uq_etl_file_batch_source UNIQUE (batch_id, source_file),
    CONSTRAINT ck_etl_file_year_month
        CHECK (source_year_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT ck_etl_file_status CHECK (file_status IN (
        'DISCOVERED', 'DOWNLOADING', 'VALIDATED', 'STAGED',
        'SKIPPED_ALREADY_PROCESSED', 'SOURCE_VERSION_CHANGED',
        'APPROVED_SOURCE_VERSION', 'FAILED'
    )),
    CONSTRAINT ck_etl_file_approval_status
        CHECK (approval_status IN ('NOT_REQUIRED', 'PENDING', 'APPROVED', 'REJECTED')),
    CONSTRAINT ck_etl_file_content_length
        CHECK (content_length IS NULL OR content_length > 0),
    CONSTRAINT ck_etl_file_sha256 CHECK (
        source_file_sha256 IS NULL
        OR source_file_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_etl_file_schema_fingerprint CHECK (
        schema_fingerprint IS NULL
        OR schema_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_etl_file_previous_sha256 CHECK (
        previous_sha256 IS NULL
        OR previous_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_etl_file_validated_identity CHECK (
        file_status NOT IN (
            'VALIDATED', 'STAGED', 'SKIPPED_ALREADY_PROCESSED',
            'SOURCE_VERSION_CHANGED', 'APPROVED_SOURCE_VERSION'
        )
        OR (
            source_file_sha256 IS NOT NULL
            AND content_length IS NOT NULL
            AND schema_fingerprint IS NOT NULL
            AND schema_version IS NOT NULL
        )
    ),
    CONSTRAINT ck_etl_file_version_change CHECK (
        file_status <> 'SOURCE_VERSION_CHANGED'
        OR (
            previous_sha256 IS NOT NULL
            AND approval_status = 'PENDING'
        )
    ),
    CONSTRAINT ck_etl_file_approval_fields CHECK (
        approval_status <> 'APPROVED'
        OR (
            approved_by IS NOT NULL
            AND approved_at IS NOT NULL
            AND approval_reason IS NOT NULL
        )
    ),
    CONSTRAINT ck_etl_file_row_counts CHECK (
        rows_read >= 0 AND rows_staged >= 0 AND rows_rejected >= 0
    )
);

CREATE TABLE IF NOT EXISTS control.etl_step_run (
    step_run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES control.etl_batch(batch_id),
    step_name varchar(100) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'PENDING',
    is_required boolean NOT NULL DEFAULT true,
    started_at timestamptz NULL,
    ended_at timestamptz NULL,
    exit_code integer NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text NULL,

    CONSTRAINT uq_etl_step_run_batch_step UNIQUE (batch_id, step_name),
    CONSTRAINT ck_etl_step_run_status
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    CONSTRAINT ck_etl_step_run_times CHECK (
        ended_at IS NULL OR (started_at IS NOT NULL AND ended_at >= started_at)
    ),
    CONSTRAINT ck_etl_step_run_terminal CHECK (
        (status IN ('SUCCESS', 'FAILED', 'SKIPPED') AND ended_at IS NOT NULL)
        OR
        (status IN ('PENDING', 'RUNNING') AND ended_at IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS control.etl_log (
    log_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id bigint NULL REFERENCES control.etl_batch(batch_id),
    etl_file_id bigint NULL REFERENCES control.etl_file(etl_file_id),
    step_run_id bigint NULL REFERENCES control.etl_step_run(step_run_id),
    pipeline_name varchar(100) NOT NULL,
    block_name varchar(150) NULL,
    log_level varchar(10) NOT NULL,
    event_code varchar(100) NULL,
    message text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    logged_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT ck_etl_log_level
        CHECK (log_level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR'))
);

COMMENT ON TABLE control.etl_batch IS
    'One row per Mage master-pipeline execution, including extraction window, discovery completeness and reconciliation counters.';
COMMENT ON TABLE control.etl_watermark IS
    'Last successful extraction time per pipeline/source. Updated only by control.commit_batch_success.';
COMMENT ON TABLE control.etl_file IS
    'One source-file attempt per batch, from discovery through validation, staging, skip or source-version review.';
COMMENT ON TABLE control.etl_step_run IS
    'Machine-readable execution result for each required pipeline step.';
COMMENT ON TABLE control.etl_log IS
    'Append-only operational events; secrets and unnecessary raw records must never be stored.';
