\set ON_ERROR_STOP on

-- Additive recovery/cleanup metadata. This migration never removes legacy data.
ALTER TABLE control.etl_backfill_file
    ADD COLUMN IF NOT EXISTS cleanup_status varchar(16) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS cleaned_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS cleanup_error text NULL;

ALTER TABLE control.etl_backfill_run
    ADD COLUMN IF NOT EXISTS dds_validation_status varchar(16) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS olap_bi_validation_status varchar(16) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS validation_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_etl_backfill_file_cleanup_status') THEN
        ALTER TABLE control.etl_backfill_file ADD CONSTRAINT ck_etl_backfill_file_cleanup_status
            CHECK (cleanup_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_etl_backfill_run_dds_validation') THEN
        ALTER TABLE control.etl_backfill_run ADD CONSTRAINT ck_etl_backfill_run_dds_validation
            CHECK (dds_validation_status IN ('PENDING', 'SUCCESS', 'FAILED'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_etl_backfill_run_bi_validation') THEN
        ALTER TABLE control.etl_backfill_run ADD CONSTRAINT ck_etl_backfill_run_bi_validation
            CHECK (olap_bi_validation_status IN ('PENDING', 'SUCCESS', 'FAILED'));
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION control.resume_failed_downstream(p_batch_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch control.etl_batch%ROWTYPE;
    v_raw_count bigint;
    v_canonical_count bigint;
    v_foreign_count bigint;
    v_file control.etl_file%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
    SELECT * INTO v_batch FROM control.etl_batch
    WHERE batch_id = p_batch_id FOR UPDATE;
    IF NOT FOUND OR NOT (
        v_batch.status IN ('STAGED', 'DBT_RUNNING', 'RECONCILING')
        OR (v_batch.status = 'FAILED' AND v_batch.error_code IS NOT DISTINCT FROM 'DOWNSTREAM_FAILED')
    ) THEN
        RAISE EXCEPTION 'Batch % is not resumable from downstream failure', p_batch_id;
    END IF;
    SELECT * INTO v_file FROM control.etl_file WHERE batch_id = p_batch_id;
    IF NOT FOUND OR v_file.file_status <> 'STAGED' OR v_file.file_version_id IS NULL
       OR (SELECT count(*) FROM control.etl_file WHERE batch_id = p_batch_id) <> 1 THEN
        RAISE EXCEPTION 'Batch % has no immutable STAGED file version', p_batch_id;
    END IF;
    SELECT count(*) INTO v_raw_count FROM stage.yellow_taxi_raw
    WHERE batch_id = p_batch_id AND file_version_id = v_file.file_version_id
      AND source_file_sha256 = v_file.source_file_sha256;
    SELECT count(*) INTO v_canonical_count FROM stage.yellow_taxi_canonical
    WHERE batch_id = p_batch_id AND file_version_id = v_file.file_version_id
      AND source_file_sha256 = v_file.source_file_sha256;
    SELECT (SELECT count(*) FROM stage.yellow_taxi_raw)
         + (SELECT count(*) FROM stage.yellow_taxi_canonical) - v_raw_count - v_canonical_count
    INTO v_foreign_count;
    IF v_raw_count = 0 OR v_raw_count <> v_file.rows_read
       OR v_canonical_count <> v_file.rows_staged
       OR v_raw_count <> v_canonical_count + v_file.rows_rejected
       OR v_foreign_count <> 0 THEN
        RAISE EXCEPTION 'Batch % staging is absent, inconsistent, or mixed', p_batch_id;
    END IF;

    UPDATE control.etl_step_run
    SET status = 'PENDING', started_at = NULL, ended_at = NULL, exit_code = NULL,
        details = '{}'::jsonb, error_message = NULL
    WHERE batch_id = p_batch_id
      AND step_name IN ('DBT_BUILD_DDS', 'DBT_TEST', 'RECONCILIATION');
    -- An in-flight legacy batch may still have an OLAP step. The DDS runner
    -- never writes a fake OLAP SUCCESS and does not use it as a DDS gate.
    UPDATE control.etl_step_run SET is_required = false
    WHERE batch_id = p_batch_id AND step_name = 'DBT_BUILD_OLAP';
    UPDATE control.etl_batch
    SET status = 'DBT_RUNNING', ended_at = NULL, heartbeat_at = clock_timestamp(),
        error_code = NULL, error_message = NULL
    WHERE batch_id = p_batch_id;
    PERFORM control.log_event(v_batch.pipeline_name, 'WARNING',
        'Resuming downstream from preserved single-batch Staging', p_batch_id,
        p_event_code => 'DOWNSTREAM_RESUMED');
END
$$;

CREATE OR REPLACE FUNCTION control.authorize_file_replay(
    p_etl_file_id bigint,
    p_expected_file_version_id bigint,
    p_requested_by text,
    p_reason text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_file control.etl_file%ROWTYPE;
    v_pipeline text;
BEGIN
    IF nullif(btrim(p_requested_by), '') IS NULL OR nullif(btrim(p_reason), '') IS NULL THEN
        RAISE EXCEPTION 'Replay actor and reason are required';
    END IF;
    SELECT f.* INTO v_file FROM control.etl_file f
    WHERE f.etl_file_id = p_etl_file_id FOR UPDATE;
    IF v_file.etl_file_id IS NULL OR v_file.file_status <> 'SKIPPED_ALREADY_PROCESSED'
       OR v_file.file_version_id IS DISTINCT FROM p_expected_file_version_id THEN
        RAISE EXCEPTION 'Replay attempt is not a verified successful file version';
    END IF;
    SELECT b.pipeline_name INTO v_pipeline FROM control.etl_batch b WHERE b.batch_id = v_file.batch_id;
    IF NOT EXISTS (
        SELECT 1 FROM control.source_file_version v
        WHERE v.file_version_id = p_expected_file_version_id
          AND v.source_file = v_file.source_file
          AND v.source_file_sha256 = v_file.source_file_sha256
    ) OR NOT EXISTS (
        SELECT 1 FROM control.file_reconciliation_history h
        JOIN control.etl_batch b ON b.batch_id = h.successful_batch_id
        WHERE h.file_version_id = p_expected_file_version_id AND b.status = 'SUCCESS'
    ) THEN
        RAISE EXCEPTION 'Replay identity or successful reconciliation history is missing';
    END IF;
    UPDATE control.etl_file
    SET file_status = 'VALIDATED', error_message = NULL
    WHERE etl_file_id = p_etl_file_id;
    PERFORM control.log_event(
        v_pipeline, 'WARNING', 'Authorized immutable Parquet replay', v_file.batch_id,
        p_etl_file_id => p_etl_file_id, p_event_code => 'FILE_REPLAY_AUTHORIZED',
        p_details => jsonb_build_object('file_version_id', p_expected_file_version_id,
            'requested_by', p_requested_by, 'reason', p_reason)
    );
END
$$;

CREATE OR REPLACE VIEW control.v_backfill_global_reconciliation AS
SELECT
    r.backfill_run_id,
    count(*) FILTER (WHERE f.status = 'SUCCESS' AND h.file_version_id IS NOT NULL) AS validated_file_count,
    count(*) FILTER (WHERE f.status = 'SUCCESS' AND b.status = 'SUCCESS') AS successful_child_count,
    count(*) FILTER (WHERE f.cleanup_status = 'SUCCESS') AS cleanup_success_count,
    count(*) FILTER (WHERE s.required_failed) AS required_step_failure_count,
    coalesce(sum(h.source_rows), 0) AS source_rows,
    coalesce(sum(h.canonical_rows), 0) AS canonical_rows,
    coalesce(sum(h.technical_rows), 0) AS technical_rows,
    coalesce(sum(h.fact_rows), 0) AS fact_rows,
    coalesce(sum(h.business_rows), 0) AS business_rows,
    coalesce(sum(h.duplicate_rows), 0) AS duplicate_rows,
    coalesce(bool_and(h.source_row_delta = 0 AND h.trusted_row_delta = 0), false) AS is_balanced
FROM control.etl_backfill_run r
JOIN control.etl_backfill_file f USING (backfill_run_id)
LEFT JOIN control.etl_batch b ON b.batch_id = f.batch_id
LEFT JOIN control.file_reconciliation_history h
  ON h.file_version_id = f.file_version_id AND h.successful_batch_id = f.batch_id
LEFT JOIN LATERAL (
    SELECT
      count(*) FILTER (WHERE step.is_required AND step.step_name IN
          ('DISCOVERY', 'FILE_VALIDATION', 'STAGING', 'DBT_BUILD_DDS', 'DBT_TEST', 'RECONCILIATION')) <> 6
      OR coalesce(bool_or(step.is_required AND step.status <> 'SUCCESS'), false)
      AS required_failed
    FROM control.etl_step_run step WHERE step.batch_id = f.batch_id
) s ON true
GROUP BY r.backfill_run_id;

CREATE OR REPLACE FUNCTION control.record_backfill_validation(
    p_backfill_run_id bigint,
    p_validation_kind text,
    p_status text,
    p_actor text,
    p_evidence jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_validation_kind NOT IN ('DDS', 'OLAP_BI') OR p_status NOT IN ('SUCCESS', 'FAILED')
       OR nullif(btrim(p_actor), '') IS NULL OR p_evidence IS NULL OR p_evidence = '{}'::jsonb THEN
        RAISE EXCEPTION 'Validation kind, status, actor and non-empty evidence are required';
    END IF;
    UPDATE control.etl_backfill_run
    SET dds_validation_status = CASE WHEN p_validation_kind = 'DDS' THEN p_status ELSE dds_validation_status END,
        olap_bi_validation_status = CASE WHEN p_validation_kind = 'OLAP_BI' THEN p_status ELSE olap_bi_validation_status END,
        validation_evidence = validation_evidence || jsonb_build_object(
            lower(p_validation_kind), jsonb_build_object(
                'status', p_status, 'actor', p_actor,
                'recorded_at', clock_timestamp(), 'evidence', p_evidence
            )
        )
    WHERE backfill_run_id = p_backfill_run_id AND expected_file_count > 0;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Backfill run % does not exist', p_backfill_run_id;
    END IF;
END
$$;
