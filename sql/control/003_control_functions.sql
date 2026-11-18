\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION control.log_event(
    p_pipeline_name varchar,
    p_log_level varchar,
    p_message text,
    p_batch_id bigint DEFAULT NULL,
    p_etl_file_id bigint DEFAULT NULL,
    p_step_run_id bigint DEFAULT NULL,
    p_block_name varchar DEFAULT NULL,
    p_event_code varchar DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id bigint;
BEGIN
    INSERT INTO control.etl_log (
        batch_id, etl_file_id, step_run_id, pipeline_name,
        block_name, log_level, event_code, message, details
    )
    VALUES (
        p_batch_id, p_etl_file_id, p_step_run_id, p_pipeline_name,
        p_block_name, p_log_level, p_event_code, p_message,
        COALESCE(p_details, '{}'::jsonb)
    )
    RETURNING log_id INTO v_log_id;

    RETURN v_log_id;
END;
$$;

CREATE OR REPLACE FUNCTION control.start_batch(
    p_pipeline_name varchar,
    p_source_name varchar,
    p_run_type varchar,
    p_initial_lset timestamptz,
    p_parent_batch_id bigint DEFAULT NULL,
    p_triggered_by varchar DEFAULT 'manual',
    p_worker_id varchar DEFAULT NULL
)
RETURNS TABLE (batch_id bigint, lset timestamptz, cet timestamptz)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id bigint;
    v_lset timestamptz;
    v_cet timestamptz;
    v_parent_status varchar(24);
BEGIN
    IF p_run_type NOT IN ('BACKFILL', 'INCREMENTAL', 'RETRY', 'TEST') THEN
        RAISE EXCEPTION 'Unsupported run_type: %', p_run_type;
    END IF;

    IF p_run_type = 'RETRY' THEN
        IF p_parent_batch_id IS NULL THEN
            RAISE EXCEPTION 'RETRY requires parent_batch_id';
        END IF;

        SELECT b.status
        INTO v_parent_status
        FROM control.etl_batch b
        WHERE b.batch_id = p_parent_batch_id;

        IF v_parent_status IS NULL THEN
            RAISE EXCEPTION 'Parent batch % does not exist', p_parent_batch_id;
        END IF;

        IF v_parent_status NOT IN ('FAILED', 'CANCELLED') THEN
            RAISE EXCEPTION 'Parent batch % must be FAILED or CANCELLED, found %',
                p_parent_batch_id, v_parent_status;
        END IF;
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_pipeline_name || ':' || p_source_name, 0)
    );

    IF EXISTS (
        SELECT 1
        FROM control.etl_batch b
        WHERE b.pipeline_name = p_pipeline_name
          AND b.source_name = p_source_name
          AND b.status IN (
              'CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING',
              'RECONCILING', 'STALE'
          )
    ) THEN
        RAISE EXCEPTION 'Active or STALE batch already exists for %.%',
            p_pipeline_name, p_source_name;
    END IF;

    INSERT INTO control.etl_watermark (
        pipeline_name,
        source_name,
        last_successful_extraction_time
    )
    VALUES (
        p_pipeline_name,
        p_source_name,
        p_initial_lset
    )
    ON CONFLICT (pipeline_name, source_name) DO NOTHING;

    SELECT w.last_successful_extraction_time
    INTO v_lset
    FROM control.etl_watermark w
    WHERE w.pipeline_name = p_pipeline_name
      AND w.source_name = p_source_name
    FOR UPDATE;

    v_cet := clock_timestamp();

    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, parent_batch_id,
        triggered_by, worker_id, lset, cet, status,
        discovery_status, heartbeat_at, started_at
    )
    VALUES (
        p_pipeline_name, p_source_name, p_run_type, p_parent_batch_id,
        p_triggered_by, p_worker_id, v_lset, v_cet, 'RUNNING',
        'PENDING', v_cet, v_cet
    )
    RETURNING control.etl_batch.batch_id INTO v_batch_id;

    INSERT INTO control.etl_step_run (batch_id, step_name, status, is_required)
    SELECT v_batch_id, step_name, 'PENDING', true
    FROM (VALUES
        ('DISCOVERY'),
        ('FILE_VALIDATION'),
        ('STAGING'),
        ('DBT_BUILD_DDS'),
        ('DBT_TEST'),
        ('RECONCILIATION'),
        ('DBT_BUILD_OLAP')
    ) AS required_steps(step_name);

    PERFORM control.log_event(
        p_pipeline_name,
        'INFO',
        'Batch started and extraction window captured.',
        v_batch_id,
        p_event_code => 'BATCH_STARTED',
        p_details => jsonb_build_object(
            'run_type', p_run_type,
            'lset', v_lset,
            'cet', v_cet,
            'parent_batch_id', p_parent_batch_id
        )
    );

    RETURN QUERY SELECT v_batch_id, v_lset, v_cet;
END;
$$;

CREATE OR REPLACE FUNCTION control.touch_batch_heartbeat(
    p_batch_id bigint,
    p_worker_id varchar DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE control.etl_batch b
    SET heartbeat_at = clock_timestamp(),
        worker_id = COALESCE(p_worker_id, b.worker_id)
    WHERE b.batch_id = p_batch_id
      AND b.status IN ('CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING', 'RECONCILING');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % is not heartbeat-eligible', p_batch_id;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION control.mark_stale_batches(
    p_timeout interval DEFAULT interval '15 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_count integer;
BEGIN
    IF p_timeout <= interval '0 seconds' THEN
        RAISE EXCEPTION 'Timeout must be positive';
    END IF;

    UPDATE control.etl_batch b
    SET status = 'STALE',
        error_code = 'HEARTBEAT_TIMEOUT',
        error_message = format('Heartbeat exceeded timeout %s', p_timeout)
    WHERE b.status IN ('CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING', 'RECONCILING')
      AND b.heartbeat_at < clock_timestamp() - p_timeout;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION control.resolve_stale_batch(
    p_batch_id bigint,
    p_terminal_status varchar,
    p_reason text,
    p_resolved_by varchar
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_pipeline_name varchar(100);
BEGIN
    IF p_terminal_status NOT IN ('FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'STALE batch can only resolve to FAILED or CANCELLED';
    END IF;

    IF NULLIF(btrim(p_reason), '') IS NULL THEN
        RAISE EXCEPTION 'Resolution reason is required';
    END IF;

    UPDATE control.etl_batch b
    SET status = p_terminal_status,
        ended_at = clock_timestamp(),
        error_code = 'STALE_BATCH_RESOLVED',
        error_message = p_reason,
        heartbeat_at = clock_timestamp()
    WHERE b.batch_id = p_batch_id
      AND b.status = 'STALE'
    RETURNING b.pipeline_name INTO v_pipeline_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % is not STALE', p_batch_id;
    END IF;

    PERFORM control.log_event(
        v_pipeline_name,
        'WARNING',
        p_reason,
        p_batch_id,
        p_event_code => 'STALE_BATCH_RESOLVED',
        p_details => jsonb_build_object(
            'resolved_by', p_resolved_by,
            'terminal_status', p_terminal_status
        )
    );
END;
$$;

CREATE OR REPLACE FUNCTION control.transition_batch(
    p_batch_id bigint,
    p_new_status varchar
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_old_status varchar(24);
BEGIN
    SELECT b.status
    INTO v_old_status
    FROM control.etl_batch b
    WHERE b.batch_id = p_batch_id
    FOR UPDATE;

    IF v_old_status IS NULL THEN
        RAISE EXCEPTION 'Batch % does not exist', p_batch_id;
    END IF;

    IF NOT (
        (v_old_status = 'CREATED' AND p_new_status = 'RUNNING')
        OR (v_old_status = 'RUNNING' AND p_new_status = 'STAGED')
        OR (v_old_status = 'STAGED' AND p_new_status = 'DBT_RUNNING')
        OR (v_old_status = 'DBT_RUNNING' AND p_new_status = 'RECONCILING')
    ) THEN
        RAISE EXCEPTION 'Invalid batch transition: % -> %', v_old_status, p_new_status;
    END IF;

    UPDATE control.etl_batch
    SET status = p_new_status,
        heartbeat_at = clock_timestamp()
    WHERE control.etl_batch.batch_id = p_batch_id;
END;
$$;

CREATE OR REPLACE FUNCTION control.update_discovery_result(
    p_batch_id bigint,
    p_discovery_status varchar,
    p_catalog_expected_count integer,
    p_catalog_checked_count integer,
    p_files_selected integer
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_discovery_status NOT IN ('RUNNING', 'SUCCESS', 'FAILED') THEN
        RAISE EXCEPTION 'Unsupported discovery status: %', p_discovery_status;
    END IF;

    IF p_catalog_expected_count < 0
       OR p_catalog_checked_count < 0
       OR p_files_selected < 0
       OR p_catalog_checked_count > p_catalog_expected_count
       OR p_files_selected > p_catalog_checked_count THEN
        RAISE EXCEPTION 'Invalid discovery counters';
    END IF;

    IF p_discovery_status = 'SUCCESS'
       AND p_catalog_checked_count <> p_catalog_expected_count THEN
        RAISE EXCEPTION 'Discovery cannot succeed: checked % of % catalog entries',
            p_catalog_checked_count, p_catalog_expected_count;
    END IF;

    UPDATE control.etl_batch b
    SET discovery_status = p_discovery_status,
        catalog_expected_count = p_catalog_expected_count,
        catalog_checked_count = p_catalog_checked_count,
        files_selected = p_files_selected,
        heartbeat_at = clock_timestamp()
    WHERE b.batch_id = p_batch_id
      AND b.status IN ('RUNNING', 'STAGED');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % is not discovery-updatable', p_batch_id;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION control.update_batch_counters(
    p_batch_id bigint,
    p_rows_raw bigint,
    p_rows_canonical bigint,
    p_rows_technical_quarantined bigint,
    p_rows_business_quarantined bigint,
    p_rows_duplicate_quarantined bigint,
    p_rows_fact_loaded bigint,
    p_rows_amount_reconciliation_eligible bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF LEAST(
        p_rows_raw,
        p_rows_canonical,
        p_rows_technical_quarantined,
        p_rows_business_quarantined,
        p_rows_duplicate_quarantined,
        p_rows_fact_loaded,
        p_rows_amount_reconciliation_eligible
    ) < 0 THEN
        RAISE EXCEPTION 'Batch counters cannot be negative';
    END IF;

    UPDATE control.etl_batch b
    SET rows_raw = p_rows_raw,
        rows_canonical = p_rows_canonical,
        rows_technical_quarantined = p_rows_technical_quarantined,
        rows_business_quarantined = p_rows_business_quarantined,
        rows_duplicate_quarantined = p_rows_duplicate_quarantined,
        rows_fact_loaded = p_rows_fact_loaded,
        rows_amount_reconciliation_eligible = p_rows_amount_reconciliation_eligible,
        heartbeat_at = clock_timestamp()
    WHERE b.batch_id = p_batch_id
      AND b.status IN ('RUNNING', 'STAGED', 'DBT_RUNNING', 'RECONCILING');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % is not counter-updatable', p_batch_id;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION control.should_discover_file(
    p_batch_id bigint,
    p_source_file varchar,
    p_source_last_modified timestamptz,
    p_catalog_sha256 char(64) DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_type varchar(20);
    v_pipeline_name varchar(100);
    v_source_name varchar(100);
    v_lset timestamptz;
    v_cet timestamptz;
    v_has_successful_filename boolean;
    v_has_successful_identity boolean;
BEGIN
    SELECT b.run_type, b.pipeline_name, b.source_name, b.lset, b.cet
    INTO v_run_type, v_pipeline_name, v_source_name, v_lset, v_cet
    FROM control.etl_batch b
    WHERE b.batch_id = p_batch_id;

    IF v_run_type IS NULL THEN
        RAISE EXCEPTION 'Batch % does not exist', p_batch_id;
    END IF;

    IF v_run_type IN ('BACKFILL', 'RETRY', 'TEST') THEN
        RETURN true;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM control.etl_file f
        JOIN control.etl_batch b ON b.batch_id = f.batch_id
        WHERE f.source_file = p_source_file
          AND b.pipeline_name = v_pipeline_name
          AND b.source_name = v_source_name
          AND f.file_status = 'STAGED'
          AND b.status = 'SUCCESS'
    )
    INTO v_has_successful_filename;

    SELECT EXISTS (
        SELECT 1
        FROM control.etl_file f
        JOIN control.etl_batch b ON b.batch_id = f.batch_id
        WHERE f.source_file = p_source_file
          AND b.pipeline_name = v_pipeline_name
          AND b.source_name = v_source_name
          AND f.source_file_sha256 = p_catalog_sha256
          AND f.file_status = 'STAGED'
          AND b.status = 'SUCCESS'
    )
    INTO v_has_successful_identity;

    RETURN
        NOT v_has_successful_filename
        OR (
            p_source_last_modified IS NOT NULL
            AND v_lset < p_source_last_modified
            AND p_source_last_modified <= v_cet
        )
        OR (
            p_catalog_sha256 IS NOT NULL
            AND v_has_successful_filename
            AND NOT v_has_successful_identity
        );
END;
$$;

CREATE OR REPLACE FUNCTION control.discover_file(
    p_batch_id bigint,
    p_source_file varchar,
    p_source_year_month char(7),
    p_source_url text DEFAULT NULL,
    p_source_last_modified timestamptz DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_etl_file_id bigint;
BEGIN
    INSERT INTO control.etl_file (
        batch_id, source_file, source_url, source_year_month,
        source_last_modified, file_status
    )
    VALUES (
        p_batch_id, p_source_file, p_source_url, p_source_year_month,
        p_source_last_modified, 'DISCOVERED'
    )
    RETURNING etl_file_id INTO v_etl_file_id;

    UPDATE control.etl_batch
    SET files_discovered = files_discovered + 1,
        heartbeat_at = clock_timestamp()
    WHERE batch_id = p_batch_id;

    RETURN v_etl_file_id;
END;
$$;

CREATE OR REPLACE FUNCTION control.mark_file_downloading(
    p_etl_file_id bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE control.etl_file f
    SET file_status = 'DOWNLOADING',
        download_started_at = clock_timestamp()
    WHERE f.etl_file_id = p_etl_file_id
      AND f.file_status = 'DISCOVERED';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'File % is not DISCOVERED', p_etl_file_id;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION control.validate_file(
    p_etl_file_id bigint,
    p_content_length bigint,
    p_source_file_sha256 char(64),
    p_schema_fingerprint char(64),
    p_schema_version varchar
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_file control.etl_file%ROWTYPE;
    v_pipeline_name varchar(100);
    v_source_name varchar(100);
    v_previous_sha256 char(64);
    v_action text;
    v_approved boolean;
    v_failed_staged boolean;
    v_file_version_id bigint;
BEGIN
    SELECT *
    INTO v_file
    FROM control.etl_file f
    WHERE f.etl_file_id = p_etl_file_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'File % does not exist', p_etl_file_id;
    END IF;

    IF v_file.file_status NOT IN ('DISCOVERED', 'DOWNLOADING') THEN
        RAISE EXCEPTION 'File % cannot be validated from status %',
            p_etl_file_id, v_file.file_status;
    END IF;

    SELECT b.pipeline_name, b.source_name
    INTO v_pipeline_name, v_source_name
    FROM control.etl_batch b
    WHERE b.batch_id = v_file.batch_id;

    IF p_content_length <= 0 THEN
        RAISE EXCEPTION 'Content length must be positive';
    END IF;

    IF p_source_file_sha256 !~ '^[0-9a-f]{64}$'
       OR p_schema_fingerprint !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'SHA-256 and schema fingerprint must be lowercase 64-character hex';
    END IF;

    INSERT INTO control.source_file_version (
        source_file, source_url, source_year_month, source_file_sha256,
        schema_fingerprint, schema_version, source_last_modified, content_length
    )
    VALUES (
        v_file.source_file, v_file.source_url, v_file.source_year_month,
        p_source_file_sha256, p_schema_fingerprint, p_schema_version,
        v_file.source_last_modified, p_content_length
    )
    ON CONFLICT (source_file, source_file_sha256) DO UPDATE
    SET source_url = EXCLUDED.source_url,
        source_year_month = EXCLUDED.source_year_month,
        schema_fingerprint = EXCLUDED.schema_fingerprint,
        schema_version = EXCLUDED.schema_version,
        source_last_modified = EXCLUDED.source_last_modified,
        content_length = EXCLUDED.content_length,
        verified_at = clock_timestamp()
    RETURNING file_version_id INTO v_file_version_id;

    IF EXISTS (
        SELECT 1
        FROM control.etl_file f
        JOIN control.etl_batch b ON b.batch_id = f.batch_id
        WHERE f.source_file = v_file.source_file
          AND b.pipeline_name = v_pipeline_name
          AND b.source_name = v_source_name
          AND f.source_file_sha256 = p_source_file_sha256
          AND f.file_status = 'STAGED'
          AND b.status = 'SUCCESS'
    ) THEN
        v_action := 'SKIP_ALREADY_PROCESSED';
    ELSE
        SELECT f.source_file_sha256
        INTO v_previous_sha256
        FROM control.etl_file f
        JOIN control.etl_batch b ON b.batch_id = f.batch_id
        WHERE f.source_file = v_file.source_file
          AND b.pipeline_name = v_pipeline_name
          AND b.source_name = v_source_name
          AND f.source_file_sha256 IS NOT NULL
          AND f.source_file_sha256 <> p_source_file_sha256
          AND f.file_status = 'STAGED'
          AND b.status = 'SUCCESS'
        ORDER BY b.batch_id DESC
        LIMIT 1;

        SELECT EXISTS (
            SELECT 1
            FROM control.etl_file f
            JOIN control.etl_batch b ON b.batch_id = f.batch_id
            WHERE f.source_file = v_file.source_file
              AND b.pipeline_name = v_pipeline_name
              AND b.source_name = v_source_name
              AND f.source_file_sha256 = p_source_file_sha256
              AND f.approval_status = 'APPROVED'
              AND f.file_status = 'APPROVED_SOURCE_VERSION'
        )
        INTO v_approved;

        SELECT EXISTS (
            SELECT 1
            FROM control.etl_file f
            JOIN control.etl_batch b ON b.batch_id = f.batch_id
            WHERE f.source_file = v_file.source_file
              AND b.pipeline_name = v_pipeline_name
              AND b.source_name = v_source_name
              AND f.source_file_sha256 = p_source_file_sha256
              AND f.file_status = 'STAGED'
              AND b.status = 'FAILED'
        )
        INTO v_failed_staged;

        IF v_previous_sha256 IS NOT NULL AND NOT v_approved THEN
            v_action := 'STOP_SOURCE_VERSION_CHANGED';
        ELSIF v_failed_staged THEN
            v_action := 'REPROCESS_IDEMPOTENTLY';
        ELSE
            v_action := 'PROCESS';
        END IF;
    END IF;

    UPDATE control.etl_file f
    SET content_length = p_content_length,
        source_file_sha256 = p_source_file_sha256,
        schema_fingerprint = p_schema_fingerprint,
        schema_version = p_schema_version,
        file_version_id = v_file_version_id,
        validated_at = clock_timestamp(),
        previous_sha256 = CASE
            WHEN v_action = 'STOP_SOURCE_VERSION_CHANGED' THEN v_previous_sha256
            ELSE f.previous_sha256
        END,
        approval_status = CASE
            WHEN v_action = 'STOP_SOURCE_VERSION_CHANGED' THEN 'PENDING'
            ELSE f.approval_status
        END,
        file_status = CASE v_action
            WHEN 'SKIP_ALREADY_PROCESSED' THEN 'SKIPPED_ALREADY_PROCESSED'
            WHEN 'STOP_SOURCE_VERSION_CHANGED' THEN 'SOURCE_VERSION_CHANGED'
            ELSE 'VALIDATED'
        END
    WHERE f.etl_file_id = p_etl_file_id;

    UPDATE control.etl_batch
    SET files_validated = files_validated + 1,
        heartbeat_at = clock_timestamp()
    WHERE batch_id = v_file.batch_id;

    RETURN v_action;
END;
$$;

CREATE OR REPLACE FUNCTION control.approve_source_version(
    p_etl_file_id bigint,
    p_approved_by varchar,
    p_approval_reason text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF NULLIF(btrim(p_approved_by), '') IS NULL
       OR NULLIF(btrim(p_approval_reason), '') IS NULL THEN
        RAISE EXCEPTION 'approved_by and approval_reason are required';
    END IF;

    UPDATE control.etl_file f
    SET approval_status = 'APPROVED',
        approved_by = p_approved_by,
        approved_at = clock_timestamp(),
        approval_reason = p_approval_reason,
        file_status = 'APPROVED_SOURCE_VERSION'
    WHERE f.etl_file_id = p_etl_file_id
      AND f.file_status = 'SOURCE_VERSION_CHANGED'
      AND f.approval_status = 'PENDING';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'File % is not pending source-version approval', p_etl_file_id;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION control.start_step(
    p_batch_id bigint,
    p_step_name varchar
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_step_run_id bigint;
BEGIN
    UPDATE control.etl_step_run s
    SET status = 'RUNNING',
        started_at = clock_timestamp(),
        ended_at = NULL,
        exit_code = NULL,
        error_message = NULL
    WHERE s.batch_id = p_batch_id
      AND s.step_name = p_step_name
      AND s.status = 'PENDING'
    RETURNING s.step_run_id INTO v_step_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Step %.% is not PENDING', p_batch_id, p_step_name;
    END IF;

    PERFORM control.touch_batch_heartbeat(p_batch_id);
    RETURN v_step_run_id;
END;
$$;

CREATE OR REPLACE FUNCTION control.finish_step(
    p_batch_id bigint,
    p_step_name varchar,
    p_status varchar,
    p_exit_code integer DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb,
    p_error_message text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_status NOT IN ('SUCCESS', 'FAILED', 'SKIPPED') THEN
        RAISE EXCEPTION 'Step terminal status must be SUCCESS, FAILED or SKIPPED';
    END IF;

    IF p_status = 'SUCCESS' AND COALESCE(p_exit_code, 0) <> 0 THEN
        RAISE EXCEPTION 'Successful step cannot have non-zero exit code';
    END IF;

    UPDATE control.etl_step_run s
    SET status = p_status,
        ended_at = clock_timestamp(),
        exit_code = COALESCE(p_exit_code, CASE WHEN p_status = 'SUCCESS' THEN 0 END),
        details = COALESCE(p_details, '{}'::jsonb),
        error_message = p_error_message
    WHERE s.batch_id = p_batch_id
      AND s.step_name = p_step_name
      AND s.status = 'RUNNING';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Step %.% is not RUNNING', p_batch_id, p_step_name;
    END IF;

    PERFORM control.touch_batch_heartbeat(p_batch_id);
END;
$$;

CREATE OR REPLACE FUNCTION control.update_file_result(
    p_etl_file_id bigint,
    p_new_status varchar,
    p_rows_read bigint DEFAULT 0,
    p_rows_staged bigint DEFAULT 0,
    p_rows_rejected bigint DEFAULT 0,
    p_error_message text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_status varchar(40);
BEGIN
    IF p_new_status NOT IN ('STAGED', 'FAILED') THEN
        RAISE EXCEPTION 'File result status must be STAGED or FAILED';
    END IF;

    IF LEAST(p_rows_read, p_rows_staged, p_rows_rejected) < 0 THEN
        RAISE EXCEPTION 'File counters cannot be negative';
    END IF;

    SELECT f.file_status
    INTO v_current_status
    FROM control.etl_file f
    WHERE f.etl_file_id = p_etl_file_id
    FOR UPDATE;

    IF v_current_status IS NULL THEN
        RAISE EXCEPTION 'File % does not exist', p_etl_file_id;
    END IF;

    IF p_new_status = 'STAGED'
       AND v_current_status NOT IN ('VALIDATED', 'APPROVED_SOURCE_VERSION') THEN
        RAISE EXCEPTION 'Cannot stage file % from status %', p_etl_file_id, v_current_status;
    END IF;

    IF p_new_status = 'FAILED'
       AND v_current_status IN ('STAGED', 'SKIPPED_ALREADY_PROCESSED') THEN
        RAISE EXCEPTION 'Cannot fail terminal file % from status %', p_etl_file_id, v_current_status;
    END IF;

    UPDATE control.etl_file f
    SET file_status = p_new_status,
        rows_read = p_rows_read,
        rows_staged = p_rows_staged,
        rows_rejected = p_rows_rejected,
        staged_at = CASE WHEN p_new_status = 'STAGED' THEN clock_timestamp() ELSE f.staged_at END,
        error_message = p_error_message
    WHERE f.etl_file_id = p_etl_file_id;
END;
$$;

CREATE OR REPLACE FUNCTION control.fail_batch(
    p_batch_id bigint,
    p_error_code varchar,
    p_error_message text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_pipeline_name varchar(100);
BEGIN
    UPDATE control.etl_batch b
    SET status = 'FAILED',
        ended_at = clock_timestamp(),
        heartbeat_at = clock_timestamp(),
        error_code = p_error_code,
        error_message = p_error_message
    WHERE b.batch_id = p_batch_id
      AND b.status IN ('CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING', 'RECONCILING')
    RETURNING b.pipeline_name INTO v_pipeline_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % is not fail-able', p_batch_id;
    END IF;

    PERFORM control.log_event(
        v_pipeline_name,
        'ERROR',
        p_error_message,
        p_batch_id,
        p_event_code => p_error_code
    );
END;
$$;

CREATE OR REPLACE FUNCTION control.resume_failed_staging(
    p_batch_id bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_pipeline_name varchar(100);
    v_batch_status varchar(24);
    v_error_code varchar(100);
    v_step_status varchar(16);
BEGIN
    SELECT b.pipeline_name, b.status, b.error_code, s.status
    INTO v_pipeline_name, v_batch_status, v_error_code, v_step_status
    FROM control.etl_batch b
    JOIN control.etl_step_run s
      ON s.batch_id = b.batch_id AND s.step_name = 'STAGING'
    WHERE b.batch_id = p_batch_id
    FOR UPDATE OF b, s;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % or its STAGING step does not exist', p_batch_id;
    END IF;
    IF v_batch_status <> 'FAILED'
       OR v_error_code <> 'STAGING_FAILED'
       OR v_step_status <> 'FAILED' THEN
        RAISE EXCEPTION
            'Batch % is not resumable from failed STAGING (batch=%, code=%, step=%)',
            p_batch_id, v_batch_status, v_error_code, v_step_status;
    END IF;

    UPDATE control.etl_batch
    SET status = 'RUNNING', ended_at = NULL, heartbeat_at = clock_timestamp(),
        error_code = NULL, error_message = NULL
    WHERE batch_id = p_batch_id;

    UPDATE control.etl_step_run
    SET status = 'PENDING', started_at = NULL, ended_at = NULL,
        exit_code = NULL, details = '{}'::jsonb, error_message = NULL
    WHERE batch_id = p_batch_id AND step_name = 'STAGING';

    PERFORM control.log_event(
        v_pipeline_name, 'WARNING',
        'Resuming idempotent STAGING after a failed load', p_batch_id,
        p_event_code => 'STAGING_RESUMED'
    );
END;
$$;

CREATE OR REPLACE FUNCTION control.commit_batch_success(
    p_batch_id bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch control.etl_batch%ROWTYPE;
BEGIN
    SELECT *
    INTO v_batch
    FROM control.etl_batch b
    WHERE b.batch_id = p_batch_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % does not exist', p_batch_id;
    END IF;

    IF v_batch.status <> 'RECONCILING' THEN
        RAISE EXCEPTION 'Batch % must be RECONCILING, found %',
            p_batch_id, v_batch.status;
    END IF;

    IF v_batch.discovery_status <> 'SUCCESS'
       OR v_batch.catalog_checked_count <> v_batch.catalog_expected_count THEN
        RAISE EXCEPTION 'Discovery is incomplete for batch %: status %, checked % of %',
            p_batch_id,
            v_batch.discovery_status,
            v_batch.catalog_checked_count,
            v_batch.catalog_expected_count;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM control.etl_file f
        WHERE f.batch_id = p_batch_id
          AND f.file_status IN (
              'DISCOVERED', 'DOWNLOADING', 'SOURCE_VERSION_CHANGED', 'FAILED'
          )
    ) THEN
        RAISE EXCEPTION 'Batch % contains unresolved files', p_batch_id;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM control.etl_step_run s
        WHERE s.batch_id = p_batch_id
          AND s.is_required
          AND s.status <> 'SUCCESS'
    ) THEN
        RAISE EXCEPTION 'Batch % has required steps that are not SUCCESS', p_batch_id;
    END IF;

    IF v_batch.rows_raw
       <> v_batch.rows_canonical + v_batch.rows_technical_quarantined THEN
        RAISE EXCEPTION 'Raw reconciliation failed for batch %', p_batch_id;
    END IF;

    IF v_batch.rows_canonical
       <> v_batch.rows_fact_loaded
          + v_batch.rows_business_quarantined
          + v_batch.rows_duplicate_quarantined THEN
        RAISE EXCEPTION 'Canonical reconciliation failed for batch %', p_batch_id;
    END IF;

    PERFORM 1
    FROM control.etl_watermark w
    WHERE w.pipeline_name = v_batch.pipeline_name
      AND w.source_name = v_batch.source_name
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Watermark missing for %.%',
            v_batch.pipeline_name, v_batch.source_name;
    END IF;

    UPDATE control.etl_batch b
    SET status = 'SUCCESS',
        ended_at = clock_timestamp(),
        heartbeat_at = clock_timestamp(),
        error_code = NULL,
        error_message = NULL
    WHERE b.batch_id = p_batch_id;

    UPDATE control.etl_watermark w
    SET last_successful_extraction_time = v_batch.cet,
        last_successful_batch_id = p_batch_id,
        watermark_version = w.watermark_version + 1,
        updated_at = clock_timestamp()
    WHERE w.pipeline_name = v_batch.pipeline_name
      AND w.source_name = v_batch.source_name;

    PERFORM control.log_event(
        v_batch.pipeline_name,
        'INFO',
        'Batch committed successfully; watermark advanced to CET.',
        p_batch_id,
        p_event_code => 'BATCH_SUCCESS',
        p_details => jsonb_build_object(
            'previous_lset', v_batch.lset,
            'new_lset', v_batch.cet
        )
    );
END;
$$;
