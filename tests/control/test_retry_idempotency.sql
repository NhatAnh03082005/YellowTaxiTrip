\set ON_ERROR_STOP on

DO $$
DECLARE
    v_ok boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM control.etl_batch r1
        JOIN control.etl_batch changed ON changed.batch_id = r1.parent_batch_id
        JOIN control.etl_batch r2 ON r2.parent_batch_id = r1.batch_id
        JOIN control.etl_file f ON f.batch_id = r2.batch_id
        WHERE r1.pipeline_name = 'qa_43_t18_file'
          AND r1.run_type = 'RETRY'
          AND r1.error_code = 'SIMULATED_DOWNSTREAM_FAILURE'
          AND changed.error_code = 'SOURCE_VERSION_REVIEWED'
          AND r2.run_type = 'RETRY'
          AND f.source_file_sha256 = repeat('b', 64)::char(64)
          AND f.file_status = 'VALIDATED'
    ) INTO v_ok;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'Retry parent chain/idempotent reprocess evidence is missing';
    END IF;
    RAISE NOTICE 'PASS T18B: persisted retry chain supports idempotent reprocess';
END;
$$;
