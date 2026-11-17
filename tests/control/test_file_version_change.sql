\set ON_ERROR_STOP on

DO $$
DECLARE
    v_ok boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM control.etl_file f
        JOIN control.etl_batch b ON b.batch_id = f.batch_id
        WHERE b.pipeline_name = 'qa_43_t18_file'
          AND f.source_file = 'yellow_tripdata_2025-02.parquet'
          AND f.source_file_sha256 = repeat('b', 64)::char(64)
          AND f.previous_sha256 = repeat('a', 64)::char(64)
          AND f.file_status = 'APPROVED_SOURCE_VERSION'
    ) INTO v_ok;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'Source-version change evidence is missing';
    END IF;
    RAISE NOTICE 'PASS T18C: filename change retains previous and new SHA-256';
END;
$$;
