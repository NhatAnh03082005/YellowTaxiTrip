\set ON_ERROR_STOP on

DO $$
DECLARE
    v_ok boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM control.etl_batch b
        JOIN control.etl_watermark w
          ON w.pipeline_name = b.pipeline_name AND w.source_name = b.source_name
        WHERE b.pipeline_name = 'qa_43_t17_empty'
          AND b.status = 'SUCCESS'
          AND b.files_selected = 0
          AND w.last_successful_batch_id = b.batch_id
          AND w.last_successful_extraction_time = b.cet
          AND w.watermark_version = 1
    ) INTO v_ok;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'Successful empty-batch watermark evidence is missing';
    END IF;
    RAISE NOTICE 'PASS T17C: persisted success watermark matches batch CET';
END;
$$;
