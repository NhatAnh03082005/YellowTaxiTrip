\set ON_ERROR_STOP on

DO $$
DECLARE
    v_batch_id bigint;
    v_step text;
    v_batch control.etl_batch%ROWTYPE;
    v_watermark control.etl_watermark%ROWTYPE;
BEGIN
    SELECT s.batch_id INTO v_batch_id
    FROM control.start_batch(
        'qa_43_t17_empty', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    PERFORM control.update_discovery_result(v_batch_id, 'SUCCESS', 24, 24, 0);
    FOREACH v_step IN ARRAY ARRAY['DISCOVERY', 'FILE_VALIDATION', 'STAGING'] LOOP
        PERFORM control.start_step(v_batch_id, v_step);
        PERFORM control.finish_step(v_batch_id, v_step, 'SUCCESS', 0);
    END LOOP;
    PERFORM control.transition_batch(v_batch_id, 'STAGED');
    PERFORM control.transition_batch(v_batch_id, 'DBT_RUNNING');
    FOREACH v_step IN ARRAY ARRAY['DBT_BUILD_DDS', 'DBT_TEST', 'DBT_BUILD_OLAP'] LOOP
        PERFORM control.start_step(v_batch_id, v_step);
        PERFORM control.finish_step(v_batch_id, v_step, 'SUCCESS', 0);
    END LOOP;
    PERFORM control.transition_batch(v_batch_id, 'RECONCILING');
    PERFORM control.start_step(v_batch_id, 'RECONCILIATION');
    PERFORM control.update_batch_counters(v_batch_id, 0, 0, 0, 0, 0, 0, 0);
    PERFORM control.finish_step(v_batch_id, 'RECONCILIATION', 'SUCCESS', 0);
    PERFORM control.commit_batch_success(v_batch_id);

    SELECT * INTO v_batch FROM control.etl_batch WHERE batch_id = v_batch_id;
    SELECT * INTO v_watermark FROM control.etl_watermark
    WHERE pipeline_name = v_batch.pipeline_name AND source_name = v_batch.source_name;
    IF v_batch.status <> 'SUCCESS'
       OR v_batch.files_selected <> 0
       OR v_watermark.last_successful_batch_id <> v_batch_id
       OR v_watermark.last_successful_extraction_time <> v_batch.cet
       OR v_watermark.watermark_version <> 1 THEN
        RAISE EXCEPTION 'Empty-batch atomic success assertions failed';
    END IF;
    RAISE NOTICE 'PASS T17A: complete empty batch advances watermark atomically';
END;
$$;
