\set ON_ERROR_STOP on

DO $$
DECLARE
    v_batch_id bigint;
    v_initial_lset timestamptz := '2025-01-01 00:00:00+00'::timestamptz;
    v_status varchar(24);
    v_watermark control.etl_watermark%ROWTYPE;
BEGIN
    SELECT s.batch_id INTO v_batch_id
    FROM control.start_batch(
        'qa_43_t17_failure', 'yellow_taxi_tripdata', 'TEST', v_initial_lset
    ) AS s;
    PERFORM control.fail_batch(
        v_batch_id, 'SIMULATED_DBT_FAILURE', 'Intentional failure for watermark test'
    );
    SELECT status INTO v_status FROM control.etl_batch WHERE batch_id = v_batch_id;
    SELECT * INTO v_watermark FROM control.etl_watermark
    WHERE pipeline_name = 'qa_43_t17_failure'
      AND source_name = 'yellow_taxi_tripdata';
    IF v_status <> 'FAILED'
       OR v_watermark.last_successful_extraction_time <> v_initial_lset
       OR v_watermark.last_successful_batch_id IS NOT NULL
       OR v_watermark.watermark_version <> 0 THEN
        RAISE EXCEPTION 'Failure incorrectly changed the watermark';
    END IF;
    RAISE NOTICE 'PASS T17B: failed batch leaves watermark unchanged';
END;
$$;
