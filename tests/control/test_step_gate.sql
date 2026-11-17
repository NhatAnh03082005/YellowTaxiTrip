\set ON_ERROR_STOP on

DO $$
DECLARE
    v_batch_id bigint;
    v_step text;
    v_status varchar(24);
    v_watermark_batch_id bigint;
BEGIN
    SELECT s.batch_id INTO v_batch_id
    FROM control.start_batch(
        'qa_43_t20_gate', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    PERFORM control.update_discovery_result(v_batch_id, 'SUCCESS', 1, 1, 0);
    PERFORM control.transition_batch(v_batch_id, 'STAGED');
    PERFORM control.transition_batch(v_batch_id, 'DBT_RUNNING');
    PERFORM control.transition_batch(v_batch_id, 'RECONCILING');

    BEGIN
        PERFORM control.commit_batch_success(v_batch_id);
        RAISE EXCEPTION 'Expected missing required steps to block success';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'Expected missing required steps to block success'
           OR position('required steps that are not SUCCESS' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    FOREACH v_step IN ARRAY ARRAY[
        'DISCOVERY', 'FILE_VALIDATION', 'STAGING', 'DBT_BUILD_DDS',
        'DBT_TEST', 'RECONCILIATION', 'DBT_BUILD_OLAP'
    ] LOOP
        PERFORM control.start_step(v_batch_id, v_step);
        PERFORM control.finish_step(v_batch_id, v_step, 'SUCCESS', 0);
    END LOOP;

    PERFORM control.update_batch_counters(v_batch_id, 10, 9, 0, 0, 0, 9, 9);
    BEGIN
        PERFORM control.commit_batch_success(v_batch_id);
        RAISE EXCEPTION 'Expected raw reconciliation mismatch to block success';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'Expected raw reconciliation mismatch to block success'
           OR position('Raw reconciliation failed' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    PERFORM control.update_batch_counters(v_batch_id, 10, 9, 1, 0, 0, 8, 8);
    BEGIN
        PERFORM control.commit_batch_success(v_batch_id);
        RAISE EXCEPTION 'Expected canonical reconciliation mismatch to block success';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'Expected canonical reconciliation mismatch to block success'
           OR position('Canonical reconciliation failed' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    PERFORM control.update_batch_counters(v_batch_id, 10, 9, 1, 0, 0, 9, 9);
    PERFORM control.commit_batch_success(v_batch_id);
    SELECT status INTO v_status FROM control.etl_batch WHERE batch_id = v_batch_id;
    SELECT last_successful_batch_id INTO v_watermark_batch_id
    FROM control.etl_watermark
    WHERE pipeline_name = 'qa_43_t20_gate'
      AND source_name = 'yellow_taxi_tripdata';
    IF v_status <> 'SUCCESS' OR v_watermark_batch_id <> v_batch_id THEN
        RAISE EXCEPTION 'Corrected reconciliation did not commit atomically';
    END IF;

    RAISE NOTICE 'PASS T20: required-step and two-level reconciliation gates';
END;
$$;
