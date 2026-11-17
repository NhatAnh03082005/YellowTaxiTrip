\set ON_ERROR_STOP on

DO $$
DECLARE
    v_backfill_id bigint;
    v_baseline_id bigint;
    v_incremental_id bigint;
    v_file_id bigint;
    v_lset timestamptz;
    v_cet timestamptz;
    v_step text;
BEGIN
    SELECT s.batch_id INTO v_backfill_id
    FROM control.start_batch(
        'qa_43_t16_backfill', 'yellow_taxi_tripdata', 'BACKFILL',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;

    IF NOT control.should_discover_file(
        v_backfill_id, 'yellow_tripdata_2024-01.parquet',
        '2024-02-01 00:00:00+00'::timestamptz,
        repeat('a', 64)::char(64)
    ) THEN
        RAISE EXCEPTION 'BACKFILL incorrectly excluded an old file';
    END IF;

    BEGIN
        PERFORM control.update_discovery_result(v_backfill_id, 'SUCCESS', 24, 23, 23);
        RAISE EXCEPTION 'Expected incomplete discovery to be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'Expected incomplete discovery to be rejected'
           OR position('Discovery cannot succeed' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    PERFORM control.update_discovery_result(v_backfill_id, 'SUCCESS', 24, 24, 24);
    PERFORM control.fail_batch(
        v_backfill_id, 'QA_TEST_COMPLETED', 'T16 BACKFILL assertions passed'
    );

    SELECT s.batch_id INTO v_baseline_id
    FROM control.start_batch(
        'qa_43_t16_incremental', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    PERFORM control.update_discovery_result(v_baseline_id, 'SUCCESS', 1, 1, 1);
    v_file_id := control.discover_file(
        v_baseline_id, 'yellow_tripdata_2025-01.parquet', '2025-01',
        'https://example.test/yellow_tripdata_2025-01.parquet',
        '2025-02-01 00:00:00+00'::timestamptz
    );
    PERFORM control.mark_file_downloading(v_file_id);
    IF control.validate_file(
        v_file_id, 1000, repeat('a', 64)::char(64),
        repeat('c', 64)::char(64), 'v1'
    ) <> 'PROCESS' THEN
        RAISE EXCEPTION 'First file version should return PROCESS';
    END IF;
    PERFORM control.update_file_result(v_file_id, 'STAGED', 1, 1, 0, NULL);

    FOREACH v_step IN ARRAY ARRAY['DISCOVERY', 'FILE_VALIDATION', 'STAGING'] LOOP
        PERFORM control.start_step(v_baseline_id, v_step);
        PERFORM control.finish_step(v_baseline_id, v_step, 'SUCCESS', 0);
    END LOOP;
    PERFORM control.transition_batch(v_baseline_id, 'STAGED');
    PERFORM control.transition_batch(v_baseline_id, 'DBT_RUNNING');
    FOREACH v_step IN ARRAY ARRAY['DBT_BUILD_DDS', 'DBT_TEST', 'DBT_BUILD_OLAP'] LOOP
        PERFORM control.start_step(v_baseline_id, v_step);
        PERFORM control.finish_step(v_baseline_id, v_step, 'SUCCESS', 0);
    END LOOP;
    PERFORM control.transition_batch(v_baseline_id, 'RECONCILING');
    PERFORM control.start_step(v_baseline_id, 'RECONCILIATION');
    PERFORM control.update_batch_counters(v_baseline_id, 1, 1, 0, 0, 0, 1, 1);
    PERFORM control.finish_step(v_baseline_id, 'RECONCILIATION', 'SUCCESS', 0);
    PERFORM control.commit_batch_success(v_baseline_id);

    SELECT s.batch_id, s.lset, s.cet
    INTO v_incremental_id, v_lset, v_cet
    FROM control.start_batch(
        'qa_43_t16_incremental', 'yellow_taxi_tripdata', 'INCREMENTAL',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;

    IF control.should_discover_file(
        v_incremental_id, 'yellow_tripdata_2025-01.parquet', v_lset,
        repeat('a', 64)::char(64)
    ) THEN
        RAISE EXCEPTION 'Known identity outside window should be excluded';
    END IF;
    IF NOT control.should_discover_file(
        v_incremental_id, 'yellow_tripdata_2025-01.parquet', v_lset,
        repeat('b', 64)::char(64)
    ) THEN
        RAISE EXCEPTION 'Changed checksum should be selected';
    END IF;
    IF NOT control.should_discover_file(
        v_incremental_id, 'yellow_tripdata_unseen.parquet',
        v_lset - interval '1 year', repeat('d', 64)::char(64)
    ) THEN
        RAISE EXCEPTION 'Never-successful file should be selected';
    END IF;
    IF v_cet > v_lset AND NOT control.should_discover_file(
        v_incremental_id, 'yellow_tripdata_2025-01.parquet',
        v_lset + ((v_cet - v_lset) / 2), repeat('a', 64)::char(64)
    ) THEN
        RAISE EXCEPTION 'File inside extraction window should be selected';
    END IF;

    PERFORM control.fail_batch(
        v_incremental_id, 'QA_TEST_COMPLETED', 'T16 INCREMENTAL assertions passed'
    );
    RAISE NOTICE 'PASS T16: BACKFILL, INCREMENTAL and discovery completeness';
END;
$$;
