\set ON_ERROR_STOP on

DO $$
DECLARE
    v_stale_id bigint;
    v_recovery_id bigint;
    v_count integer;
    v_status varchar(24);
BEGIN
    SELECT s.batch_id INTO v_stale_id
    FROM control.start_batch(
        'qa_43_t19_stale', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;

    UPDATE control.etl_batch
    SET heartbeat_at = clock_timestamp() - interval '20 minutes'
    WHERE batch_id = v_stale_id;
    v_count := control.mark_stale_batches(interval '15 minutes');
    SELECT status INTO v_status FROM control.etl_batch WHERE batch_id = v_stale_id;
    IF v_count < 1 OR v_status <> 'STALE' THEN
        RAISE EXCEPTION 'Lost heartbeat did not produce STALE status';
    END IF;

    BEGIN
        PERFORM * FROM control.start_batch(
            'qa_43_t19_stale', 'yellow_taxi_tripdata', 'TEST',
            '1970-01-01 00:00:00+00'::timestamptz
        );
        RAISE EXCEPTION 'Expected STALE batch to block a new run';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'Expected STALE batch to block a new run'
           OR position('Active or STALE batch already exists' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    PERFORM control.resolve_stale_batch(
        v_stale_id, 'FAILED', 'Worker stopped during QA stale test', 'project_lead'
    );
    SELECT s.batch_id INTO v_recovery_id
    FROM control.start_batch(
        'qa_43_t19_stale', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    PERFORM control.fail_batch(v_recovery_id, 'QA_TEST_COMPLETED', 'STALE recovery assertions passed');

    RAISE NOTICE 'PASS T19B: heartbeat timeout, STALE blocking and recovery';
END;
$$;
