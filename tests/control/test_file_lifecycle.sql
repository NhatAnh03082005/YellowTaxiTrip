\set ON_ERROR_STOP on

DO $$
DECLARE
    v_batch_a bigint;
    v_batch_skip bigint;
    v_batch_changed bigint;
    v_retry_1 bigint;
    v_retry_2 bigint;
    v_file_id bigint;
    v_step text;
    v_action text;
    v_file control.etl_file%ROWTYPE;
BEGIN
    -- Establish a successful historical version A.
    SELECT s.batch_id INTO v_batch_a
    FROM control.start_batch(
        'qa_43_t18_file', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    PERFORM control.update_discovery_result(v_batch_a, 'SUCCESS', 1, 1, 1);
    v_file_id := control.discover_file(
        v_batch_a, 'yellow_tripdata_2025-02.parquet', '2025-02',
        'https://example.test/yellow_tripdata_2025-02.parquet',
        '2025-03-01 00:00:00+00'::timestamptz
    );
    PERFORM control.mark_file_downloading(v_file_id);
    v_action := control.validate_file(
        v_file_id, 2000, repeat('a', 64)::char(64),
        repeat('c', 64)::char(64), 'v1'
    );
    IF v_action <> 'PROCESS' THEN
        RAISE EXCEPTION 'Initial version expected PROCESS, got %', v_action;
    END IF;
    PERFORM control.update_file_result(v_file_id, 'STAGED', 10, 10, 0, NULL);
    FOREACH v_step IN ARRAY ARRAY['DISCOVERY', 'FILE_VALIDATION', 'STAGING'] LOOP
        PERFORM control.start_step(v_batch_a, v_step);
        PERFORM control.finish_step(v_batch_a, v_step, 'SUCCESS', 0);
    END LOOP;
    PERFORM control.transition_batch(v_batch_a, 'STAGED');
    PERFORM control.transition_batch(v_batch_a, 'DBT_RUNNING');
    FOREACH v_step IN ARRAY ARRAY['DBT_BUILD_DDS', 'DBT_TEST', 'DBT_BUILD_OLAP'] LOOP
        PERFORM control.start_step(v_batch_a, v_step);
        PERFORM control.finish_step(v_batch_a, v_step, 'SUCCESS', 0);
    END LOOP;
    PERFORM control.transition_batch(v_batch_a, 'RECONCILING');
    PERFORM control.start_step(v_batch_a, 'RECONCILIATION');
    PERFORM control.update_batch_counters(v_batch_a, 10, 10, 0, 0, 0, 10, 10);
    PERFORM control.finish_step(v_batch_a, 'RECONCILIATION', 'SUCCESS', 0);
    PERFORM control.commit_batch_success(v_batch_a);

    -- Same filename and checksum must be skipped.
    SELECT s.batch_id INTO v_batch_skip
    FROM control.start_batch(
        'qa_43_t18_file', 'yellow_taxi_tripdata', 'INCREMENTAL',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    v_file_id := control.discover_file(
        v_batch_skip, 'yellow_tripdata_2025-02.parquet', '2025-02', NULL,
        '2025-03-01 00:00:00+00'::timestamptz
    );
    PERFORM control.mark_file_downloading(v_file_id);
    v_action := control.validate_file(
        v_file_id, 2000, repeat('a', 64)::char(64),
        repeat('c', 64)::char(64), 'v1'
    );
    IF v_action <> 'SKIP_ALREADY_PROCESSED' THEN
        RAISE EXCEPTION 'Known version expected SKIP, got %', v_action;
    END IF;
    PERFORM control.fail_batch(v_batch_skip, 'QA_TEST_COMPLETED', 'Skip assertion passed');

    -- Same filename with version B must stop for approval.
    SELECT s.batch_id INTO v_batch_changed
    FROM control.start_batch(
        'qa_43_t18_file', 'yellow_taxi_tripdata', 'INCREMENTAL',
        '1970-01-01 00:00:00+00'::timestamptz
    ) AS s;
    v_file_id := control.discover_file(
        v_batch_changed, 'yellow_tripdata_2025-02.parquet', '2025-02', NULL,
        '2025-03-02 00:00:00+00'::timestamptz
    );
    PERFORM control.mark_file_downloading(v_file_id);
    v_action := control.validate_file(
        v_file_id, 2001, repeat('b', 64)::char(64),
        repeat('c', 64)::char(64), 'v1'
    );
    IF v_action <> 'STOP_SOURCE_VERSION_CHANGED' THEN
        RAISE EXCEPTION 'Changed version expected STOP, got %', v_action;
    END IF;
    SELECT * INTO v_file FROM control.etl_file WHERE etl_file_id = v_file_id;
    IF v_file.file_status <> 'SOURCE_VERSION_CHANGED'
       OR v_file.approval_status <> 'PENDING'
       OR v_file.previous_sha256 <> repeat('a', 64)::char(64) THEN
        RAISE EXCEPTION 'Changed-version metadata is incomplete';
    END IF;
    PERFORM control.approve_source_version(
        v_file_id, 'project_lead', 'Verified replacement from official TLC source'
    );
    SELECT * INTO v_file FROM control.etl_file WHERE etl_file_id = v_file_id;
    IF v_file.file_status <> 'APPROVED_SOURCE_VERSION'
       OR v_file.approval_status <> 'APPROVED'
       OR v_file.approved_by <> 'project_lead'
       OR v_file.approved_at IS NULL
       OR v_file.approval_reason IS NULL THEN
        RAISE EXCEPTION 'Source-version approval evidence is incomplete';
    END IF;
    PERFORM control.fail_batch(
        v_batch_changed, 'SOURCE_VERSION_REVIEWED',
        'Approved version will be processed by a retry batch'
    );

    -- First retry processes approved B.
    SELECT s.batch_id INTO v_retry_1
    FROM control.start_batch(
        'qa_43_t18_file', 'yellow_taxi_tripdata', 'RETRY',
        '1970-01-01 00:00:00+00'::timestamptz, v_batch_changed
    ) AS s;
    v_file_id := control.discover_file(
        v_retry_1, 'yellow_tripdata_2025-02.parquet', '2025-02', NULL,
        '2025-03-02 00:00:00+00'::timestamptz
    );
    PERFORM control.mark_file_downloading(v_file_id);
    v_action := control.validate_file(
        v_file_id, 2001, repeat('b', 64)::char(64),
        repeat('c', 64)::char(64), 'v1'
    );
    IF v_action <> 'PROCESS' THEN
        RAISE EXCEPTION 'Approved version expected PROCESS, got %', v_action;
    END IF;
    PERFORM control.update_file_result(v_file_id, 'STAGED', 10, 10, 0, NULL);
    PERFORM control.fail_batch(v_retry_1, 'SIMULATED_DOWNSTREAM_FAILURE', 'Retry staging succeeded; downstream failed');

    -- A later retry of the staged version must explicitly reprocess idempotently.
    SELECT s.batch_id INTO v_retry_2
    FROM control.start_batch(
        'qa_43_t18_file', 'yellow_taxi_tripdata', 'RETRY',
        '1970-01-01 00:00:00+00'::timestamptz, v_retry_1
    ) AS s;
    v_file_id := control.discover_file(
        v_retry_2, 'yellow_tripdata_2025-02.parquet', '2025-02', NULL,
        '2025-03-02 00:00:00+00'::timestamptz
    );
    PERFORM control.mark_file_downloading(v_file_id);
    v_action := control.validate_file(
        v_file_id, 2001, repeat('b', 64)::char(64),
        repeat('c', 64)::char(64), 'v1'
    );
    IF v_action <> 'REPROCESS_IDEMPOTENTLY' THEN
        RAISE EXCEPTION 'Failed staged retry expected REPROCESS, got %', v_action;
    END IF;
    PERFORM control.fail_batch(v_retry_2, 'QA_TEST_COMPLETED', 'Retry assertions passed');

    RAISE NOTICE 'PASS T18: lifecycle, skip, version approval and retry idempotency';
END;
$$;
