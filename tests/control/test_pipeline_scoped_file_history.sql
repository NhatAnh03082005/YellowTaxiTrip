\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    v_qa_batch_id bigint;
    v_production_batch_id bigint;
    v_production_file_id bigint;
    v_action text;
BEGIN
    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, lset, cet, status,
        discovery_status, ended_at
    )
    VALUES (
        'qa_pipeline_scope_test', 'yellow_taxi_tripdata', 'TEST',
        '1970-01-01 00:00:00+00', clock_timestamp(), 'SUCCESS',
        'SUCCESS', clock_timestamp()
    )
    RETURNING batch_id INTO v_qa_batch_id;

    INSERT INTO control.etl_file (
        batch_id, source_file, source_year_month, content_length,
        source_file_sha256, schema_fingerprint, schema_version,
        file_status, validated_at, staged_at
    )
    VALUES (
        v_qa_batch_id, 'yellow_tripdata_2025-01.parquet', '2025-01', 100,
        repeat('a', 64), repeat('c', 64), 'qa_schema',
        'STAGED', clock_timestamp(), clock_timestamp()
    );

    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, lset, cet, status,
        discovery_status
    )
    VALUES (
        'pl_taxi_master_scope_test', 'yellow_taxi_tripdata', 'BACKFILL',
        '1970-01-01 00:00:00+00', clock_timestamp(), 'RUNNING',
        'SUCCESS'
    )
    RETURNING batch_id INTO v_production_batch_id;

    INSERT INTO control.etl_file (
        batch_id, source_file, source_year_month, file_status,
        download_started_at
    )
    VALUES (
        v_production_batch_id, 'yellow_tripdata_2025-01.parquet', '2025-01',
        'DOWNLOADING', clock_timestamp()
    )
    RETURNING etl_file_id INTO v_production_file_id;

    v_action := control.validate_file(
        v_production_file_id,
        200,
        repeat('b', 64)::char(64),
        repeat('d', 64)::char(64),
        'yt_schema_v002'
    );

    IF v_action <> 'PROCESS' THEN
        RAISE EXCEPTION
            'Cross-pipeline QA history contaminated production action: %',
            v_action;
    END IF;

    RAISE NOTICE
        'PASS: file history is scoped by pipeline_name and source_name';
END;
$$;

ROLLBACK;
