\set ON_ERROR_STOP on

DO $$
DECLARE
    v_missing text;
BEGIN
    WITH expected(table_schema, table_name, constraint_name) AS (
        VALUES
            ('stage', 'yellow_taxi_raw', 'uq_yellow_taxi_raw_source_record'),
            ('stage', 'yellow_taxi_raw', 'fk_yellow_taxi_raw_batch'),
            ('stage', 'yellow_taxi_canonical', 'uq_yellow_taxi_canonical_source_record'),
            ('stage', 'yellow_taxi_canonical', 'fk_yellow_taxi_canonical_batch'),
            ('dq', 'technical_quarantine', 'uq_technical_quarantine_source_record'),
            ('dq', 'technical_quarantine', 'fk_technical_quarantine_batch'),
            ('stage', 'taxi_zone_geometry', 'fk_taxi_zone_geometry_lookup')
    )
    SELECT string_agg(
        expected.table_schema || '.' || expected.table_name || '.' ||
        expected.constraint_name,
        ', ' ORDER BY expected.constraint_name
    )
    INTO v_missing
    FROM expected
    LEFT JOIN information_schema.table_constraints actual
      ON actual.table_schema = expected.table_schema
     AND actual.table_name = expected.table_name
     AND actual.constraint_name = expected.constraint_name
    WHERE actual.constraint_name IS NULL;

    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing task 4.5 constraints: %', v_missing;
    END IF;
END;
$$;

DO $$
DECLARE
    v_batch_id bigint;
BEGIN
    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, triggered_by, worker_id,
        lset, cet, status, discovery_status
    )
    VALUES (
        'qa_45_constraints', 'yellow_taxi_tripdata', 'TEST', 'sql_test', 'psql',
        '1970-01-01 00:00:00+00', clock_timestamp(), 'RUNNING', 'PENDING'
    )
    RETURNING batch_id INTO v_batch_id;

    INSERT INTO stage.yellow_taxi_raw (
        batch_id, source_file, source_year_month, source_file_sha256,
        source_row_number, source_record_key, schema_fingerprint, schema_version
    )
    VALUES (
        v_batch_id, 'constraint_test.parquet', '2024-05', repeat('a', 64),
        1, repeat('b', 64), repeat('c', 64), 'yt_schema_v001'
    );

    BEGIN
        INSERT INTO stage.yellow_taxi_raw (
            batch_id, source_file, source_year_month, source_file_sha256,
            source_row_number, source_record_key, schema_fingerprint, schema_version
        )
        VALUES (
            v_batch_id, 'constraint_test.parquet', '2024-05', repeat('a', 64),
            1, repeat('b', 64), repeat('c', 64), 'yt_schema_v001'
        );
        RAISE EXCEPTION 'Duplicate source_record_key was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    DELETE FROM stage.yellow_taxi_raw WHERE batch_id = v_batch_id;
    DELETE FROM control.etl_batch WHERE batch_id = v_batch_id;
END;
$$;

SELECT 'PASS: task 4.5 constraints and source-key uniqueness' AS result;
