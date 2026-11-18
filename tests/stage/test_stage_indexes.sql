\set ON_ERROR_STOP on

DO $$
DECLARE
    v_missing text;
BEGIN
    WITH expected(schema_name, index_name) AS (
        VALUES
            ('stage', 'ix_taxi_zone_geometry_gist'),
            ('stage', 'ix_yellow_taxi_raw_file_sha'),
            ('stage', 'ix_yellow_taxi_canonical_file_sha'),
            ('dq', 'ix_technical_quarantine_batch'),
            ('dq', 'ix_technical_quarantine_file_sha'),
            ('dq', 'ix_technical_quarantine_year_month'),
            ('dq', 'ix_technical_quarantine_flags_gin')
    )
    SELECT string_agg(
        expected.schema_name || '.' || expected.index_name,
        ', ' ORDER BY expected.index_name
    )
    INTO v_missing
    FROM expected
    LEFT JOIN pg_indexes actual
      ON actual.schemaname = expected.schema_name
     AND actual.indexname = expected.index_name
    WHERE actual.indexname IS NULL;

    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing task 4.5 indexes: %', v_missing;
    END IF;
END;
$$;

DO $$
DECLARE
    v_definition text;
BEGIN
    SELECT indexdef INTO v_definition
    FROM pg_indexes
    WHERE schemaname = 'stage'
      AND indexname = 'ix_taxi_zone_geometry_gist';
    IF v_definition NOT ILIKE '%USING gist%' THEN
        RAISE EXCEPTION 'Geometry index is not GiST: %', v_definition;
    END IF;

    SELECT indexdef INTO v_definition
    FROM pg_indexes
    WHERE schemaname = 'dq'
      AND indexname = 'ix_technical_quarantine_flags_gin';
    IF v_definition NOT ILIKE '%USING gin%' THEN
        RAISE EXCEPTION 'Cast-error index is not GIN: %', v_definition;
    END IF;
END;
$$;

SELECT 'PASS: task 4.5 required indexes' AS result;
