\set ON_ERROR_STOP on

DO $$
DECLARE
    v_missing text;
BEGIN
    SELECT string_agg(name, ', ' ORDER BY name)
    INTO v_missing
    FROM unnest(ARRAY[
        'stage.taxi_zone_raw',
        'stage.taxi_zone_geometry',
        'stage.yellow_taxi_raw',
        'stage.yellow_taxi_canonical',
        'dq.technical_quarantine'
    ]) AS expected(name)
    WHERE to_regclass(name) IS NULL;

    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing task 4.5 tables: %', v_missing;
    END IF;
END;
$$;

DO $$
DECLARE
    v_count integer;
BEGIN
    SELECT count(*) INTO v_count
    FROM information_schema.columns
    WHERE table_schema = 'stage'
      AND table_name = 'yellow_taxi_canonical'
      AND column_name IN (
          'stage_row_id', 'vendor_id', 'pickup_datetime', 'dropoff_datetime',
          'passenger_count', 'trip_distance', 'rate_code_id',
          'store_and_fwd_flag', 'pu_location_id', 'do_location_id',
          'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
          'tolls_amount', 'improvement_surcharge', 'total_amount',
          'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee',
          'cast_error_flags', 'raw_payload', 'batch_id', 'source_file',
          'source_year_month', 'source_file_sha256', 'source_row_number',
          'source_record_key', 'schema_fingerprint', 'schema_version',
          'source_last_modified', 'ingested_at'
      );

    IF v_count <> 33 THEN
        RAISE EXCEPTION 'Canonical contract expected 33 columns, found %', v_count;
    END IF;
END;
$$;

DO $$
DECLARE
    v_type text;
    v_srid integer;
BEGIN
    SELECT type, srid
    INTO v_type, v_srid
    FROM geometry_columns
    WHERE f_table_schema = 'stage'
      AND f_table_name = 'taxi_zone_geometry'
      AND f_geometry_column = 'geometry';

    IF v_type <> 'MULTIPOLYGON' OR v_srid <> 2263 THEN
        RAISE EXCEPTION 'Expected MULTIPOLYGON SRID 2263, found % SRID %',
            v_type, v_srid;
    END IF;
END;
$$;

SELECT 'PASS: task 4.5 table and canonical schema contract' AS result;
