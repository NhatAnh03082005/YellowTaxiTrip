\set ON_ERROR_STOP on
-- Roll back an EMPTY slim-v2 contract to the retained slim-v1 physical tables.
BEGIN;
DO $guard$
BEGIN
  IF NOT EXISTS(
    SELECT 1 FROM stage.stage_storage_contract
    WHERE singleton AND storage_mode='slim-v2'
  ) THEN
    RAISE EXCEPTION 'Slim-v2 is not installed';
  END IF;
  IF EXISTS(SELECT 1 FROM stage.yellow_taxi_trip_data)
     OR EXISTS(SELECT 1 FROM stage.stage_file_batch) THEN
    RAISE EXCEPTION 'Slim-v2 staging contains data; reconcile/export it before rollback';
  END IF;
  IF EXISTS(SELECT 1 FROM control.etl_batch WHERE status NOT IN ('SUCCESS','FAILED','CANCELLED')) THEN
    RAISE EXCEPTION 'An active/pending batch exists; refusing rollback';
  END IF;
END;
$guard$;

CREATE OR REPLACE VIEW stage.yellow_taxi_raw AS
SELECT
  d.raw_row_id, d."VendorID", d."tpep_pickup_datetime", d."tpep_dropoff_datetime",
  d.passenger_count, d.trip_distance, d."RatecodeID", d.store_and_fwd_flag,
  d."PULocationID", d."DOLocationID", d.payment_type, d.fare_amount, d.extra,
  d.mta_tax, d.tip_amount, d.tolls_amount, d.improvement_surcharge,
  d.total_amount, d.congestion_surcharge, d."Airport_fee", d.cbd_congestion_fee,
  m.batch_id, d.file_version_id, v.source_file, v.source_year_month,
  v.source_file_sha256, d.source_row_number, d.source_record_key,
  v.schema_fingerprint, v.schema_version, m.source_last_modified,
  d.ingested_at, d.raw_payload
FROM stage.yellow_taxi_raw_data d
JOIN stage.stage_file_batch m USING(file_version_id)
JOIN control.source_file_version v USING(file_version_id);

CREATE OR REPLACE VIEW stage.yellow_taxi_canonical AS
SELECT
  d.stage_row_id, d.vendor_id, d.pickup_datetime, d.dropoff_datetime,
  d.passenger_count, d.trip_distance, d.rate_code_id, d.store_and_fwd_flag,
  d.pu_location_id, d.do_location_id, d.payment_type_id, d.fare_amount,
  d.extra, d.mta_tax, d.tip_amount, d.tolls_amount, d.improvement_surcharge,
  d.total_amount, d.congestion_surcharge, d.airport_fee, d.cbd_congestion_fee,
  d.cast_error_flags, d.raw_payload, m.batch_id, d.file_version_id,
  v.source_file, v.source_year_month, v.source_file_sha256,
  d.source_row_number, d.source_record_key, v.schema_fingerprint,
  v.schema_version, m.source_last_modified, d.ingested_at
FROM stage.yellow_taxi_canonical_data d
JOIN stage.stage_file_batch m USING(file_version_id)
JOIN control.source_file_version v USING(file_version_id);

DROP TABLE stage.yellow_taxi_trip_data;
UPDATE stage.stage_storage_contract SET storage_mode='slim-v1' WHERE singleton;
COMMIT;
