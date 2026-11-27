\set ON_ERROR_STOP on
-- Upgrade empty slim-v1 staging to one physical trip row per source row.
-- Logical Raw/Canonical view contracts remain unchanged for dbt and DQ tests.
BEGIN;

DO $guard$
DECLARE
  current_mode text;
BEGIN
  IF to_regclass('stage.stage_storage_contract') IS NULL THEN
    RAISE EXCEPTION 'Apply reviewed migration 009 before slim-v2';
  END IF;
  SELECT storage_mode INTO current_mode FROM stage.stage_storage_contract WHERE singleton;
  IF current_mode <> 'slim-v1' THEN
    RAISE EXCEPTION 'Expected slim-v1 contract, found %', current_mode;
  END IF;
  IF EXISTS(SELECT 1 FROM stage.yellow_taxi_raw_data)
     OR EXISTS(SELECT 1 FROM stage.yellow_taxi_canonical_data)
     OR EXISTS(SELECT 1 FROM stage.stage_file_batch) THEN
    RAISE EXCEPTION 'Slim-v1 staging is not empty; complete DDS and approved cleanup first';
  END IF;
  IF EXISTS(SELECT 1 FROM control.etl_batch WHERE status NOT IN ('SUCCESS','FAILED','CANCELLED')) THEN
    RAISE EXCEPTION 'An active/pending batch exists; refusing slim-v2 migration';
  END IF;
END;
$guard$;

CREATE UNLOGGED TABLE stage.yellow_taxi_trip_data (
  trip_row_id bigint GENERATED ALWAYS AS IDENTITY,

  raw_vendor_id bigint NULL,
  raw_pickup_datetime timestamp without time zone NULL,
  raw_dropoff_datetime timestamp without time zone NULL,
  raw_passenger_count bigint NULL,
  raw_trip_distance double precision NULL,
  raw_rate_code_id bigint NULL,
  raw_store_and_fwd_flag text NULL,
  raw_pu_location_id bigint NULL,
  raw_do_location_id bigint NULL,
  raw_payment_type_id bigint NULL,
  raw_fare_amount double precision NULL,
  raw_extra double precision NULL,
  raw_mta_tax double precision NULL,
  raw_tip_amount double precision NULL,
  raw_tolls_amount double precision NULL,
  raw_improvement_surcharge double precision NULL,
  raw_total_amount double precision NULL,
  raw_congestion_surcharge double precision NULL,
  raw_airport_fee double precision NULL,
  raw_cbd_congestion_fee double precision NULL,

  vendor_id smallint NULL,
  pickup_datetime timestamp without time zone NULL,
  dropoff_datetime timestamp without time zone NULL,
  passenger_count smallint NULL,
  trip_distance numeric(10,3) NULL,
  rate_code_id smallint NULL,
  store_and_fwd_flag char(1) NULL,
  pu_location_id smallint NULL,
  do_location_id smallint NULL,
  payment_type_id smallint NULL,
  fare_amount numeric(12,2) NULL,
  extra numeric(12,2) NULL,
  mta_tax numeric(12,2) NULL,
  tip_amount numeric(12,2) NULL,
  tolls_amount numeric(12,2) NULL,
  improvement_surcharge numeric(12,2) NULL,
  total_amount numeric(12,2) NULL,
  congestion_surcharge numeric(12,2) NULL,
  airport_fee numeric(12,2) NULL,
  cbd_congestion_fee numeric(12,2) NULL,

  cast_error_flags jsonb NOT NULL DEFAULT '{}'::jsonb,
  raw_payload jsonb NULL,
  technical_valid boolean NOT NULL,
  file_version_id bigint NOT NULL,
  source_row_number bigint NOT NULL,
  source_record_key char(64) NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT yellow_taxi_trip_data_pkey PRIMARY KEY (trip_row_id),
  CONSTRAINT yellow_taxi_trip_data_source_record_key_key UNIQUE (source_record_key),
  CONSTRAINT ck_yellow_taxi_trip_data_file_version CHECK (file_version_id > 0),
  CONSTRAINT ck_yellow_taxi_trip_data_row_number CHECK (source_row_number > 0),
  CONSTRAINT ck_yellow_taxi_trip_data_record_key CHECK (source_record_key ~ '^[0-9a-f]{64}$'),
  CONSTRAINT ck_yellow_taxi_trip_data_store_flag
    CHECK (store_and_fwd_flag IS NULL OR store_and_fwd_flag IN ('Y','N')),
  CONSTRAINT ck_yellow_taxi_trip_data_cast_flags
    CHECK (jsonb_typeof(cast_error_flags) = 'object'),
  CONSTRAINT ck_yellow_taxi_trip_data_raw_payload
    CHECK (raw_payload IS NULL OR jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT ck_yellow_taxi_trip_data_reject_payload
    CHECK (technical_valid OR (cast_error_flags <> '{}'::jsonb AND raw_payload IS NOT NULL))
);

-- CREATE OR REPLACE preserves the view OIDs, so reviewed downstream dbt views
-- keep working without CASCADE or hidden dependency loss.
CREATE OR REPLACE VIEW stage.yellow_taxi_raw AS
SELECT
  d.trip_row_id AS raw_row_id,
  d.raw_vendor_id AS "VendorID",
  d.raw_pickup_datetime AS "tpep_pickup_datetime",
  d.raw_dropoff_datetime AS "tpep_dropoff_datetime",
  d.raw_passenger_count AS passenger_count,
  d.raw_trip_distance AS trip_distance,
  d.raw_rate_code_id AS "RatecodeID",
  d.raw_store_and_fwd_flag AS store_and_fwd_flag,
  d.raw_pu_location_id AS "PULocationID",
  d.raw_do_location_id AS "DOLocationID",
  d.raw_payment_type_id AS payment_type,
  d.raw_fare_amount AS fare_amount,
  d.raw_extra AS extra,
  d.raw_mta_tax AS mta_tax,
  d.raw_tip_amount AS tip_amount,
  d.raw_tolls_amount AS tolls_amount,
  d.raw_improvement_surcharge AS improvement_surcharge,
  d.raw_total_amount AS total_amount,
  d.raw_congestion_surcharge AS congestion_surcharge,
  d.raw_airport_fee AS "Airport_fee",
  d.raw_cbd_congestion_fee AS cbd_congestion_fee,
  m.batch_id,
  d.file_version_id,
  v.source_file,
  v.source_year_month,
  v.source_file_sha256,
  d.source_row_number,
  d.source_record_key,
  v.schema_fingerprint,
  v.schema_version,
  m.source_last_modified,
  d.ingested_at,
  d.raw_payload
FROM stage.yellow_taxi_trip_data d
JOIN stage.stage_file_batch m USING(file_version_id)
JOIN control.source_file_version v USING(file_version_id);

CREATE OR REPLACE VIEW stage.yellow_taxi_canonical AS
SELECT
  d.trip_row_id AS stage_row_id,
  d.vendor_id,
  d.pickup_datetime,
  d.dropoff_datetime,
  d.passenger_count,
  d.trip_distance,
  d.rate_code_id,
  d.store_and_fwd_flag,
  d.pu_location_id,
  d.do_location_id,
  d.payment_type_id,
  d.fare_amount,
  d.extra,
  d.mta_tax,
  d.tip_amount,
  d.tolls_amount,
  d.improvement_surcharge,
  d.total_amount,
  d.congestion_surcharge,
  d.airport_fee,
  d.cbd_congestion_fee,
  d.cast_error_flags,
  d.raw_payload,
  m.batch_id,
  d.file_version_id,
  v.source_file,
  v.source_year_month,
  v.source_file_sha256,
  d.source_row_number,
  d.source_record_key,
  v.schema_fingerprint,
  v.schema_version,
  m.source_last_modified,
  d.ingested_at
FROM stage.yellow_taxi_trip_data d
JOIN stage.stage_file_batch m USING(file_version_id)
JOIN control.source_file_version v USING(file_version_id)
WHERE d.technical_valid;

ALTER TABLE stage.stage_storage_contract
  DROP CONSTRAINT stage_storage_contract_storage_mode_check;
ALTER TABLE stage.stage_storage_contract
  ADD CONSTRAINT stage_storage_contract_storage_mode_check
  CHECK(storage_mode IN ('slim-v1','slim-v2'));
UPDATE stage.stage_storage_contract SET storage_mode='slim-v2' WHERE singleton;

COMMENT ON TABLE stage.yellow_taxi_trip_data IS
  'Slim-v2 single physical source row. Raw and accepted Canonical remain separate logical views.';
COMMENT ON VIEW stage.yellow_taxi_raw IS
  'Lossless logical Raw contract over slim-v2; original cast failures remain in raw_payload.';
COMMENT ON VIEW stage.yellow_taxi_canonical IS
  'Canonical accepted partition over slim-v2; rejected rows remain in technical quarantine.';
COMMIT;
