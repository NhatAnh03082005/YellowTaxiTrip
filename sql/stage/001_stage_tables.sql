\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS stage.taxi_zone_raw (
    location_id smallint PRIMARY KEY,
    borough varchar(50) NOT NULL,
    zone_name varchar(150) NOT NULL,
    service_zone varchar(50) NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_taxi_zone_raw_location_id
        CHECK (location_id BETWEEN 1 AND 265)
);

CREATE TABLE IF NOT EXISTS stage.taxi_zone_geometry (
    location_id smallint PRIMARY KEY,
    "OBJECTID" integer NULL,
    "Shape_Leng" double precision NULL,
    "Shape_Area" double precision NULL,
    geometry geometry(MultiPolygon, 2263) NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_taxi_zone_geometry_lookup
        FOREIGN KEY (location_id)
        REFERENCES stage.taxi_zone_raw(location_id),
    CONSTRAINT ck_taxi_zone_geometry_location_id
        CHECK (location_id BETWEEN 1 AND 263),
    CONSTRAINT ck_taxi_zone_geometry_srid
        CHECK (ST_SRID(geometry) = 2263),
    CONSTRAINT ck_taxi_zone_geometry_nonempty
        CHECK (NOT ST_IsEmpty(geometry)),
    CONSTRAINT ck_taxi_zone_geometry_valid
        CHECK (ST_IsValid(geometry))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.yellow_taxi_raw (
    raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    "VendorID" bigint NULL,
    "tpep_pickup_datetime" timestamp without time zone NULL,
    "tpep_dropoff_datetime" timestamp without time zone NULL,
    passenger_count bigint NULL,
    trip_distance double precision NULL,
    "RatecodeID" bigint NULL,
    store_and_fwd_flag text NULL,
    "PULocationID" bigint NULL,
    "DOLocationID" bigint NULL,
    payment_type bigint NULL,
    fare_amount double precision NULL,
    extra double precision NULL,
    mta_tax double precision NULL,
    tip_amount double precision NULL,
    tolls_amount double precision NULL,
    improvement_surcharge double precision NULL,
    total_amount double precision NULL,
    congestion_surcharge double precision NULL,
    "Airport_fee" double precision NULL,
    cbd_congestion_fee double precision NULL,

    batch_id bigint NOT NULL,
    file_version_id bigint NOT NULL,
    source_file varchar(255) NOT NULL,
    source_year_month char(7) NOT NULL,
    source_file_sha256 char(64) NOT NULL,
    source_row_number bigint NOT NULL,
    source_record_key char(64) NOT NULL,
    schema_fingerprint char(64) NOT NULL,
    schema_version varchar(20) NOT NULL,
    source_last_modified timestamptz NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_yellow_taxi_raw_source_record
        UNIQUE (source_record_key),
    CONSTRAINT fk_yellow_taxi_raw_batch
        FOREIGN KEY (batch_id)
        REFERENCES control.etl_batch(batch_id),
    CONSTRAINT fk_yellow_taxi_raw_file_version
        FOREIGN KEY (file_version_id)
        REFERENCES control.source_file_version(file_version_id),
    CONSTRAINT ck_yellow_taxi_raw_year_month
        CHECK (source_year_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT ck_yellow_taxi_raw_sha256
        CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_yellow_taxi_raw_schema_fingerprint
        CHECK (schema_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_yellow_taxi_raw_record_key
        CHECK (source_record_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_yellow_taxi_raw_row_number
        CHECK (source_row_number > 0),
    CONSTRAINT ck_yellow_taxi_raw_schema_version
        CHECK (schema_version IN ('yt_schema_v001', 'yt_schema_v002'))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.yellow_taxi_canonical (
    stage_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

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
    batch_id bigint NOT NULL,
    file_version_id bigint NOT NULL,
    source_file varchar(255) NOT NULL,
    source_year_month char(7) NOT NULL,
    source_file_sha256 char(64) NOT NULL,
    source_row_number bigint NOT NULL,
    source_record_key char(64) NOT NULL,
    schema_fingerprint char(64) NOT NULL,
    schema_version varchar(20) NOT NULL,
    source_last_modified timestamptz NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_yellow_taxi_canonical_source_record
        UNIQUE (source_record_key),
    CONSTRAINT fk_yellow_taxi_canonical_batch
        FOREIGN KEY (batch_id)
        REFERENCES control.etl_batch(batch_id),
    CONSTRAINT fk_yellow_taxi_canonical_file_version
        FOREIGN KEY (file_version_id)
        REFERENCES control.source_file_version(file_version_id),
    CONSTRAINT ck_yellow_taxi_canonical_store_flag
        CHECK (store_and_fwd_flag IS NULL OR store_and_fwd_flag IN ('Y', 'N')),
    CONSTRAINT ck_yellow_taxi_canonical_year_month
        CHECK (source_year_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT ck_yellow_taxi_canonical_sha256
        CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_yellow_taxi_canonical_schema_fingerprint
        CHECK (schema_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_yellow_taxi_canonical_record_key
        CHECK (source_record_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_yellow_taxi_canonical_row_number
        CHECK (source_row_number > 0),
    CONSTRAINT ck_yellow_taxi_canonical_schema_version
        CHECK (schema_version IN ('yt_schema_v001', 'yt_schema_v002')),
    CONSTRAINT ck_yellow_taxi_canonical_raw_payload
        CHECK (raw_payload IS NULL OR jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT ck_yellow_taxi_canonical_cast_flags
        CHECK (jsonb_typeof(cast_error_flags) = 'object')
);

CREATE TABLE IF NOT EXISTS dq.technical_quarantine (
    quarantine_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

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

    cast_error_flags jsonb NOT NULL,
    raw_payload jsonb NOT NULL,
    batch_id bigint NOT NULL,
    file_version_id bigint NOT NULL,
    source_file varchar(255) NOT NULL,
    source_year_month char(7) NOT NULL,
    source_file_sha256 char(64) NOT NULL,
    source_row_number bigint NOT NULL,
    source_record_key char(64) NOT NULL,
    schema_fingerprint char(64) NOT NULL,
    schema_version varchar(20) NOT NULL,
    source_last_modified timestamptz NULL,
    quarantined_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_technical_quarantine_source_record
        UNIQUE (source_record_key),
    CONSTRAINT fk_technical_quarantine_batch
        FOREIGN KEY (batch_id)
        REFERENCES control.etl_batch(batch_id),
    CONSTRAINT fk_technical_quarantine_file_version
        FOREIGN KEY (file_version_id)
        REFERENCES control.source_file_version(file_version_id),
    CONSTRAINT ck_technical_quarantine_has_error
        CHECK (cast_error_flags <> '{}'::jsonb),
    CONSTRAINT ck_technical_quarantine_cast_flags
        CHECK (jsonb_typeof(cast_error_flags) = 'object'),
    CONSTRAINT ck_technical_quarantine_raw_payload
        CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT ck_technical_quarantine_year_month
        CHECK (source_year_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT ck_technical_quarantine_sha256
        CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_technical_quarantine_schema_fingerprint
        CHECK (schema_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_technical_quarantine_record_key
        CHECK (source_record_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_technical_quarantine_row_number
        CHECK (source_row_number > 0),
    CONSTRAINT ck_technical_quarantine_schema_version
        CHECK (schema_version IN ('yt_schema_v001', 'yt_schema_v002'))
);
