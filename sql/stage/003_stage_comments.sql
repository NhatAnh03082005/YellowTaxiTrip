\set ON_ERROR_STOP on

COMMENT ON TABLE stage.taxi_zone_raw IS
    'Validated TLC Taxi Zone lookup; 265 natural LocationID members.';
COMMENT ON TABLE stage.taxi_zone_geometry IS
    'Validated TLC Taxi Zone polygons; 263 EPSG:2263 MultiPolygon members.';
COMMENT ON TABLE stage.yellow_taxi_raw IS
    'Immutable source-precision Yellow Taxi staging rows with stable source identity.';
COMMENT ON TABLE stage.yellow_taxi_canonical IS
    'Technically canonical Yellow Taxi rows; business DQ belongs to dbt.';
COMMENT ON TABLE dq.technical_quarantine IS
    'Rows rejected for required technical cast failures with raw payload retained.';

COMMENT ON COLUMN stage.yellow_taxi_raw.source_record_key IS
    'SHA256(lower(source_file_sha256) || '':'' || decimal(source_row_number)).';
COMMENT ON COLUMN stage.yellow_taxi_canonical.source_record_key IS
    'Stable idempotency key passed from Mage; unique and independent of batch_id.';
COMMENT ON COLUMN stage.yellow_taxi_canonical.pickup_datetime IS
    'New York local wall time; timestamp without time zone.';
COMMENT ON COLUMN stage.yellow_taxi_canonical.cbd_congestion_fee IS
    'Structurally absent/null for schema v001 (2024); recognized in schema v002.';
COMMENT ON COLUMN dq.technical_quarantine.cast_error_flags IS
    'JSON object identifying required technical fields that could not be cast.';
