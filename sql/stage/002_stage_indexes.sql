\set ON_ERROR_STOP on

CREATE INDEX IF NOT EXISTS ix_taxi_zone_geometry_gist
    ON stage.taxi_zone_geometry
    USING gist (geometry);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_raw_batch
    ON stage.yellow_taxi_raw(batch_id);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_raw_file_sha
    ON stage.yellow_taxi_raw(source_file_sha256);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_raw_year_month
    ON stage.yellow_taxi_raw(source_year_month);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_raw_file_row
    ON stage.yellow_taxi_raw(source_file_sha256, source_row_number);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_batch
    ON stage.yellow_taxi_canonical(batch_id);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_file_sha
    ON stage.yellow_taxi_canonical(source_file_sha256);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_year_month
    ON stage.yellow_taxi_canonical(source_year_month);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_pickup
    ON stage.yellow_taxi_canonical(pickup_datetime);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_pu_location
    ON stage.yellow_taxi_canonical(pu_location_id);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_do_location
    ON stage.yellow_taxi_canonical(do_location_id);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_batch
    ON dq.technical_quarantine(batch_id);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_file_sha
    ON dq.technical_quarantine(source_file_sha256);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_year_month
    ON dq.technical_quarantine(source_year_month);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_flags_gin
    ON dq.technical_quarantine
    USING gin (cast_error_flags);
