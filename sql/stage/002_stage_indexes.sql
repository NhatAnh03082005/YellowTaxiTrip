\set ON_ERROR_STOP on

CREATE INDEX IF NOT EXISTS ix_taxi_zone_geometry_gist
    ON stage.taxi_zone_geometry
    USING gist (geometry);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_raw_file_sha
    ON stage.yellow_taxi_raw(source_file_sha256);

CREATE INDEX IF NOT EXISTS ix_yellow_taxi_canonical_file_sha
    ON stage.yellow_taxi_canonical(source_file_sha256);


CREATE INDEX IF NOT EXISTS ix_technical_quarantine_batch
    ON dq.technical_quarantine(batch_id);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_file_sha
    ON dq.technical_quarantine(source_file_sha256);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_year_month
    ON dq.technical_quarantine(source_year_month);

CREATE INDEX IF NOT EXISTS ix_technical_quarantine_flags_gin
    ON dq.technical_quarantine
    USING gin (cast_error_flags);
