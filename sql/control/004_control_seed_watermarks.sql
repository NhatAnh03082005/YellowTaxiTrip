\set ON_ERROR_STOP on

INSERT INTO control.etl_watermark (
    pipeline_name,
    source_name,
    last_successful_extraction_time,
    watermark_version
)
VALUES (
    'pl_taxi_master',
    'yellow_taxi_tripdata',
    timestamptz '1970-01-01 00:00:00+00',
    0
)
ON CONFLICT (pipeline_name, source_name) DO NOTHING;
