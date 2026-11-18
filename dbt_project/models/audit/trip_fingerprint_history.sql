{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_record_key',
    schema='audit',
    tags=['audit', 'fingerprint']
) }}

select
    decode(trim(source_record_key), 'hex') as source_record_key,
    file_version_id,
    source_row_number,
    decode(trim(source_file_sha256), 'hex') as source_file_sha256,
    decode(trim(raw_exact_fingerprint), 'hex') as raw_exact_fingerprint,
    decode(trim(candidate_trip_fingerprint), 'hex') as candidate_trip_fingerprint,
    primary_disposition,
    batch_id as last_batch_id,
    clock_timestamp() as updated_at
from {{ ref('int_taxi_trip_classified') }}

