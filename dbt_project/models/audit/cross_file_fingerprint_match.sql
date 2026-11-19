{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['fingerprint_type', 'fingerprint',
                'first_file_version_id', 'matched_file_version_id'],
    schema='audit',
    tags=['audit', 'fingerprint']
) }}

{% set history_relation = source('audit', 'v_trip_fingerprint_history')
    if var('use_fingerprint_history_candidate', false)
    else source('audit', 'trip_fingerprint_history') %}

with current_rows as (
    select * from {{ ref('int_taxi_trip_fingerprinted') }}
), matches as (
    select
        'RAW_EXACT'::varchar(16) as fingerprint_type,
        decode(trim(c.raw_exact_fingerprint), 'hex') as fingerprint,
        least(c.file_version_id, h.file_version_id) as first_file_version_id,
        greatest(c.file_version_id, h.file_version_id) as matched_file_version_id,
        c.batch_id as detected_batch_id
    from current_rows c
    join {{ history_relation }} h
      on h.raw_exact_fingerprint = decode(trim(c.raw_exact_fingerprint), 'hex')
     and h.file_version_id <> c.file_version_id
    where c.raw_exact_fingerprint is not null

    union

    select
        'CANDIDATE'::varchar(16),
        decode(trim(c.candidate_trip_fingerprint), 'hex'),
        least(c.file_version_id, h.file_version_id),
        greatest(c.file_version_id, h.file_version_id),
        c.batch_id
    from current_rows c
    join {{ history_relation }} h
      on h.candidate_trip_fingerprint = decode(trim(c.candidate_trip_fingerprint), 'hex')
     and h.file_version_id <> c.file_version_id
)
select distinct
    fingerprint_type,
    fingerprint,
    first_file_version_id,
    matched_file_version_id,
    detected_batch_id,
    clock_timestamp() as detected_at
from matches

