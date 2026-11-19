{{ config(materialized='view', schema='audit', enabled=var('enable_fingerprint_history_candidate', false), tags=['fingerprint_candidate']) }}

{# Candidate only: physical sources avoid a cycle through classification.  Do not
   switch consumers until legacy coverage and duplicate semantics are proven. #}
with persistent as not materialized (
{% for source_name, relation in [('fingerprint_persistent', 'fact_taxi_trip'), ('fingerprint_dq', 'business_quarantine'), ('fingerprint_dq', 'duplicate_quarantine')] %}
select
    decode(trim(source_record_key), 'hex') as source_record_key,
    file_version_id,
    source_row_number,
    decode(trim(source_file_sha256), 'hex') as source_file_sha256,
    decode(trim(raw_exact_fingerprint), 'hex') as raw_exact_fingerprint,
    decode(trim(candidate_trip_fingerprint), 'hex') as candidate_trip_fingerprint,
    primary_disposition,
    batch_id as last_batch_id
from {{ source(source_name, relation) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
), retired as (
    select distinct on (h.source_record_key)
        decode(trim(h.source_record_key), 'hex') as source_record_key,
        (h.prior_fact ->> 'file_version_id')::bigint as file_version_id,
        (h.prior_fact ->> 'source_row_number')::bigint as source_row_number,
        decode(trim(h.prior_fact ->> 'source_file_sha256'), 'hex') as source_file_sha256,
        decode(trim(h.prior_fact ->> 'raw_exact_fingerprint'), 'hex') as raw_exact_fingerprint,
        decode(trim(h.prior_fact ->> 'candidate_trip_fingerprint'), 'hex') as candidate_trip_fingerprint,
        h.from_disposition as primary_disposition,
        (h.prior_fact ->> 'batch_id')::bigint as last_batch_id
    from {{ source('fact_lifecycle', 'history') }} h
    where h.change_type = 'REPLACE_VERSION'
    order by h.source_record_key, h.event_id desc
)
select * from persistent
union all
select r.* from retired r
where not exists (select 1 from persistent p where p.source_record_key = r.source_record_key)
