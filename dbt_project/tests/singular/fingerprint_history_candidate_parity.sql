{{ config(enabled=(not var('dbt_minimal_mode', false)) and var('enable_fingerprint_history_candidate', false), tags=['fingerprint_candidate']) }}

{# Run only in an isolated fixture or for one explicitly selected file version.
   A zero-row result is necessary, not sufficient, for a production cutover. #}
{% set file_version = var('fingerprint_parity_file_version', 0) | int %}
{% if not var('dbt_minimal_mode', false) and var('enable_fingerprint_history_candidate', false) and file_version <= 0 %}
    {{ exceptions.raise_compiler_error('fingerprint_parity_file_version must be a positive file version id') }}
{% endif %}
with legacy as (
    select source_record_key, file_version_id, source_row_number,
           source_file_sha256, raw_exact_fingerprint, candidate_trip_fingerprint,
           primary_disposition
    from {{ source('audit', 'trip_fingerprint_history') }}
    where file_version_id = {{ file_version | int }}
), candidate as (
    select source_record_key, file_version_id, source_row_number,
           source_file_sha256, raw_exact_fingerprint, candidate_trip_fingerprint,
           primary_disposition
    from {{ ref('v_trip_fingerprint_history') }}
    where file_version_id = {{ file_version | int }}
), difference as (
    (select * from legacy except all select * from candidate)
    union all
    (select * from candidate except all select * from legacy)
)
select * from difference
union all
select null, {{ file_version | int }}, null, null, null, null, 'MISSING_LEGACY_BASELINE'
where not exists (select 1 from legacy)
