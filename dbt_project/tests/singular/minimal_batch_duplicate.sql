{{ config(enabled=var('dbt_minimal_mode', false), tags=['minimal_batch']) }}
{% if var('dbt_minimal_mode', false) %}
with ranked as (
    select source_record_key, file_version_id, source_row_number,
           raw_exact_fingerprint, candidate_trip_fingerprint,
           duplicate_occurrence_number, duplicate_survivor_source_record_key,
           has_in_file_candidate_match, primary_disposition,
           row_number() over(partition by file_version_id,raw_exact_fingerprint
                             order by source_row_number) as expected_occurrence,
           first_value(source_record_key) over(
             partition by file_version_id,raw_exact_fingerprint
             order by source_row_number) as expected_survivor,
           count(*) over(partition by file_version_id,candidate_trip_fingerprint)>1
             as expected_candidate
    from {{ ref('int_taxi_trip_classified') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
), checks as (
    select 'DUPLICATE_SURVIVOR'::text as rule, count(*) as failures from ranked
    where raw_exact_fingerprint is null or candidate_trip_fingerprint is null
       or duplicate_occurrence_number is distinct from expected_occurrence
       or duplicate_survivor_source_record_key is distinct from expected_survivor
       or (expected_occurrence>1) is distinct from (primary_disposition='DUPLICATE')
    union all
    select 'CANDIDATE_DIAGNOSTIC', count(*) from ranked
    where has_in_file_candidate_match is distinct from expected_candidate
    union all
    select 'DUPLICATE_TARGET', count(*) from {{ ref('duplicate_quarantine') }} d
    join ranked r using(source_record_key)
    where d.file_version_id={{ minimal_version_id() }}
      and (d.duplicate_occurrence_number is distinct from r.expected_occurrence
        or d.duplicate_survivor_source_record_key is distinct from r.expected_survivor)
    union all
    select 'CANDIDATE_SUMMARY', case when not exists (
      select 1 from {{ ref('int_taxi_trip_classified').schema }}.dbt_minimal_candidate_summary s
      cross join (select count(*) filter(where has_in_file_candidate_match) candidate_rows,
                         count(distinct candidate_trip_fingerprint)
                            filter(where has_in_file_candidate_match) candidate_groups
                  from ranked) a
      where s.batch_id={{ minimal_batch_id() }} and s.file_version_id={{ minimal_version_id() }}
        and s.candidate_rows is not distinct from a.candidate_rows
        and s.candidate_groups is not distinct from a.candidate_groups) then 1 else 0 end
)
select rule, failures from checks where failures>0
{% endif %}
