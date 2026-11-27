{{ config(enabled=var('dbt_minimal_mode', false), tags=['minimal_batch']) }}
{% if var('dbt_minimal_mode', false) %}
with scope as (
    select f.batch_id, f.file_version_id, v.source_file, v.source_file_sha256,
           v.source_year_month, v.schema_fingerprint, v.schema_version
    from {{ source('control','etl_file') }} f
    join {{ source('control','source_file_version') }} v using(file_version_id)
    where f.batch_id={{ minimal_batch_id() }} and f.file_version_id={{ minimal_version_id() }}
      and f.file_status='STAGED' and f.source_file=v.source_file
      and f.source_file_sha256=v.source_file_sha256
), checks as (
    select 'FILE_SCOPE'::text as rule, abs(count(*)-1) as failures from scope
    union all
    select 'WORKSPACE_SCOPE', case when exists (
      select 1 from {{ ref('int_taxi_trip_classified') }} c
      where c.batch_id is distinct from {{ minimal_batch_id() }}
         or c.file_version_id is distinct from {{ minimal_version_id() }})
      then 1 else 0 end
    union all
    select 'WORKSPACE_MARKER', case when exists (
      select 1 where not exists (
        select 1 from {{ ref('int_taxi_trip_classified').schema }}.dbt_minimal_workspace_state w
        where w.model_name='int_taxi_trip_classified'
          and w.batch_id={{ minimal_batch_id() }}
          and w.file_version_id={{ minimal_version_id() }} and w.valid))
      then 1 else 0 end
    union all
    select 'STAGING_MIXED_SCOPE', count(*) from (
      select batch_id,file_version_id from {{ source('stage','yellow_taxi_raw') }}
      union all select batch_id,file_version_id from {{ source('stage','yellow_taxi_canonical') }}
    ) staged
    where staged.batch_id is distinct from {{ minimal_batch_id() }}
       or staged.file_version_id is distinct from {{ minimal_version_id() }}
    union all
    select 'TECHNICAL_SCOPE', count(*) from {{ source('mage_dq','technical_quarantine') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id is distinct from {{ minimal_version_id() }}
    union all
    select 'RAW_IDENTITY', count(*) from {{ source('stage','yellow_taxi_raw') }} r
    cross join scope s
    where r.batch_id={{ minimal_batch_id() }} and r.file_version_id={{ minimal_version_id() }}
      and (r.source_file is distinct from s.source_file
        or r.source_file_sha256 is distinct from s.source_file_sha256
        or r.source_year_month is distinct from s.source_year_month
        or r.schema_fingerprint is distinct from s.schema_fingerprint
        or r.schema_version is distinct from s.schema_version
        or r.source_row_number is null or r.source_row_number<1
        or r.source_record_key is distinct from
           encode(digest(lower(r.source_file_sha256)::text || ':' || r.source_row_number::text,'sha256'),'hex'))
    union all
    select 'CANONICAL_IDENTITY', count(*) from {{ source('stage','yellow_taxi_canonical') }} c
    cross join scope s
    where c.batch_id={{ minimal_batch_id() }} and c.file_version_id={{ minimal_version_id() }}
      and (c.source_file is distinct from s.source_file
        or c.source_file_sha256 is distinct from s.source_file_sha256
        or c.source_year_month is distinct from s.source_year_month
        or c.schema_fingerprint is distinct from s.schema_fingerprint
        or c.schema_version is distinct from s.schema_version
        or c.source_row_number is null or c.source_row_number<1
        or c.source_record_key is distinct from
           encode(digest(lower(c.source_file_sha256)::text || ':' || c.source_row_number::text,'sha256'),'hex'))
    union all
    select 'TARGET_BATCH_LINEAGE', count(*) from (
      select file_version_id, batch_id from {{ ref('fact_taxi_trip') }} where file_version_id={{ minimal_version_id() }}
      union all select file_version_id, batch_id from {{ ref('business_quarantine') }} where file_version_id={{ minimal_version_id() }}
      union all select file_version_id, batch_id from {{ ref('duplicate_quarantine') }} where file_version_id={{ minimal_version_id() }}
    ) t where t.batch_id is null or not exists (
      select 1 from {{ source('control','etl_batch') }} b where b.batch_id=t.batch_id)
)
select rule, failures from checks where failures>0
{% endif %}
