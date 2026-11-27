{{ config(enabled=var('dbt_minimal_mode', false), tags=['minimal_batch']) }}
{% if var('dbt_minimal_mode', false) %}
-- Aggregate by source key, so every row is checked while stored diagnostics stay small.
with source_keys as (
    select source_record_key, count(*) as raw_n, 0::bigint as canonical_n,
           0::bigint as technical_n, 0::bigint as classified_n,
           0::bigint as fact_n, 0::bigint as business_n, 0::bigint as duplicate_n,
           null::text as disposition
    from {{ source('stage','yellow_taxi_raw') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    group by source_record_key
    union all
    select source_record_key,0,count(*),0,0,0,0,0,null::text
    from {{ source('stage','yellow_taxi_canonical') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    group by source_record_key
    union all
    select source_record_key,0,0,count(*),0,0,0,0,null::text
    from {{ source('mage_dq','technical_quarantine') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    group by source_record_key
    union all
    select source_record_key,0,0,0,count(*),0,0,0,max(primary_disposition)
    from {{ ref('int_taxi_trip_classified') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    group by source_record_key
    union all
    select source_record_key,0,0,0,0,count(*),0,0,null::text
    from {{ ref('fact_taxi_trip') }}
    where file_version_id={{ minimal_version_id() }}
    group by source_record_key
    union all
    select source_record_key,0,0,0,0,0,count(*),0,null::text
    from {{ ref('business_quarantine') }}
    where file_version_id={{ minimal_version_id() }}
    group by source_record_key
    union all
    select source_record_key,0,0,0,0,0,0,count(*),null::text
    from {{ ref('duplicate_quarantine') }}
    where file_version_id={{ minimal_version_id() }}
    group by source_record_key
), per_key as (
    select source_record_key, sum(raw_n) raw_n, sum(canonical_n) canonical_n,
           sum(technical_n) technical_n, sum(classified_n) classified_n,
           sum(fact_n) fact_n, sum(business_n) business_n,
           sum(duplicate_n) duplicate_n, max(disposition) disposition
    from source_keys group by source_record_key
), checks as (
    select count(*) filter(where source_record_key is null or raw_n<>1
             or raw_n<>canonical_n+technical_n) as source_partition,
           count(*) filter(where canonical_n<>classified_n) as canonical_classified,
           count(*) filter(where classified_n<>fact_n+business_n+duplicate_n
       or (disposition in ('ACCEPTED','WARNING') and fact_n<>1)
       or (disposition='BUSINESS_QUARANTINE' and business_n<>1)
       or (disposition='DUPLICATE' and duplicate_n<>1)
       or (classified_n>0 and disposition not in
           ('ACCEPTED','WARNING','BUSINESS_QUARANTINE','DUPLICATE'))) as classified_target
    from per_key
)
select rule, failures from checks cross join lateral (values
    ('SOURCE_PARTITION'::text, source_partition),
    ('CANONICAL_CLASSIFIED', canonical_classified),
    ('CLASSIFIED_TARGET', classified_target)
) failures_by_rule(rule, failures)
where failures>0
{% endif %}
