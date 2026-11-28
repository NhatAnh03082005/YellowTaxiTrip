{{ config(enabled=var('dbt_minimal_mode', false), tags=['minimal_batch']) }}
{% if var('dbt_minimal_mode', false) %}
-- Each source row contributes one branch tag. Aggregate once per key; keep
-- numeric counters and the existing failure predicates without assuming unique input.
with source_keys as (
    select source_record_key,0::smallint as branch,null::text as disposition
    from {{ source('stage','yellow_taxi_raw') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    union all
    select source_record_key,1::smallint as branch,null::text as disposition
    from {{ source('stage','yellow_taxi_canonical') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    union all
    select source_record_key,2::smallint as branch,null::text as disposition
    from {{ source('mage_dq','technical_quarantine') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    union all
    select source_record_key,3::smallint as branch,primary_disposition as disposition
    from {{ ref('int_taxi_trip_classified') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    union all
    select source_record_key,4::smallint as branch,null::text as disposition
    from {{ ref('fact_taxi_trip') }}
    where file_version_id={{ minimal_version_id() }}
    union all
    select source_record_key,5::smallint as branch,null::text as disposition
    from {{ ref('business_quarantine') }}
    where file_version_id={{ minimal_version_id() }}
    union all
    select source_record_key,6::smallint as branch,null::text as disposition
    from {{ ref('duplicate_quarantine') }}
    where file_version_id={{ minimal_version_id() }}
), per_key as (
    select source_record_key,
        count(*) filter(where branch=0)::numeric as raw_n,
        count(*) filter(where branch=1)::numeric as canonical_n,
        count(*) filter(where branch=2)::numeric as technical_n,
        count(*) filter(where branch=3)::numeric as classified_n,
        count(*) filter(where branch=4)::numeric as fact_n,
        count(*) filter(where branch=5)::numeric as business_n,
        count(*) filter(where branch=6)::numeric as duplicate_n,
        max(disposition) as disposition
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
