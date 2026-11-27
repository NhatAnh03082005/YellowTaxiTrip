{{ config(enabled=var('dbt_minimal_mode', false) and var('minimal_full_audit_enabled', false),
    tags=['minimal_full_audit']) }}
{# The orchestrator passes the approved catalog names. No year filter is embedded in SQL. #}
{% set expected_files = var('full_audit_file_names', []) %}
{% set expected_file_count = var('full_audit_expected_file_count', none) %}
with expected(source_file) as (
    {% if expected_files | length > 0 %}
    values
    {% for name in expected_files %}
      ('{{ name | replace("'", "''") }}'::text){{ ',' if not loop.last }}
    {% endfor %}
    {% else %}
    select null::text where false
    {% endif %}
), ranked as (
    select f.source_file, f.file_version_id,
           row_number() over(partition by f.source_file order by b.batch_id desc) rn
    from {{ source('control','etl_file') }} f
    join {{ source('control','etl_batch') }} b using(batch_id)
    where b.status='SUCCESS' and f.file_version_id is not null
), selected as (
    select e.source_file, r.file_version_id
    from expected e left join ranked r on r.source_file=e.source_file and r.rn=1
), reconciled as (
    select s.source_file,s.file_version_id,r.file_version_id as reconciled_version,
           r.source_row_delta,r.trusted_row_delta,
           r.fact_rows,r.business_rows,r.duplicate_rows,r.technical_rows,
           r.fact_fare_amount,r.fact_tip_amount,r.fact_total_amount,r.revenue_amount,
           r.source_fare_eligible_delta,r.trusted_fare_eligible_delta,
           r.source_tip_eligible_delta,r.trusted_tip_eligible_delta,
           r.source_total_eligible_delta,r.trusted_total_eligible_delta,
           r.source_fare_delta,r.trusted_fare_delta,
           r.source_tip_delta,r.trusted_tip_delta,
           r.source_total_delta,r.trusted_total_delta
    from selected s left join {{ ref('file_reconciliation') }} r using(file_version_id)
), fact_actual as (
    select count(*) rows, coalesce(sum(fare_amount),0) fare,
           coalesce(sum(tip_amount),0) tip, coalesce(sum(total_amount),0) total,
           coalesce(sum(revenue_amount),0) revenue
    from {{ ref('fact_taxi_trip') }}
), business_actual as (
    select count(*) rows, coalesce(sum(fare_amount),0) fare,
           coalesce(sum(tip_amount),0) tip, coalesce(sum(total_amount),0) total
    from {{ ref('business_quarantine') }}
), duplicate_actual as (
    select count(*) rows, coalesce(sum(fare_amount),0) fare,
           coalesce(sum(tip_amount),0) tip, coalesce(sum(total_amount),0) total
    from {{ ref('duplicate_quarantine') }}
), technical_actual as (
    select count(*) rows from {{ source('mage_dq','technical_quarantine') }} t
    join selected s using(file_version_id)
), checks as (
    select 'CONFIG_EXPECTED_FILES'::text rule, case when count(*)=0
      or count(*)<>count(distinct source_file)
      or count(*)<>{% if expected_file_count is none %}-1{% else %}{{ expected_file_count | int }}{% endif %}
      then 1 else 0 end::bigint failures from expected
    union all
    select 'DDS_SUCCESS_FILE', count(*) from selected where file_version_id is null
    union all
    select 'REFERENCE_LOOKUP', case when
      (select count(*) from {{ source('stage','taxi_zone_raw') }})
      <> {{ var('zone_lookup_expected_count') }} then 1 else 0 end
    union all
    select 'REFERENCE_GEOMETRY', case when
      (select count(*) from {{ source('stage','taxi_zone_geometry') }})
      <> {{ var('zone_geometry_expected_count') }} then 1 else 0 end
    union all
    select 'FILE_RECONCILIATION', count(*) from reconciled
      where reconciled_version is null or source_row_delta<>0 or trusted_row_delta<>0
        or source_fare_eligible_delta<>0 or trusted_fare_eligible_delta<>0
        or source_tip_eligible_delta<>0 or trusted_tip_eligible_delta<>0
        or source_total_eligible_delta<>0 or trusted_total_eligible_delta<>0
        or abs(source_fare_delta)>{{ var('amount_reconciliation_tolerance') }}
        or abs(trusted_fare_delta)>{{ var('amount_reconciliation_tolerance') }}
        or abs(source_tip_delta)>{{ var('amount_reconciliation_tolerance') }}
        or abs(trusted_tip_delta)>{{ var('amount_reconciliation_tolerance') }}
        or abs(source_total_delta)>{{ var('amount_reconciliation_tolerance') }}
        or abs(trusted_total_delta)>{{ var('amount_reconciliation_tolerance') }}
    union all
    select 'GLOBAL_FACT_COUNTS', case when
      (select rows from fact_actual) is distinct from
      (select coalesce(sum(fact_rows),0) from reconciled)
      then 1 else 0 end
    union all
    select 'GLOBAL_FACT_MONEY', case when
      (select row(fare,tip,total,revenue) from fact_actual) is distinct from
      (select row(coalesce(sum(fact_fare_amount),0),coalesce(sum(fact_tip_amount),0),
                  coalesce(sum(fact_total_amount),0),coalesce(sum(revenue_amount),0))
       from reconciled)
      then 1 else 0 end
    union all
    select 'GLOBAL_REJECT_COUNTS', case when
      (select row(rows,(select rows from duplicate_actual),(select rows from technical_actual))
       from business_actual) is distinct from
      (select row(coalesce(sum(business_rows),0)::bigint,
                  coalesce(sum(duplicate_rows),0)::bigint,
                  coalesce(sum(technical_rows),0)::bigint) from reconciled)
      then 1 else 0 end
    union all
    select 'GLOBAL_REJECT_MONEY', case when
      (select row((select fare from business_actual),(select tip from business_actual),
                  (select total from business_actual),(select fare from duplicate_actual),
                  (select tip from duplicate_actual),(select total from duplicate_actual)))
      is distinct from
      (select row(coalesce(sum(r.business_fare_amount),0),coalesce(sum(r.business_tip_amount),0),
                  coalesce(sum(r.business_total_amount),0),coalesce(sum(r.duplicate_fare_amount),0),
                  coalesce(sum(r.duplicate_tip_amount),0),coalesce(sum(r.duplicate_total_amount),0))
       from {{ ref('file_reconciliation') }} r join selected s using(file_version_id))
      then 1 else 0 end
    union all
    select 'GLOBAL_GRAIN', count(*) from (
      select source_record_key from {{ ref('fact_taxi_trip') }}
      group by source_record_key having count(*)<>1
    ) duplicates
    union all
    select 'GLOBAL_TRIP_KEY', count(*) from (
      select trip_key from {{ ref('fact_taxi_trip') }}
      group by trip_key having count(*)<>1
    ) duplicates
    union all
    select 'GLOBAL_FK', count(*) from {{ ref('fact_taxi_trip') }} f
    where f.trip_key is null or f.source_record_key is null or f.trip_count is distinct from 1
      or not exists(select 1 from {{ ref('dim_date') }} d where d.date_key=f.pickup_date_key)
      or not exists(select 1 from {{ ref('dim_date') }} d where d.date_key=f.dropoff_date_key)
      or not exists(select 1 from {{ ref('dim_time') }} d where d.time_key=f.pickup_time_key)
      or not exists(select 1 from {{ ref('dim_time') }} d where d.time_key=f.dropoff_time_key)
      or not exists(select 1 from {{ ref('dim_zone') }} d where d.zone_key=f.pickup_zone_key)
      or not exists(select 1 from {{ ref('dim_zone') }} d where d.zone_key=f.dropoff_zone_key)
      or not exists(select 1 from {{ ref('dim_vendor') }} d where d.vendor_key=f.vendor_key)
      or not exists(select 1 from {{ ref('dim_payment_type') }} d where d.payment_type_key=f.payment_type_key)
      or not exists(select 1 from {{ ref('dim_rate_code') }} d where d.rate_code_key=f.rate_code_key)
)
select rule,failures from checks where failures>0
