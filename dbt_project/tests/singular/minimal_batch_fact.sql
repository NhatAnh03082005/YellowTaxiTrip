{{ config(enabled=var('dbt_minimal_mode', false), tags=['minimal_batch']) }}
{% if var('dbt_minimal_mode', false) %}
-- Inline each consumer's projection instead of spooling every Fact column.
-- OFFSET 0 keeps this file-scoped input behind a subquery boundary: folding
-- without that boundary can choose a Parallel Hash exceeding Docker shm.
with current_fact as not materialized (
    select * from {{ ref('fact_taxi_trip') }}
    where file_version_id={{ minimal_version_id() }}
    offset 0
), accepted as (
    select * from {{ ref('int_taxi_trip_classified') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
      and primary_disposition in ('ACCEPTED','WARNING')
), row_counts as (
    -- Share the same scoped Fact scan for policy, FK and role mapping checks.
    select count(*) filter(where f.trip_key is null or f.source_record_key is null or f.trip_count is distinct from 1
       or f.primary_disposition not in ('ACCEPTED','WARNING')
       or f.is_revenue_kpi_eligible is distinct from (f.total_amount>0)
       or f.revenue_amount is distinct from
          (case when f.total_amount>0 then f.total_amount end)
       or f.is_tip_kpi_eligible is distinct from coalesce(
          f.payment_type_id={{ var('recorded_card_payment_code') }}
          and f.fare_amount>0 and f.tip_amount>=0,false)
       or f.is_speed_kpi_eligible is distinct from coalesce(
          f.duration_seconds>0
          and f.duration_seconds<={{ var('duration_warning_minutes') }}*60
          and f.trip_distance_miles>0
          and f.row_speed_mph<={{ var('speed_warning_mph') }},false)
       or f.is_duration_kpi_eligible is distinct from coalesce(
          f.duration_seconds>0
          and f.duration_seconds<={{ var('duration_warning_minutes') }}*60,false)
       or f.is_passenger_kpi_eligible is distinct from coalesce(
          f.passenger_count between 1 and {{ var('passenger_kpi_max') }},false)) as policy_failures,
           count(*) filter(where f.pickup_date_key is null or f.dropoff_date_key is null
       or f.pickup_time_key is null or f.dropoff_time_key is null
       or f.pickup_zone_key is null or f.dropoff_zone_key is null
       or f.vendor_key is null or f.payment_type_key is null or f.rate_code_key is null
       or f.pickup_date_key not in(select d.date_key from {{ ref('dim_date') }} d where d.date_key is not null)
       or f.dropoff_date_key not in(select d.date_key from {{ ref('dim_date') }} d where d.date_key is not null)
       or f.pickup_time_key not in(select d.time_key from {{ ref('dim_time') }} d where d.time_key is not null)
       or f.dropoff_time_key not in(select d.time_key from {{ ref('dim_time') }} d where d.time_key is not null)
       or f.pickup_zone_key not in(select d.zone_key from {{ ref('dim_zone') }} d where d.zone_key is not null)
       or f.dropoff_zone_key not in(select d.zone_key from {{ ref('dim_zone') }} d where d.zone_key is not null)
       or f.vendor_key not in(select d.vendor_key from {{ ref('dim_vendor') }} d where d.vendor_key is not null)
       or f.payment_type_key not in(select d.payment_type_key from {{ ref('dim_payment_type') }} d where d.payment_type_key is not null)
       or f.rate_code_key not in(select d.rate_code_key from {{ ref('dim_rate_code') }} d where d.rate_code_key is not null)) as fk_failures,
           count(*) filter(where pd.full_date is distinct from f.pickup_datetime::date
       or dd.full_date is distinct from f.dropoff_datetime::date
       or pt.hour_of_day is distinct from extract(hour from f.pickup_datetime)::smallint
       or pt.minute_of_hour is distinct from extract(minute from f.pickup_datetime)::smallint
       or dt.hour_of_day is distinct from extract(hour from f.dropoff_datetime)::smallint
       or dt.minute_of_hour is distinct from extract(minute from f.dropoff_datetime)::smallint
       or f.pickup_zone_key is distinct from coalesce(pz.zone_key,0)
       or f.dropoff_zone_key is distinct from coalesce(dz.zone_key,0)
       or f.vendor_key is distinct from coalesce(v.vendor_key,0)
       or f.payment_type_key is distinct from coalesce(p.payment_type_key,0)
       or f.rate_code_key is distinct from coalesce(r.rate_code_key,0)) as mapping_failures
    from current_fact f
    left join {{ ref('dim_date') }} pd on pd.date_key=f.pickup_date_key
    left join {{ ref('dim_date') }} dd on dd.date_key=f.dropoff_date_key
    left join {{ ref('dim_time') }} pt on pt.time_key=f.pickup_time_key
    left join {{ ref('dim_time') }} dt on dt.time_key=f.dropoff_time_key
    left join {{ ref('dim_zone') }} pz on pz.location_id=f.pu_location_id
    left join {{ ref('dim_zone') }} dz on dz.location_id=f.do_location_id
    left join {{ ref('dim_vendor') }} v on v.vendor_id=f.vendor_id
    left join {{ ref('dim_payment_type') }} p on p.payment_type_id=f.payment_type_id
    left join {{ ref('dim_rate_code') }} r on r.rate_code_id=f.rate_code_id
), checks as (
    select 'FACT_VALUES'::text as rule, count(*) as failures
    from current_fact f join accepted a using(source_record_key)
    where row(f.pickup_datetime,f.dropoff_datetime,f.vendor_id,f.rate_code_id,
              f.payment_type_id,f.pu_location_id,f.do_location_id,
              f.passenger_count,f.trip_distance_miles,f.duration_seconds,
              f.row_speed_mph,f.fare_amount,f.tip_amount,f.total_amount,
              f.calculated_total_amount,f.revenue_amount,f.primary_disposition,
              f.dq_rule_ids,f.component_reconciliation_status)
      is distinct from
          row(a.pickup_datetime,a.dropoff_datetime,a.vendor_id,a.rate_code_id,
              a.payment_type_id,a.pu_location_id,a.do_location_id,
              a.passenger_count,a.trip_distance,a.duration_seconds,
              a.row_speed_mph,a.fare_amount,a.tip_amount,a.reported_total_amount,
              a.calculated_total_amount,a.revenue_amount,a.primary_disposition,
              a.dq_rule_ids,a.component_reconciliation_status)
       or row(f.is_revenue_kpi_eligible,f.is_tip_kpi_eligible,
              f.is_speed_kpi_eligible,f.is_duration_kpi_eligible,
              f.is_passenger_kpi_eligible)
          is distinct from
          row(a.is_revenue_kpi_eligible,a.is_tip_kpi_eligible,
              a.is_speed_kpi_eligible,a.is_duration_kpi_eligible,
              a.is_passenger_kpi_eligible)
    union all
    select r.rule,r.failures from row_counts c
    cross join lateral (values
        ('FACT_MEASURE_POLICY'::text,c.policy_failures),
        ('FACT_ROLE_FK'::text,c.fk_failures),
        ('FACT_ROLE_MAPPING'::text,c.mapping_failures)
    ) r(rule,failures)
)
select rule, failures from checks where failures>0
{% endif %}
