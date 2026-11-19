{% if var('dbt_minimal_mode', false) %}
{{ config(materialized='minimal_fact', tags=['dds', 'fact']) }}
-- depends_on: {{ ref('int_taxi_trip_classified') }}
-- depends_on: {{ source('mage_dq', 'technical_quarantine') }}
-- depends_on: {{ source('stage', 'yellow_taxi_raw') }}

select
    e.source_record_key,
    coalesce(pd.date_key, 0)::integer as pickup_date_key,
    coalesce(dd.date_key, 0)::integer as dropoff_date_key,
    coalesce(pt.time_key, 0)::smallint as pickup_time_key,
    coalesce(dt.time_key, 0)::smallint as dropoff_time_key,
    coalesce(pz.zone_key, 0)::integer as pickup_zone_key,
    coalesce(dz.zone_key, 0)::integer as dropoff_zone_key,
    coalesce(v.vendor_key, 0)::integer as vendor_key,
    coalesce(p.payment_type_key, 0)::integer as payment_type_key,
    coalesce(r.rate_code_key, 0)::integer as rate_code_key,
    1::smallint as trip_count,
    e.pickup_datetime,
    e.dropoff_datetime,
    e.vendor_id,
    e.rate_code_id,
    e.payment_type_id,
    e.pu_location_id,
    e.do_location_id,
    e.passenger_count,
    e.trip_distance as trip_distance_miles,
    e.duration_seconds::integer as duration_seconds,
    e.row_speed_mph,
    e.fare_amount,
    e.extra,
    e.mta_tax,
    e.tip_amount,
    e.tolls_amount,
    e.improvement_surcharge,
    e.total_amount,
    e.congestion_surcharge,
    e.airport_fee,
    e.cbd_congestion_fee,
    e.calculated_total_amount,
    e.revenue_amount,
    e.component_reconciliation_status,
    e.is_revenue_kpi_eligible,
    e.is_tip_kpi_eligible,
    e.is_speed_kpi_eligible,
    e.is_duration_kpi_eligible,
    e.is_passenger_kpi_eligible,
    e.store_and_fwd_flag,
    e.primary_disposition,
    e.dq_rule_ids,
    e.batch_id,
    e.file_version_id,
    e.source_row_number
from {{ ref('int_taxi_trip_accepted') }} e
left join {{ ref('dim_date') }} pd on pd.full_date = e.pickup_datetime::date
left join {{ ref('dim_date') }} dd on dd.full_date = e.dropoff_datetime::date
left join {{ ref('dim_time') }} pt
    on pt.time_key = (extract(hour from e.pickup_datetime)::integer * 60
        + extract(minute from e.pickup_datetime)::integer + 1)
left join {{ ref('dim_time') }} dt
    on dt.time_key = (extract(hour from e.dropoff_datetime)::integer * 60
        + extract(minute from e.dropoff_datetime)::integer + 1)
left join {{ ref('dim_zone') }} pz on pz.location_id = e.pu_location_id
left join {{ ref('dim_zone') }} dz on dz.location_id = e.do_location_id
left join {{ ref('dim_vendor') }} v on v.vendor_id = e.vendor_id
left join {{ ref('dim_payment_type') }} p on p.payment_type_id = e.payment_type_id
left join {{ ref('dim_rate_code') }} r on r.rate_code_id = e.rate_code_id
where e.batch_id={{ minimal_batch_id() }} and e.file_version_id={{ minimal_version_id() }}
{% else %}
{{ config(materialized='fact_identity', tags=['dds', 'fact']) }}
-- depends_on: {{ ref('int_taxi_trip_classified') }}
-- depends_on: {{ source('mage_dq', 'technical_quarantine') }}
-- depends_on: {{ source('stage', 'yellow_taxi_raw') }}

select
    e.source_record_key,
    e.raw_exact_fingerprint,
    e.candidate_trip_fingerprint,
    coalesce(pd.date_key, 0)::integer as pickup_date_key,
    coalesce(dd.date_key, 0)::integer as dropoff_date_key,
    coalesce(pt.time_key, 0)::smallint as pickup_time_key,
    coalesce(dt.time_key, 0)::smallint as dropoff_time_key,
    coalesce(pz.zone_key, 0)::integer as pickup_zone_key,
    coalesce(dz.zone_key, 0)::integer as dropoff_zone_key,
    coalesce(v.vendor_key, 0)::integer as vendor_key,
    coalesce(p.payment_type_key, 0)::integer as payment_type_key,
    coalesce(r.rate_code_key, 0)::integer as rate_code_key,
    1::smallint as trip_count,
    e.pickup_datetime,
    e.dropoff_datetime,
    e.vendor_id,
    e.rate_code_id,
    e.payment_type_id,
    e.pu_location_id,
    e.do_location_id,
    e.passenger_count,
    e.trip_distance as trip_distance_miles,
    e.duration_seconds::integer as duration_seconds,
    e.row_speed_mph,
    e.fare_amount,
    e.extra,
    e.mta_tax,
    e.tip_amount,
    e.tolls_amount,
    e.improvement_surcharge,
    e.total_amount,
    e.congestion_surcharge,
    e.airport_fee,
    e.cbd_congestion_fee,
    e.reported_total_amount,
    e.calculated_total_amount,
    e.revenue_amount,
    e.row_tip_rate,
    e.component_reconciliation_status,
    e.is_revenue_kpi_eligible,
    e.is_tip_kpi_eligible,
    e.is_speed_kpi_eligible,
    e.is_duration_kpi_eligible,
    e.is_passenger_kpi_eligible,
    e.passenger_band,
    e.passenger_group,
    e.distance_band,
    e.pickup_time_band,
    dt.time_band as dropoff_time_band,
    e.store_and_fwd_flag,
    e.primary_disposition,
    e.dq_severity,
    e.dq_rule_ids,
    e.reversal_or_adjustment_candidate,
    e.batch_id,
    e.file_version_id,
    e.source_file,
    e.source_year_month,
    e.source_file_sha256,
    e.source_row_number,
    e.schema_fingerprint,
    e.schema_version,
    e.source_last_modified,
    e.ingested_at
from {{ ref('int_taxi_trip_accepted') }} e
left join {{ ref('dim_date') }} pd on pd.full_date = e.pickup_datetime::date
left join {{ ref('dim_date') }} dd on dd.full_date = e.dropoff_datetime::date
left join {{ ref('dim_time') }} pt
    on pt.time_key = (extract(hour from e.pickup_datetime)::integer * 60
        + extract(minute from e.pickup_datetime)::integer + 1)
left join {{ ref('dim_time') }} dt
    on dt.time_key = (extract(hour from e.dropoff_datetime)::integer * 60
        + extract(minute from e.dropoff_datetime)::integer + 1)
left join {{ ref('dim_zone') }} pz on pz.location_id = e.pu_location_id
left join {{ ref('dim_zone') }} dz on dz.location_id = e.do_location_id
left join {{ ref('dim_vendor') }} v on v.vendor_id = e.vendor_id
left join {{ ref('dim_payment_type') }} p on p.payment_type_id = e.payment_type_id
left join {{ ref('dim_rate_code') }} r on r.rate_code_id = e.rate_code_id
{% if var('load_batch_id', none) is not none %}
where e.batch_id = {{ var('load_batch_id') | int }}
   or e.raw_exact_fingerprint in (
       select current_batch.raw_exact_fingerprint
       from {{ ref('int_taxi_trip_fingerprinted') }} current_batch
       where current_batch.batch_id = {{ var('load_batch_id') | int }}
   )
   or e.candidate_trip_fingerprint in (
       select current_batch.candidate_trip_fingerprint
       from {{ ref('int_taxi_trip_fingerprinted') }} current_batch
       where current_batch.batch_id = {{ var('load_batch_id') | int }}
   )
{% endif %}
{% endif %}
