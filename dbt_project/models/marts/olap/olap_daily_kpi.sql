{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='pickup_date_key',
    schema='olap',
    tags=['olap', 'powerbi'],
    pre_hook="{{ invalidate_replacement_olap('pickup_date_key') }}"
) }}

{% if var('apply_approved_fact_replacement', false) %}
with affected as (
    {{ replacement_olap_scope('pickup_date_key') }}
)
{% else %}
with current_files as (
    select distinct file_version_id from {{ ref('stg_yellow_taxi') }}
)
{% endif %}
select
    f.pickup_date_key,
    count(*)::bigint as trip_count,
    sum(f.passenger_count) filter (where f.is_passenger_kpi_eligible) as passenger_count,
    sum(f.revenue_amount) filter (where f.is_revenue_kpi_eligible) as total_revenue,
    sum(f.tip_amount) filter (where f.is_tip_kpi_eligible) as recorded_tip_amount,
    sum(f.fare_amount) filter (where f.is_tip_kpi_eligible) as eligible_fare_amount,
    sum(f.trip_distance_miles) filter (where f.is_speed_kpi_eligible) * 3600.0
        / nullif(sum(f.duration_seconds) filter (where f.is_speed_kpi_eligible), 0) as average_speed_mph,
    clock_timestamp() as refreshed_at
from {{ ref('fact_taxi_trip') }} f
{% if var('apply_approved_fact_replacement', false) %}
join affected a using (pickup_date_key)
{% else %}
join current_files c using (file_version_id)
{% endif %}
group by 1

