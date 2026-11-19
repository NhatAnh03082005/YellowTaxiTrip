{# Minimal affected-grain OLAP implementation is T07; never run legacy queries on slim Fact. #}
{{ config(enabled=not var('dbt_minimal_mode', false)) }}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['source_year_month', 'pickup_zone_key', 'vendor_key',
                'payment_type_key', 'rate_code_key'],
    schema='olap',
    tags=['olap', 'powerbi'],
    pre_hook="{{ invalidate_replacement_olap('source_year_month') }}"
) }}

{% if var('apply_approved_fact_replacement', false) %}
with affected as (
    {{ replacement_olap_scope('source_year_month') }}
)
{% else %}
with current_files as (
    select distinct file_version_id from {{ ref('stg_yellow_taxi') }}
)
{% endif %}
select
    f.source_year_month,
    f.pickup_zone_key,
    f.vendor_key,
    f.payment_type_key,
    f.rate_code_key,
    count(*)::bigint as trip_count,
    sum(f.passenger_count) filter (where f.is_passenger_kpi_eligible) as passenger_count,
    sum(f.revenue_amount) filter (where f.is_revenue_kpi_eligible) as total_revenue,
    sum(f.tip_amount) filter (where f.is_tip_kpi_eligible) as recorded_tip_amount,
    sum(f.fare_amount) filter (where f.is_tip_kpi_eligible) as eligible_fare_amount,
    case when sum(f.fare_amount) filter (where f.is_tip_kpi_eligible) > 0
        then round(
            sum(f.tip_amount) filter (where f.is_tip_kpi_eligible)
            / sum(f.fare_amount) filter (where f.is_tip_kpi_eligible), 6
        ) end as recorded_card_tip_rate,
    sum(f.trip_distance_miles) filter (where f.is_speed_kpi_eligible) * 3600.0
        / nullif(sum(f.duration_seconds) filter (where f.is_speed_kpi_eligible), 0) as average_speed_mph,
    avg(f.duration_seconds) filter (where f.is_duration_kpi_eligible) as average_duration_seconds,
    clock_timestamp() as refreshed_at
from {{ ref('fact_taxi_trip') }} f
{% if var('apply_approved_fact_replacement', false) %}
join affected a using (source_year_month)
{% else %}
join current_files c using (file_version_id)
{% endif %}
group by 1, 2, 3, 4, 5

