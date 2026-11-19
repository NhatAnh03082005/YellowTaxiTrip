{# Minimal affected-grain OLAP implementation is T07; never run legacy queries on slim Fact. #}
{{ config(enabled=not var('dbt_minimal_mode', false)) }}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['source_year_month', 'pickup_zone_key', 'dropoff_zone_key'],
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
    f.dropoff_zone_key,
    count(*)::bigint as trip_count,
    sum(f.revenue_amount) filter (where f.is_revenue_kpi_eligible) as total_revenue,
    avg(f.trip_distance_miles) as average_trip_distance_miles,
    avg(f.duration_seconds) filter (where f.is_duration_kpi_eligible) as average_duration_seconds,
    clock_timestamp() as refreshed_at
from {{ ref('fact_taxi_trip') }} f
{% if var('apply_approved_fact_replacement', false) %}
join affected a using (source_year_month)
{% else %}
join current_files c using (file_version_id)
{% endif %}
group by 1, 2, 3

