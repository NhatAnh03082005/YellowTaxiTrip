{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='pickup_date_key',
    schema='olap',
    tags=['olap', 'powerbi']
) }}

with current_files as (
    select distinct file_version_id from {{ ref('stg_yellow_taxi') }}
)
select
    f.pickup_date_key,
    count(*)::bigint as trip_count,
    sum(f.passenger_count) filter (where f.is_passenger_kpi_eligible) as passenger_count,
    sum(f.revenue_amount) filter (where f.is_revenue_kpi_eligible) as total_revenue,
    sum(f.tip_amount) filter (where f.is_tip_kpi_eligible) as recorded_tip_amount,
    sum(f.fare_amount) filter (where f.is_tip_kpi_eligible) as eligible_fare_amount,
    avg(f.row_speed_mph) filter (where f.is_speed_kpi_eligible) as average_speed_mph,
    clock_timestamp() as refreshed_at
from {{ ref('fact_taxi_trip') }} f
join current_files c using (file_version_id)
group by 1

