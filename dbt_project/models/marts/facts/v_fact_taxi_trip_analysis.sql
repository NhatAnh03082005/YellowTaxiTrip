{{ config(materialized='view', enabled=var('dbt_minimal_mode', false), tags=['minimal_dds']) }}
select f.*, v.source_file,v.source_year_month,v.source_file_sha256,
       v.schema_version,v.schema_fingerprint,v.source_last_modified,
       f.total_amount as reported_total_amount,
       case when f.fare_amount>0 then round(f.tip_amount/f.fare_amount,6) end as row_tip_rate,
       case when f.passenger_count between 1 and {{ var('passenger_kpi_max') }} then f.passenger_count::text
            when f.passenger_count is null then 'UNKNOWN' else 'OUT_OF_RANGE' end as passenger_band,
       case when f.passenger_count is null then 'UNKNOWN' when f.passenger_count<=0 then 'OUT_OF_RANGE'
            when f.passenger_count={{ var('passenger_group_single_size') }} then 'SOLO'
            when f.passenger_count<={{ var('passenger_group_middle_max') }} then 'SMALL_GROUP' else 'LARGE_GROUP' end as passenger_group,
       case when f.trip_distance_miles is null then 'UNKNOWN' when f.trip_distance_miles<0 then 'NEGATIVE'
            when f.trip_distance_miles=0 then 'ZERO'
            when f.trip_distance_miles<{{ var('distance_band_edges_miles')[0] }} then 'BAND_1'
            when f.trip_distance_miles<{{ var('distance_band_edges_miles')[1] }} then 'BAND_2'
            when f.trip_distance_miles<{{ var('distance_band_edges_miles')[2] }} then 'BAND_3' else 'BAND_4' end as distance_band,
       pt.time_band as pickup_time_band, dt.time_band as dropoff_time_band,
       ('AMT-002'=any(f.dq_rule_ids) or 'AMT-003'=any(f.dq_rule_ids)) as reversal_or_adjustment_candidate
from {{ ref('fact_taxi_trip') }} f
join {{ source('control','source_file_version') }} v using(file_version_id)
left join {{ ref('dim_time') }} pt on pt.time_key=f.pickup_time_key
left join {{ ref('dim_time') }} dt on dt.time_key=f.dropoff_time_key
