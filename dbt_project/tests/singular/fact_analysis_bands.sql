select f.source_record_key
from {{ ref('fact_taxi_trip') }} f
left join {{ ref('dim_time') }} pickup_time on pickup_time.time_key = f.pickup_time_key
left join {{ ref('dim_time') }} dropoff_time on dropoff_time.time_key = f.dropoff_time_key
where f.passenger_group is distinct from case
        when f.passenger_count is null then 'UNKNOWN'
        when f.passenger_count <= 0 then 'OUT_OF_RANGE'
        when f.passenger_count = {{ var('passenger_group_single_size') }} then 'SOLO'
        when f.passenger_count <= {{ var('passenger_group_middle_max') }} then 'SMALL_GROUP'
        else 'LARGE_GROUP'
    end
   or f.distance_band is distinct from case
        when f.trip_distance_miles is null then 'UNKNOWN'
        when f.trip_distance_miles < 0 then 'NEGATIVE'
        when f.trip_distance_miles = 0 then 'ZERO'
        when f.trip_distance_miles < {{ var('distance_band_edges_miles')[0] }} then 'BAND_1'
        when f.trip_distance_miles < {{ var('distance_band_edges_miles')[1] }} then 'BAND_2'
        when f.trip_distance_miles < {{ var('distance_band_edges_miles')[2] }} then 'BAND_3'
        else 'BAND_4'
    end
   or f.pickup_time_band is distinct from pickup_time.time_band
   or f.dropoff_time_band is distinct from dropoff_time.time_band
