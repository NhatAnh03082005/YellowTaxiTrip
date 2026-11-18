with base as (
    select
        s.*,
        -- A malformed source date can be decades away. Classify it before
        -- narrowing to the Fact's integer duration under the configured bound.
        extract(epoch from (dropoff_datetime - pickup_datetime))::bigint as duration_seconds,
        total_amount::numeric(12,2) as reported_total_amount,
        case when source_year_month < '{{ var("cbd_schema_start_month") }}' then true else false end as cbd_structurally_absent
    from {{ ref('stg_yellow_taxi') }} s
), calculated as (
    select
        b.*,
        case when duration_seconds > 0 and trip_distance > 0
            then round(trip_distance * 3600.0 / duration_seconds, 2)::numeric(10,2)
        end as row_speed_mph,
        case when fare_amount > 0 then round(tip_amount / fare_amount, 6)::numeric(12,6) end as row_tip_rate,
        case when fare_amount is not null and extra is not null and mta_tax is not null
            and tip_amount is not null and tolls_amount is not null
            and improvement_surcharge is not null and congestion_surcharge is not null
            and airport_fee is not null
            and (cbd_structurally_absent or cbd_congestion_fee is not null)
            then (fare_amount + extra + mta_tax + tip_amount + tolls_amount
                + improvement_surcharge + congestion_surcharge + airport_fee
                + case when cbd_structurally_absent then 0 else cbd_congestion_fee end)::numeric(12,2)
        end as calculated_total_amount,
        case
            when passenger_count between 1 and {{ var('passenger_kpi_max') }} then passenger_count::text
            when passenger_count is null then 'UNKNOWN'
            else 'OUT_OF_RANGE'
        end as passenger_band,
        case
            when passenger_count is null then 'UNKNOWN'
            when passenger_count <= 0 then 'OUT_OF_RANGE'
            when passenger_count = {{ var('passenger_group_single_size') }} then 'SOLO'
            when passenger_count <= {{ var('passenger_group_middle_max') }} then 'SMALL_GROUP'
            else 'LARGE_GROUP'
        end as passenger_group,
        case
            when trip_distance is null then 'UNKNOWN'
            when trip_distance < 0 then 'NEGATIVE'
            when trip_distance = 0 then 'ZERO'
            when trip_distance < {{ var('distance_band_edges_miles')[0] }} then 'BAND_1'
            when trip_distance < {{ var('distance_band_edges_miles')[1] }} then 'BAND_2'
            when trip_distance < {{ var('distance_band_edges_miles')[2] }} then 'BAND_3'
            else 'BAND_4'
        end as distance_band,
        case
            when pickup_datetime is null then 'UNKNOWN'
            when extract(hour from pickup_datetime) >= {{ var('time_band_start_hours')[0] }}
                and extract(hour from pickup_datetime) < {{ var('time_band_start_hours')[1] }} then 'MORNING'
            when extract(hour from pickup_datetime) >= {{ var('time_band_start_hours')[1] }}
                and extract(hour from pickup_datetime) < {{ var('time_band_start_hours')[2] }} then 'AFTERNOON'
            when extract(hour from pickup_datetime) >= {{ var('time_band_start_hours')[2] }}
                and extract(hour from pickup_datetime) < {{ var('time_band_start_hours')[3] }} then 'EVENING'
            else 'NIGHT'
        end as pickup_time_band
    from base b
)
select
    c.*,
    case
        when reported_total_amount is null or calculated_total_amount is null then 'NOT_RECONCILABLE'
        when abs(reported_total_amount - calculated_total_amount) <= {{ var('component_match_tolerance') }} then 'MATCHED'
        when abs(reported_total_amount - calculated_total_amount) <= {{ var('component_rounding_tolerance') }} then 'ROUNDING_WARNING'
        else 'REVIEW_WARNING'
    end as component_reconciliation_status
from calculated c
