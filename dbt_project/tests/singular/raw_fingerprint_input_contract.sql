-- Fail if raw values needed by the approved ordered 19/20-column contract are missing.
select source_record_key
from {{ ref('int_taxi_trip_fingerprinted') }}
where raw_values is null
   or not (raw_values ?& array[
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
        'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge'
   ])
   or not (raw_values ? 'Airport_fee' or raw_values ? 'airport_fee')
   or (source_year_month >= '{{ var('cbd_schema_start_month') }}'
       and not raw_values ? 'cbd_congestion_fee')
   or (source_year_month < '{{ var('cbd_schema_start_month') }}'
       and raw_values ? 'cbd_congestion_fee')
