select source_record_key
from {{ ref('fact_taxi_trip') }}
where pickup_date_key = -1 or dropoff_date_key = -1
   or pickup_time_key = -1 or dropoff_time_key = -1
   or pickup_zone_key = -1 or dropoff_zone_key = -1
   or vendor_key = -1 or payment_type_key = -1 or rate_code_key = -1
   or (pu_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})
       and pickup_zone_key <= 0)
   or (do_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})
       and dropoff_zone_key <= 0)
   or (payment_type_id = {{ var('flex_fare_payment_code') }}
       and payment_type_key <= 0)
