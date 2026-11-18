select source_record_key
from {{ ref('fact_taxi_trip') }}
where (payment_type_id = {{ var('flex_fare_payment_code') }} and payment_type_key <= 0)
   or (rate_code_id = {{ var('business_unknown_rate_code') }} and rate_code_key <= 0)
   or (pu_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }}) and pickup_zone_key <= 0)
   or (do_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }}) and dropoff_zone_key <= 0)
