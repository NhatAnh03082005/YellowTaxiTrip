select c.source_record_key
from {{ ref('int_taxi_trip_classified') }} c
left join {{ ref('payment_type_codes') }} p on p.payment_type_id = c.payment_type_id
where (c.payment_type_id = {{ var('flex_fare_payment_code') }}
       and (p.payment_type_key is null or p.payment_type_key <= 0 or c.code_003))
   or ((c.pu_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})
       or c.do_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }}))
       and not c.loc_002)
