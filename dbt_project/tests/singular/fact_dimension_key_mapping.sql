-- A valid FK can still point at the wrong business member.
select f.source_record_key
from {{ ref('fact_taxi_trip') }} f
left join {{ ref('dim_zone') }} pu on pu.location_id = f.pu_location_id
left join {{ ref('dim_zone') }} dz on dz.location_id = f.do_location_id
left join {{ ref('dim_vendor') }} v on v.vendor_id = f.vendor_id
left join {{ ref('dim_payment_type') }} p on p.payment_type_id = f.payment_type_id
left join {{ ref('dim_rate_code') }} r on r.rate_code_id = f.rate_code_id
where f.pickup_zone_key is distinct from coalesce(pu.zone_key, 0)
   or f.dropoff_zone_key is distinct from coalesce(dz.zone_key, 0)
   or f.vendor_key is distinct from coalesce(v.vendor_key, 0)
   or f.payment_type_key is distinct from coalesce(p.payment_type_key, 0)
   or f.rate_code_key is distinct from coalesce(r.rate_code_key, 0)
