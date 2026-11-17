with all_keys as (
    select 'date' as dimension_name, date_key::integer as dimension_key from {{ ref('dim_date') }}
    union all select 'time', time_key::integer from {{ ref('dim_time') }}
    union all select 'zone', zone_key from {{ ref('dim_zone') }}
    union all select 'vendor', vendor_key from {{ ref('dim_vendor') }}
    union all select 'payment', payment_type_key from {{ ref('dim_payment_type') }}
    union all select 'rate', rate_code_key from {{ ref('dim_rate_code') }}
)
select dimension_name
from all_keys
group by dimension_name
having count(*) filter (where dimension_key = 0) <> 1
    or count(*) filter (where dimension_key = -1) <> 1
    or count(*) filter (where dimension_key > 0) = 0
union all
select 'time_grain'
from {{ ref('dim_time') }}
having count(*) <> 1442
union all
select 'zone_count'
from {{ ref('dim_zone') }}
having count(*) <> (select count(*) + 2 from {{ ref('zone_key_map') }})
