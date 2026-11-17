select 1
from {{ ref('stg_taxi_zone') }}
having count(*) <> {{ var('zone_lookup_expected_count') }}
