select source_record_key
from {{ ref('int_taxi_trip_classified') }}
where has_unmapped_rule
