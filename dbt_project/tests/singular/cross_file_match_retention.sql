-- A cross-file match is a warning signal, never the sole duplicate exclusion.
select source_record_key
from {{ ref('int_taxi_trip_classified') }}
where dup_002
  and not dup_001
  and primary_disposition = 'DUPLICATE'
