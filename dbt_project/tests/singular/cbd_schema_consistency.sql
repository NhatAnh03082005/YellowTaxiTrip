select source_record_key
from {{ ref('int_taxi_trip_enriched') }}
where (source_year_month < '{{ var('cbd_schema_start_month') }}'
       and (not cbd_structurally_absent or cbd_congestion_fee is not null))
   or (source_year_month >= '{{ var('cbd_schema_start_month') }}'
       and cbd_structurally_absent)
