{{ config(enabled=not var('dbt_minimal_mode', false)) }}
select source_record_key
from {{ ref('fact_taxi_trip') }}
where (pickup_datetime is not null
       and to_char(pickup_datetime, 'YYYY-MM') <> source_year_month
       and not ('TIME-005' = any(dq_rule_ids)))
   or (source_year_month < '{{ var('cbd_schema_start_month') }}'
       and cbd_congestion_fee is not null)
