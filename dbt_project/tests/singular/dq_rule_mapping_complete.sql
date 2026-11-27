{{ config(tags=['minimal_batch']) }}
with violations as (
select source_record_key
from {{ ref('int_taxi_trip_classified') }}
where has_unmapped_rule
)
select 'DQ_RULE_MAPPING' as rule,count(*) as failures
from violations having count(*)>0
