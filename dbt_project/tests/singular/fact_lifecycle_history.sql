{{ config(enabled=(not var('dbt_minimal_mode', false)) and var('fact_lifecycle_mode', 'guard') == 'archive') }}

with ordered as (
    select h.*,
           row_number() over (partition by source_record_key order by event_id) as event_number,
           lag(to_disposition) over (partition by source_record_key order by event_id) as previous_disposition
    from {{ source('fact_lifecycle', 'history') }} h
), latest as (
    select distinct on (source_record_key)
           source_record_key, trip_key, to_disposition
    from ordered order by source_record_key, event_id desc
)
select source_record_key from ordered
where event_number > 1 and from_disposition is distinct from previous_disposition
union all
select source_record_key from ordered
group by source_record_key
having count(distinct trip_key) > 1
union all
select f.source_record_key
from {{ ref('fact_taxi_trip') }} f
left join latest h on h.source_record_key = f.source_record_key
where h.source_record_key is null
   or h.trip_key is distinct from f.trip_key
   or h.to_disposition <> f.primary_disposition
union all
select h.source_record_key
from latest h
left join {{ ref('fact_taxi_trip') }} f on f.source_record_key = h.source_record_key
where (h.to_disposition in ('ACCEPTED', 'WARNING') and f.source_record_key is null)
   or (h.to_disposition not in ('ACCEPTED', 'WARNING') and f.source_record_key is not null)
union all
select c.source_record_key
from {{ ref('int_taxi_trip_classified') }} c
left join latest h on h.source_record_key = c.source_record_key
where c.primary_disposition in ('BUSINESS_QUARANTINE', 'DUPLICATE')
  and h.to_disposition is distinct from c.primary_disposition
union all
select t.source_record_key
from {{ source('mage_dq', 'technical_quarantine') }} t
left join latest h on h.source_record_key = t.source_record_key
where h.to_disposition is distinct from 'TECHNICAL_REJECT'
