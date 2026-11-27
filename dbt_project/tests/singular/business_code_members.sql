{{ config(tags=['minimal_batch']) }}
with violations as (
select c.source_record_key
from {{ ref('int_taxi_trip_classified') }} c
left join {{ ref('payment_type_codes') }} p on p.payment_type_id = c.payment_type_id
where (c.payment_type_id = {{ var('flex_fare_payment_code') }}
       and (p.payment_type_key is null or p.payment_type_key <= 0
            or {% if var('dbt_minimal_mode', false) %}'CODE-003'=any(c.dq_rule_ids){% else %}c.code_003{% endif %}))
   or ((c.pu_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})
       or c.do_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }}))
       and not {% if var('dbt_minimal_mode', false) %}'LOC-002'=any(c.dq_rule_ids){% else %}c.loc_002{% endif %})
)
select 'BUSINESS_CODE_MEMBERS' as rule,count(*) as failures
from violations having count(*)>0
