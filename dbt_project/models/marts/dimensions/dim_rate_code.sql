select rate_code_key, rate_code_id, rate_code_name,
       false as is_technical_member, 'BUSINESS'::text as member_type
from {{ ref('rate_codes') }}
union all
select 0, null::smallint, 'Unknown or unmapped rate', true, 'UNKNOWN'
union all
select -1, null::smallint, 'Rate not applicable', true, 'NOT_APPLICABLE'
