select vendor_key, vendor_id, vendor_name,
       false as is_technical_member, 'BUSINESS'::text as member_type
from {{ ref('vendor_codes') }}
union all
select 0, null::smallint, 'Unknown or unmapped vendor', true, 'UNKNOWN'
union all
select -1, null::smallint, 'Vendor not applicable', true, 'NOT_APPLICABLE'
