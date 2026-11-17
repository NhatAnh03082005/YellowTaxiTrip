select payment_type_key, payment_type_id, payment_type_name,
       false as is_technical_member, 'BUSINESS'::text as member_type
from {{ ref('payment_type_codes') }}
union all
select 0, null::smallint, 'Unknown or unmapped payment', true, 'UNKNOWN'
union all
select -1, null::smallint, 'Payment not applicable', true, 'NOT_APPLICABLE'
