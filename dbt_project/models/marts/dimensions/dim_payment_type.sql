{% if var('dbt_minimal_mode', false) %}
{{ config(materialized='minimal_reference',tags=['minimal_reference'], reference_key='payment_type_key', reference_natural_key='payment_type_id',reference_dependencies=['payment_type_codes']) }}
{% endif %}
select payment_type_key, payment_type_id, payment_type_name,
       false as is_technical_member, 'BUSINESS'::text as member_type
from {{ ref('payment_type_codes') }}
union all
select 0, null::smallint, 'Unknown or unmapped payment', true, 'UNKNOWN'
union all
select -1, null::smallint, 'Payment not applicable', true, 'NOT_APPLICABLE'
