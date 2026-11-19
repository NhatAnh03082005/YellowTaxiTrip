{% if var('dbt_minimal_mode', false) %}
{{ config(materialized='minimal_reference',tags=['minimal_reference'], reference_key='vendor_key', reference_natural_key='vendor_id',reference_dependencies=['vendor_codes']) }}
{% endif %}
select vendor_key, vendor_id, vendor_name,
       false as is_technical_member, 'BUSINESS'::text as member_type
from {{ ref('vendor_codes') }}
union all
select 0, null::smallint, 'Unknown or unmapped vendor', true, 'UNKNOWN'
union all
select -1, null::smallint, 'Vendor not applicable', true, 'NOT_APPLICABLE'
